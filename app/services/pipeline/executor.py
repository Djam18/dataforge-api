"""Pipeline execution engine.

Runs each step sequentially. Passes output of step N as input to step N+1.
Steps are pluggable — add a new StepType and register a runner.

Pattern: Strategy + Chain of Responsibility.
Each step type has a runner function. The executor chains them.
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any
import httpx

from app.models.pipeline import (
    PipelineExecution,
    PipelineStep,
    StepResult,
    ExecutionStatus,
    StepType,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────
#  Step runners
# ──────────────────────────────────────────

async def run_http_fetch(config: dict, context: dict) -> dict:
    """Fetch data from an HTTP endpoint."""
    url     = config.get("url", "")
    method  = config.get("method", "GET").upper()
    headers = config.get("headers") or {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(method, url, headers=headers)
        resp.raise_for_status()
        return {"status_code": resp.status_code, "data": resp.json(), "url": url}


async def run_sql_query(config: dict, context: dict) -> dict:
    """Run a read-only SQL query against a configured data source."""
    # In prod: look up DataSource by id, decrypt credentials, run query
    # Stub for demo purposes
    query = config.get("query", "SELECT 1")
    logger.info(f"[executor] sql_query: {query[:80]!r}")
    return {"query": query, "rows": [], "row_count": 0}


async def run_transform(config: dict, context: dict) -> dict:
    """Apply a Python expression transform to the context data.

    Security note: eval() on user input is dangerous.
    In production: use a sandboxed interpreter (RestrictedPython) or
    pre-defined transform functions. This is an MVP demo.
    """
    transform_fn = config.get("transform_fn", "data")
    data = context.get("data", {})
    # Safe-ish for internal use only — never expose to untrusted input
    result = eval(transform_fn, {"data": data, "__builtins__": {}})  # noqa: S307
    return {"transformed": result}


async def run_http_webhook(config: dict, context: dict) -> dict:
    """POST context data to a webhook URL."""
    url     = config.get("url", "")
    headers = config.get("headers") or {"Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(url, json=context, headers=headers)
        return {"status_code": resp.status_code, "delivered": resp.is_success}


RUNNERS = {
    StepType.HTTP_FETCH:   run_http_fetch,
    StepType.SQL_QUERY:    run_sql_query,
    StepType.TRANSFORM:    run_transform,
    StepType.HTTP_WEBHOOK: run_http_webhook,
}


# ──────────────────────────────────────────
#  Main executor
# ──────────────────────────────────────────

async def execute_pipeline(
    execution: PipelineExecution,
    steps: list[PipelineStep],
    db,
) -> None:
    """Run all enabled steps in order. Persist results to DB."""
    context: dict[str, Any] = {}

    execution.status     = ExecutionStatus.RUNNING
    execution.started_at = datetime.now(tz=timezone.utc)
    await db.commit()

    for step in sorted(steps, key=lambda s: s.position):
        if not step.enabled:
            continue

        config  = json.loads(step.config or "{}")
        runner  = RUNNERS.get(step.step_type)
        if not runner:
            logger.warning(f"[executor] no runner for step_type={step.step_type!r}")
            continue

        result = StepResult(
            execution_id=execution.id,
            step_id=step.id,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(tz=timezone.utc),
        )
        db.add(result)
        await db.flush()

        t0 = time.perf_counter()
        attempt = 0

        while attempt <= step.retry_count:
            try:
                output  = await asyncio.wait_for(runner(config, context), timeout=step.timeout_seconds)
                context = {**context, **output}  # Merge immutably

                result.status      = ExecutionStatus.SUCCESS
                result.output      = json.dumps(output)
                result.duration_ms = int((time.perf_counter() - t0) * 1000)
                logger.info(f"[executor] step={step.name!r} ok ({result.duration_ms}ms)")
                break

            except asyncio.TimeoutError:
                result.error  = f"timeout after {step.timeout_seconds}s"
                attempt += 1
            except Exception as exc:
                result.error  = str(exc)
                attempt += 1
                logger.error(f"[executor] step={step.name!r} attempt={attempt} error={exc}")

            if attempt > step.retry_count:
                result.status      = ExecutionStatus.FAILED
                result.duration_ms = int((time.perf_counter() - t0) * 1000)
                execution.status   = ExecutionStatus.FAILED
                execution.error    = f"Step '{step.name}' failed: {result.error}"
                execution.finished_at = datetime.now(tz=timezone.utc)
                await db.commit()
                return

        await db.commit()

    execution.status      = ExecutionStatus.SUCCESS
    execution.finished_at = datetime.now(tz=timezone.utc)
    await db.commit()
    logger.info(f"[executor] execution_id={execution.id} completed successfully")
