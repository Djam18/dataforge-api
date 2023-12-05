"""Pipeline executor tests.

Testing async code with pytest-asyncio.
The executor is pure async Python — no FastAPI needed for these unit tests.
Mocking httpx calls with respx (httpx's mock library).
"""

import json
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from app.services.pipeline.executor import (
    execute_pipeline,
    run_http_fetch,
    run_transform,
    RUNNERS,
)
from app.models.pipeline import (
    PipelineExecution,
    PipelineStep,
    StepResult,
    ExecutionStatus,
    StepType,
)


# ──────────────────────────────────────────
#  Fixtures
# ──────────────────────────────────────────

def make_execution(id: int = 1, pipeline_id: int = 1) -> PipelineExecution:
    ex = MagicMock(spec=PipelineExecution)
    ex.id         = id
    ex.pipeline_id = pipeline_id
    ex.status     = ExecutionStatus.PENDING
    ex.error      = None
    ex.started_at = None
    ex.finished_at = None
    return ex


def make_step(
    id: int = 1,
    step_type: StepType = StepType.TRANSFORM,
    config: dict = None,
    position: int = 0,
    retry_count: int = 0,
    timeout_seconds: int = 30,
    enabled: bool = True,
) -> PipelineStep:
    step = MagicMock(spec=PipelineStep)
    step.id             = id
    step.step_type      = step_type
    step.config         = json.dumps(config or {})
    step.position       = position
    step.retry_count    = retry_count
    step.timeout_seconds = timeout_seconds
    step.enabled        = enabled
    step.name           = f"step_{id}"
    return step


def make_db():
    db = AsyncMock()
    db.add    = MagicMock()
    db.flush  = AsyncMock()
    db.commit = AsyncMock()
    return db


# ──────────────────────────────────────────
#  Step runner unit tests
# ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_transform_identity():
    """Transform with 'data' expression returns input unchanged."""
    result = await run_transform({"transform_fn": "data"}, {"data": {"key": "value"}})
    assert result["transformed"] == {"key": "value"}


@pytest.mark.asyncio
async def test_run_transform_custom_expression():
    result = await run_transform(
        {"transform_fn": "{'count': len(data)}"},
        {"data": [1, 2, 3]}
    )
    assert result["transformed"] == {"count": 3}


@pytest.mark.asyncio
async def test_run_http_fetch_success():
    import respx
    import httpx

    with respx.mock:
        respx.get("https://api.example.com/data").mock(
            return_value=httpx.Response(200, json={"items": [1, 2, 3]})
        )
        result = await run_http_fetch(
            {"url": "https://api.example.com/data", "method": "GET"},
            {}
        )
    assert result["status_code"] == 200
    assert result["data"] == {"items": [1, 2, 3]}


# ──────────────────────────────────────────
#  Executor integration tests
# ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_execute_pipeline_success():
    """Full pipeline with one transform step — succeeds."""
    execution = make_execution()
    step      = make_step(
        step_type=StepType.TRANSFORM,
        config={"transform_fn": "{'processed': True}"},
    )
    db = make_db()

    await execute_pipeline(execution, [step], db)

    assert execution.status == ExecutionStatus.SUCCESS
    assert execution.finished_at is not None
    assert db.commit.call_count >= 1


@pytest.mark.asyncio
async def test_execute_pipeline_skips_disabled_steps():
    """Disabled steps are not executed."""
    execution = make_execution()
    step      = make_step(enabled=False)
    db        = make_db()

    await execute_pipeline(execution, [step], db)

    # No StepResult should be added for disabled steps
    # execution still succeeds (no steps run = no failures)
    assert execution.status == ExecutionStatus.SUCCESS


@pytest.mark.asyncio
async def test_execute_pipeline_marks_failed_on_step_error():
    """When a step raises an unrecoverable error, execution is marked FAILED."""
    execution = make_execution()
    step      = make_step(
        step_type=StepType.TRANSFORM,
        config={"transform_fn": "1/0"},  # ZeroDivisionError
        retry_count=0,
    )
    db = make_db()

    await execute_pipeline(execution, [step], db)

    assert execution.status == ExecutionStatus.FAILED
    assert execution.error is not None


@pytest.mark.asyncio
async def test_steps_executed_in_position_order():
    """Steps run in ascending position order regardless of list order."""
    order = []

    async def mock_runner(config, context):
        order.append(config["position"])
        return {}

    execution = make_execution()
    steps = [
        make_step(id=1, position=2, config={"position": 2}),
        make_step(id=2, position=0, config={"position": 0}),
        make_step(id=3, position=1, config={"position": 1}),
    ]
    db = make_db()

    with patch.dict(
        "app.services.pipeline.executor.RUNNERS",
        {StepType.TRANSFORM: mock_runner}
    ):
        for s in steps:
            s.step_type = StepType.TRANSFORM
        await execute_pipeline(execution, steps, db)

    assert order == [0, 1, 2]
