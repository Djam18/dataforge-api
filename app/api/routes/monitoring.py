"""Monitoring endpoints — health, readiness, execution stats.

Learning point: separate health from readiness.
- /health: is the process alive? (liveness probe)
- /ready:  can it serve traffic? checks DB + Redis + Celery (readiness probe)

Kubernetes uses both. Health failures → restart pod.
Readiness failures → remove from load balancer rotation (no restart).
"""

import time
import logging
from datetime import datetime, timedelta, UTC

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.pipeline import Pipeline, PipelineExecution, ExecutionStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])

_startup_time = time.time()


@router.get("/health")
async def health() -> dict:
    """Liveness probe — always returns 200 if the process is up."""
    return {
        "status": "ok",
        "uptime_seconds": int(time.time() - _startup_time),
        "timestamp": datetime.now(UTC).isoformat(),
    }


@router.get("/ready")
async def readiness(db: AsyncSession = Depends(get_db)) -> dict:
    """Readiness probe — checks DB connectivity."""
    checks: dict[str, str] = {}

    # Database check
    try:
        await db.execute(select(func.now()))
        checks["database"] = "ok"
    except Exception as exc:
        logger.error("[monitoring] DB check failed: %s", exc)
        checks["database"] = "error"

    # Redis check (via Celery app ping)
    try:
        from app.workers.celery_app import celery_app
        celery_app.backend.client.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        logger.warning("[monitoring] Redis check failed: %s", exc)
        checks["redis"] = "error"

    all_ok = all(v == "ok" for v in checks.values())
    return {
        "status": "ready" if all_ok else "degraded",
        "checks": checks,
        "timestamp": datetime.now(UTC).isoformat(),
    }


@router.get("/stats")
async def execution_stats(
    hours: int = 24,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Execution statistics for the last N hours.

    Returns counts by status plus a p99-style duration approximation.
    Real percentiles require a time-series DB (Prometheus/InfluxDB) —
    here we just do a simple average from the SQL layer.
    """
    since = datetime.now(UTC) - timedelta(hours=hours)

    # Counts per status
    status_rows = await db.execute(
        select(PipelineExecution.status, func.count().label("n"))
        .where(PipelineExecution.started_at >= since)
        .group_by(PipelineExecution.status)
    )
    counts = {row.status.value: row.n for row in status_rows}

    # Average duration for completed executions
    duration_row = await db.execute(
        select(
            func.avg(
                func.extract(
                    "epoch",
                    PipelineExecution.finished_at - PipelineExecution.started_at,
                )
            ).label("avg_seconds")
        )
        .where(
            PipelineExecution.status == ExecutionStatus.SUCCESS,
            PipelineExecution.started_at >= since,
        )
    )
    avg_seconds = duration_row.scalar() or 0

    # Pipeline count
    total_pipelines = await db.scalar(select(func.count()).select_from(Pipeline))

    return {
        "period_hours": hours,
        "total_pipelines": total_pipelines,
        "executions": counts,
        "avg_success_duration_seconds": round(float(avg_seconds), 2),
        "since": since.isoformat(),
    }
