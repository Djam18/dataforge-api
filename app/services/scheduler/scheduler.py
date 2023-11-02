"""APScheduler integration — cron/interval triggers for pipelines.

Airflow uses DAGs defined in Python files. Too heavy for our use case.
APScheduler + database job store is sufficient for <100 scheduled pipelines.

Each Pipeline with a schedule field gets an APScheduler job.
On trigger: enqueue a Celery task (don't run inline — Celery for durability).
"""

import logging
import os
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _build_scheduler() -> AsyncIOScheduler:
    jobstores = {
        "default": RedisJobStore(
            jobs_key="dataforge:apscheduler:jobs",
            run_times_key="dataforge:apscheduler:run_times",
            host=REDIS_URL,
        )
    }
    executors = {"default": AsyncIOExecutor()}
    return AsyncIOScheduler(
        jobstores=jobstores,
        executors=executors,
        timezone="UTC",
    )


scheduler: AsyncIOScheduler = _build_scheduler()


async def _trigger_pipeline(pipeline_id: int) -> None:
    """Called by APScheduler when a pipeline's cron fires."""
    from app.workers.tasks import run_pipeline_task
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker
    from app.models.pipeline import PipelineExecution, ExecutionStatus

    logger.info(f"[scheduler] triggering pipeline_id={pipeline_id}")

    engine = create_async_engine(os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./dataforge.db"))
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as db:
        execution = PipelineExecution(
            pipeline_id=pipeline_id,
            status=ExecutionStatus.PENDING,
            triggered_by="scheduler",
        )
        db.add(execution)
        await db.commit()
        await db.refresh(execution)

    # Enqueue Celery task asynchronously
    run_pipeline_task.delay(execution.id)
    logger.info(f"[scheduler] enqueued execution_id={execution.id}")


def schedule_pipeline(pipeline_id: int, cron_expr: str) -> None:
    """Add or replace an APScheduler job for this pipeline."""
    job_id  = f"pipeline:{pipeline_id}"
    trigger = CronTrigger.from_crontab(cron_expr, timezone="UTC")

    scheduler.add_job(
        _trigger_pipeline,
        trigger=trigger,
        id=job_id,
        args=[pipeline_id],
        replace_existing=True,
        misfire_grace_time=300,  # Allow 5min late fires
    )
    logger.info(f"[scheduler] scheduled pipeline_id={pipeline_id} cron={cron_expr!r}")


def unschedule_pipeline(pipeline_id: int) -> None:
    job_id = f"pipeline:{pipeline_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info(f"[scheduler] removed job for pipeline_id={pipeline_id}")


def get_next_run(pipeline_id: int) -> datetime | None:
    job = scheduler.get_job(f"pipeline:{pipeline_id}")
    return job.next_run_time if job else None
