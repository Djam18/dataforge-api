"""Celery task definitions.

Pattern: Celery task → create asyncio event loop → run async executor.
This is the bridge between Celery's sync world and FastAPI's async world.
"""

import asyncio
import logging
from datetime import datetime, timezone

from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="dataforge.run_pipeline",
    max_retries=0,  # retries handled inside executor per-step
    time_limit=3600,  # hard limit: 1h per pipeline execution
    soft_time_limit=3300,
)
def run_pipeline_task(self, execution_id: int) -> dict:
    """Execute a pipeline. Called by the API when a run is triggered.

    Celery tasks must be sync — asyncio.run() bridges to the async executor.
    """
    logger.info(f"[celery] starting execution_id={execution_id}")

    async def _run():
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        import os
        from app.models.pipeline import PipelineExecution, ExecutionStatus

        engine = create_async_engine(
            os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./dataforge.db"),
            echo=False,
        )
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            # Load execution + pipeline + steps in one query
            result = await db.execute(
                select(PipelineExecution)
                .where(PipelineExecution.id == execution_id)
                .options(
                    selectinload(PipelineExecution.pipeline).selectinload(
                        type(PipelineExecution.pipeline.property.mapper.class_).steps
                    )
                )
            )
            execution = result.scalar_one_or_none()

            if not execution:
                logger.error(f"[celery] execution_id={execution_id} not found")
                return {"error": "execution not found"}

            if execution.status != ExecutionStatus.PENDING:
                logger.warning(f"[celery] execution already {execution.status!r} — skipping")
                return {"skipped": True, "status": execution.status}

            execution.celery_task_id = self.request.id

            from app.services.pipeline.executor import execute_pipeline
            await execute_pipeline(execution, execution.pipeline.steps, db)

            return {"execution_id": execution_id, "status": execution.status.value}

    return asyncio.run(_run())


@celery_app.task(name="dataforge.cancel_execution")
def cancel_execution_task(execution_id: int) -> dict:
    """Mark an execution as cancelled. The executor checks status before each step."""
    async def _cancel():
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker
        import os
        from app.models.pipeline import PipelineExecution, ExecutionStatus

        engine = create_async_engine(
            os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./dataforge.db")
        )
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            execution = await db.get(PipelineExecution, execution_id)
            if execution and execution.status == ExecutionStatus.RUNNING:
                execution.status      = ExecutionStatus.CANCELLED
                execution.finished_at = datetime.now(tz=timezone.utc)
                await db.commit()
                return {"cancelled": True}
            return {"cancelled": False}

    return asyncio.run(_cancel())
