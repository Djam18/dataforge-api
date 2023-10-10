"""Celery application — async task queue for pipeline execution.

Why Celery and not just asyncio BackgroundTasks?
- BackgroundTasks dies with the request worker (no retry, no persistence).
- Celery persists tasks in Redis, retries on failure, shows in Flower dashboard.
- For a pipeline orchestrator, we need durable execution guarantees.

This is the same lesson as FastAPI vs Django: use the right tool.
BackgroundTasks for fire-and-forget. Celery for durable, monitored work.
"""

import os
import asyncio
import logging
from celery import Celery

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "dataforge",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,        # Re-queue if worker dies mid-task
    worker_prefetch_multiplier=1,  # One task at a time per worker (important for long tasks)
    result_expires=86400,       # Keep results 24h
)
