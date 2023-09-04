# dataforge-api

ETL pipeline orchestrator — define, schedule, monitor data pipelines via REST API.

Inspired by Airflow but much simpler. Built with FastAPI + Celery + PostgreSQL.

## Stack

- FastAPI 0.103+ + Pydantic V2 (model_config = ConfigDict)
- Celery 5 + Redis (task broker + result backend)
- async SQLAlchemy 2.0 + Alembic
- APScheduler (cron/interval triggers)
- OpenTelemetry (traces + metrics)
- pytest 80%+ coverage

## Architecture

```
dataforge-api/
  app/
    api/routes/        — FastAPI routers (pipelines, executions, sources)
    models/            — SQLAlchemy 2.0 models
    services/
      pipeline/        — execution engine, step runners
      scheduler/       — APScheduler integration
    workers/           — Celery task definitions
  tests/
```

## Run

```bash
# API server
uvicorn app.main:app --reload

# Celery worker
celery -A app.workers.celery_app worker --loglevel=info

# Celery beat (scheduler)
celery -A app.workers.celery_app beat --loglevel=info
```
