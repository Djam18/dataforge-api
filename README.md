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

## Architecture

```
dataforge-api/
  app/
    api/routes/        — FastAPI routers (pipelines, executions, monitoring)
    models/            — SQLAlchemy 2.0 mapped_column models
    services/
      pipeline/        — execution engine, step runners, retry logic
      scheduler/       — APScheduler cron/interval triggers
      alerts.py        — Slack + email failure notifications
      lineage.py       — data lineage DAG (Redis-backed)
    workers/           — Celery task definitions (run, cancel)
    plugins/           — entry-point step type registry + CSV reader example
    telemetry.py       — OpenTelemetry traces, metrics, @traced decorator
  tests/
    test_pipelines/    — executor unit tests, step runner mocks
```

## Features

| Feature | Implementation |
|---------|---------------|
| Pipeline CRUD | FastAPI + Pydantic V2 + SQLAlchemy 2.0 |
| Async execution | Celery 5 + Redis broker |
| Cron scheduling | APScheduler + RedisJobStore |
| Step types | http_fetch, sql_query, transform, webhook + plugins |
| Plugin system | Python entry points (`dataforge.steps` group) |
| Data lineage | Schema snapshots per step, Redis DAG, 30-day TTL |
| Observability | OpenTelemetry auto-instrumentation + custom histogram |
| Alerts | Slack webhook + SMTP email on failure/recovery |
| Monitoring | /health (liveness) + /ready (readiness) + /stats |

## Run

```bash
# API server
uvicorn app.main:app --reload

# Celery worker
celery -A app.workers.celery_app worker --loglevel=info

# Celery beat (scheduler)
celery -A app.workers.celery_app beat --loglevel=info

# Tests
pytest --cov=app --cov-report=term-missing
```

## Key Learning Points

**FastAPI vs Django for ETL orchestration:**
FastAPI wins here because pipelines are inherently async (HTTP fetches,
DB queries, file I/O can all run concurrently). Django's sync ORM would
require `sync_to_async` wrappers everywhere.

**Celery + asyncio bridge:**
Celery tasks are sync by default. The pattern used here is `asyncio.run()`
inside each task — creates a fresh event loop per task. Works well for
isolated tasks. Don't share event loops between tasks.

**Why not Airflow?**
Airflow is great for complex DAG dependencies and has a rich ecosystem.
DataForge is for simpler use cases where the overhead of Airflow's
scheduler, metadata DB, and worker architecture is too much. Same lesson
as Django vs FastAPI — right tool for the job.

**Plugin pattern via entry points:**
`importlib.metadata.entry_points(group="dataforge.steps")` discovers
installed plugins with zero configuration. Same mechanism powers pytest
plugins, Flask extensions, and Celery serialisers.

**Lineage vs observability:**
Observability answers "did it run?" Lineage answers "what data did it
touch and produce?" Both are needed for production ETL. OpenTelemetry for
the former, custom Redis DAG for the latter (until a proper OpenLineage
integration is warranted).
