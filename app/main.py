"""FastAPI application entrypoint.

Python 3.13 + FastAPI 0.115 upgrade notes:

Python 3.13:
  - asyncio is ~30% faster for task scheduling (measured in microbenchmarks)
  - PEP 702 @deprecated is stdlib now — no more third-party warnings package
  - TypeVar defaults (PEP 696) make our generic ApiResponse cleaner
  - Better error messages on type errors in strict mypy mode

FastAPI 0.115:
  - lifespan() context manager is the canonical startup/shutdown (not @app.on_event)
  - Better OpenAPI generation — discriminated unions, better $ref handling
  - query_string_validator for custom query param parsing
  - Annotated[] is now the preferred way to declare dependencies

The @app.on_event("startup") pattern still works but is deprecated.
Lifespan is cleaner — startup and shutdown are colocated, resource cleanup
is guaranteed even if startup fails halfway through.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import pipelines, monitoring
from app.db import init_db
from app.plugins import load_plugins
from app.services.scheduler.scheduler import get_scheduler
from app.telemetry import setup_tracing


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan — startup then shutdown.

    Replaces the old @app.on_event("startup") / @app.on_event("shutdown") pair.
    Resources acquired before 'yield' are released in the finally block after.
    This guarantees cleanup even if startup raises an exception halfway through.
    """
    # Startup
    await init_db()
    load_plugins()

    scheduler = get_scheduler()
    scheduler.start()

    setup_tracing(app)

    yield  # ← application runs here

    # Shutdown
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="DataForge API",
    description="ETL pipeline orchestrator — define, schedule, monitor data pipelines",
    version="0.3.0",
    lifespan=lifespan,
    # FastAPI 0.115: generate_unique_id_function for consistent operation IDs
    # Used by OpenAPI clients (openapi-generator, orval) for clean method names
    generate_unique_id_function=lambda route: f"{route.tags[0]}-{route.name}" if route.tags else route.name,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in prod via env var
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipelines.router)
app.include_router(monitoring.router)
