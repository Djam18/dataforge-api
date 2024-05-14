"""Plugin system — dynamic step type registration via entry points.

Learning point: Python entry points (importlib.metadata) are the cleanest
way to build extensible plugin systems. No manual config files.
Third-party packages declare:

    [project.entry-points."dataforge.steps"]
    my_step = "my_package.steps:run_my_step"

And DataForge discovers them automatically at startup.

This is how pytest plugins work (pytest11 entry point), how Flask
extensions register themselves, and how Celery finds custom serialisers.

Alternative: explicit INSTALLED_PLUGINS list in config (Django-style).
Tradeoff: entry points = zero config for end users, harder to debug;
explicit list = more ceremony, easier to reason about.

We use entry points here because the use case (ETL step runners)
benefits from clean third-party extensibility without touching core code.
"""

import logging
from typing import Callable, Awaitable, Any

logger = logging.getLogger(__name__)

# Registry: step_type_name → async runner function
# Built-in runners from executor.py are registered at startup.
_STEP_REGISTRY: dict[str, Callable[..., Awaitable[Any]]] = {}


def register_step(name: str, runner: Callable[..., Awaitable[Any]]) -> None:
    """Register a step runner function under a given type name.

    runner signature: async (step_config: dict, context: dict) -> dict
    """
    if name in _STEP_REGISTRY:
        logger.warning("[plugins] Step type '%s' already registered — overwriting", name)
    _STEP_REGISTRY[name] = runner
    logger.info("[plugins] Registered step type: %s", name)


def get_runner(step_type: str) -> Callable[..., Awaitable[Any]] | None:
    """Retrieve a registered runner by step type name."""
    return _STEP_REGISTRY.get(step_type)


def list_step_types() -> list[str]:
    """Return all currently registered step type names."""
    return sorted(_STEP_REGISTRY.keys())


def load_plugins() -> None:
    """Discover and load all plugins registered via entry points.

    Looks for the 'dataforge.steps' entry point group. Each entry point
    should resolve to an async callable matching the runner signature.

    Called once at application startup in main.py lifespan.
    """
    try:
        from importlib.metadata import entry_points
        eps = entry_points(group="dataforge.steps")
        for ep in eps:
            try:
                runner = ep.load()
                register_step(ep.name, runner)
                logger.info("[plugins] Loaded plugin step: %s from %s", ep.name, ep.value)
            except Exception as exc:
                logger.error("[plugins] Failed to load plugin '%s': %s", ep.name, exc)
    except Exception as exc:
        logger.warning("[plugins] Entry point discovery failed: %s", exc)

    # Always register built-in step types after plugins
    # (plugins can override builtins if they want)
    _register_builtins()


def _register_builtins() -> None:
    """Register the built-in step runners from executor.py."""
    from app.services.pipeline.executor import (
        run_http_fetch,
        run_sql_query,
        run_transform,
        run_http_webhook,
    )
    from app.models.pipeline import StepType

    builtins = {
        StepType.HTTP_FETCH.value: run_http_fetch,
        StepType.SQL_QUERY.value:  run_sql_query,
        StepType.TRANSFORM.value:  run_transform,
        StepType.WEBHOOK.value:    run_http_webhook,
    }
    for name, runner in builtins.items():
        if name not in _STEP_REGISTRY:  # don't overwrite plugin overrides
            register_step(name, runner)
