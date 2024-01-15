"""OpenTelemetry instrumentation — traces and metrics.

Learning point: observability in microservices/async apps is much harder than
in a Django monolith. In Django: one request = one thread = easy traceback.
In FastAPI async: one request spans multiple coroutines, possibly multiple workers.

OpenTelemetry provides vendor-neutral tracing. Export to Jaeger (local) or
Honeycomb/Datadog (prod). The FastAPI auto-instrumentation catches all routes.

Metrics: custom Histogram for pipeline execution duration.
"""

import os
import logging
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
#  Tracing setup
# ──────────────────────────────────────────

def setup_tracing(app) -> None:
    """Attach OpenTelemetry auto-instrumentation to a FastAPI app."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        resource = Resource.create({"service.name": "dataforge-api", "service.version": "0.1.0"})
        provider = TracerProvider(resource=resource)

        # Exporter: Jaeger (local) or OTLP (prod)
        otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        if otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        else:
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter
            exporter = ConsoleSpanExporter()

        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        FastAPIInstrumentor.instrument_app(app)
        logger.info("[telemetry] OpenTelemetry tracing configured")

    except ImportError:
        logger.warning("[telemetry] opentelemetry packages not installed — tracing disabled")


# ──────────────────────────────────────────
#  Metrics
# ──────────────────────────────────────────

_execution_histogram = None

def _get_histogram():
    global _execution_histogram
    if _execution_histogram is None:
        try:
            from opentelemetry import metrics
            meter = metrics.get_meter("dataforge.pipeline")
            _execution_histogram = meter.create_histogram(
                "pipeline.execution.duration_ms",
                description="Pipeline execution duration in milliseconds",
                unit="ms",
            )
        except Exception:
            pass
    return _execution_histogram


def record_execution_duration(pipeline_id: int, duration_ms: int, status: str) -> None:
    """Record pipeline execution duration metric."""
    hist = _get_histogram()
    if hist:
        hist.record(duration_ms, {"pipeline_id": str(pipeline_id), "status": status})


# ──────────────────────────────────────────
#  Trace decorator for async functions
# ──────────────────────────────────────────

def traced(span_name: str = ""):
    """Decorator that wraps an async function in an OpenTelemetry span."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                from opentelemetry import trace
                tracer   = trace.get_tracer("dataforge")
                name     = span_name or f"{func.__module__}.{func.__qualname__}"
                with tracer.start_as_current_span(name):
                    return await func(*args, **kwargs)
            except Exception:
                # If tracing fails, still run the function
                return await func(*args, **kwargs)
        return wrapper
    return decorator
