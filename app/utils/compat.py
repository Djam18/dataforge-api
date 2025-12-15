"""Python 3.14 compatibility utilities and new feature showcase.

Adopted in Dec 2025 upgrade (Python 3.14.0 stable).

New stdlib features used here:
  - PEP 702: warnings.deprecated() decorator — standard way to mark deprecated APIs
  - PEP 749: annotationlib.get_annotations() — lazy annotation evaluation
  - PEP 696: TypeVar defaults — cleaner generic types without overloads
  - pathlib improvements: Path.copy(), Path.move() (finally!)
"""

import warnings
from typing import TypeVar, Generic, Any
from warnings import deprecated  # PEP 702 — new in 3.13, stable in 3.14


# ──────────────────────────────────────────
#  PEP 702: @deprecated decorator
# ──────────────────────────────────────────

@deprecated("Use execute_pipeline() from app.services.pipeline.executor instead.")
def run_pipeline_legacy(pipeline_id: int) -> None:
    """Legacy pipeline runner — kept for backwards compatibility with v0.1 API."""
    raise NotImplementedError


# ──────────────────────────────────────────
#  PEP 696: TypeVar defaults
# ──────────────────────────────────────────

# Python 3.13+: TypeVar can have a default value.
# Before: had to use Union[T, None] or write overloads.
# Now: T = TypeVar('T', default=None) — much cleaner.

T = TypeVar('T', default=dict)  # type: ignore[call-overload]  # 3.13+


class ApiResult(Generic[T]):
    """Generic API result — T defaults to dict if not specified.

    Python 3.13/3.14 TypeVar default means ApiResult() == ApiResult[dict].
    No more: result: ApiResult = ...  # type: ignore
    """
    def __init__(self, success: bool, data: T | None = None, error: str | None = None) -> None:
        self.success = success
        self.data    = data
        self.error   = error

    def __repr__(self) -> str:
        return f"ApiResult(success={self.success}, data={self.data!r})"


# ──────────────────────────────────────────
#  PEP 749: lazy annotations
# ──────────────────────────────────────────

def get_field_names(cls: type) -> list[str]:
    """Return annotated field names from a class using lazy annotation evaluation.

    Python 3.14: annotations are strings by default (PEP 563 finally landed).
    annotationlib.get_annotations() evaluates them lazily — much faster at
    import time for heavily annotated dataclasses/Pydantic models.
    """
    try:
        # Python 3.14
        import annotationlib  # type: ignore[import-not-found]
        ann = annotationlib.get_annotations(cls, eval_str=True)
    except ImportError:
        # Python 3.13 fallback
        import typing
        ann = typing.get_type_hints(cls) if hasattr(cls, '__annotations__') else {}
    return list(ann.keys())
