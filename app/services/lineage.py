"""Data lineage graph — tracks what data flowed through each pipeline step.

Learning point: lineage is different from observability.
- Observability = did the pipeline run? how long? did it fail?
- Lineage = what data existed before the run, what did each step produce,
  how does data trace back to its origin?

Lineage matters for:
  - Compliance (GDPR, HIPAA) — show exactly which raw records produced output X
  - Debugging — understand why a downstream report changed
  - Impact analysis — "if I change step 3, what breaks downstream?"

Implementation here is a lightweight adjacency-list DAG stored in Redis.
A proper solution would use a dedicated lineage store (OpenLineage / Marquez).

Each "lineage node" captures:
  - execution_id, step_id, step_name
  - input_snapshot: keys/shape of data flowing in (not raw data — just schema)
  - output_snapshot: keys/shape of data flowing out
  - row_count: how many records processed (if applicable)
  - parents: list of step node IDs this step depended on
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, UTC
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class LineageNode:
    node_id: str           # f"exec:{execution_id}:step:{step_id}"
    execution_id: int
    step_id: int
    step_name: str
    step_type: str
    started_at: str        # ISO-8601
    finished_at: Optional[str]
    input_schema: dict     # {key: type_name} — snapshot of context keys
    output_schema: dict    # {key: type_name} — keys this step added
    row_count: Optional[int]
    status: str            # success | failed | skipped
    parents: list[str] = field(default_factory=list)  # parent node_ids


def _schema_of(data: dict) -> dict:
    """Extract a {key: typename} schema snapshot — never stores actual values."""
    return {k: type(v).__name__ for k, v in data.items()}


def _get_redis():
    """Lazy Redis client — returns None if Redis not configured."""
    try:
        import redis
        import os
        client = redis.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        client.ping()
        return client
    except Exception:
        return None


_LINEAGE_TTL = 60 * 60 * 24 * 30  # 30 days


def record_step_lineage(
    execution_id: int,
    step_id: int,
    step_name: str,
    step_type: str,
    context_before: dict,
    context_after: dict,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    parent_step_ids: list[int],
    row_count: Optional[int] = None,
) -> Optional[LineageNode]:
    """Persist a lineage node for one step execution.

    Returns the node (even if Redis is unavailable — caller can log it).
    """
    node_id = f"exec:{execution_id}:step:{step_id}"
    parent_ids = [f"exec:{execution_id}:step:{pid}" for pid in parent_step_ids]

    # Only snapshot keys added/changed by this step
    new_keys = set(context_after) - set(context_before)
    output_schema = {k: type(context_after[k]).__name__ for k in new_keys}

    node = LineageNode(
        node_id=node_id,
        execution_id=execution_id,
        step_id=step_id,
        step_name=step_name,
        step_type=step_type,
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        input_schema=_schema_of(context_before),
        output_schema=output_schema,
        row_count=row_count,
        status=status,
        parents=parent_ids,
    )

    client = _get_redis()
    if client:
        try:
            key = f"lineage:{node_id}"
            client.setex(key, _LINEAGE_TTL, json.dumps(asdict(node)))
            # Also add to execution index for fast lookup
            client.sadd(f"lineage:exec:{execution_id}", node_id)
            client.expire(f"lineage:exec:{execution_id}", _LINEAGE_TTL)
        except Exception as exc:
            logger.warning("[lineage] Redis write failed: %s", exc)

    return node


def get_execution_lineage(execution_id: int) -> list[LineageNode]:
    """Retrieve all lineage nodes for an execution, sorted by start time."""
    client = _get_redis()
    if not client:
        return []

    try:
        node_ids = client.smembers(f"lineage:exec:{execution_id}")
        nodes: list[LineageNode] = []
        for node_id in node_ids:
            raw = client.get(f"lineage:{node_id}")
            if raw:
                nodes.append(LineageNode(**json.loads(raw)))
        return sorted(nodes, key=lambda n: n.started_at)
    except Exception as exc:
        logger.warning("[lineage] Redis read failed: %s", exc)
        return []


def build_lineage_graph(execution_id: int) -> dict:
    """Build a DAG representation suitable for frontend visualisation.

    Returns:
        {
          "nodes": [{"id": ..., "label": ..., "status": ...}, ...],
          "edges": [{"from": ..., "to": ...}, ...],
        }
    """
    nodes_raw = get_execution_lineage(execution_id)

    nodes = [
        {
            "id": n.node_id,
            "label": n.step_name,
            "type": n.step_type,
            "status": n.status,
            "input_schema": n.input_schema,
            "output_schema": n.output_schema,
            "row_count": n.row_count,
            "started_at": n.started_at,
            "finished_at": n.finished_at,
        }
        for n in nodes_raw
    ]

    edges = [
        {"from": parent_id, "to": n.node_id}
        for n in nodes_raw
        for parent_id in n.parents
    ]

    return {
        "execution_id": execution_id,
        "nodes": nodes,
        "edges": edges,
    }
