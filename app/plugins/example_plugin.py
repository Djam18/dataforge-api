"""Example built-in plugin step: CSV reader.

Shows the plugin contract — async function, takes config + context dict,
returns a dict of outputs to merge into the pipeline context.

To ship this as a third-party plugin, a package would add to pyproject.toml:
    [project.entry-points."dataforge.steps"]
    csv_reader = "my_etl_package.steps:run_csv_reader"

Built-in version registered via _register_builtins() for demo purposes.
"""

import csv
import io
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


async def run_csv_reader(step_config: dict, context: dict) -> dict:
    """Read a CSV file and return rows as a list of dicts.

    Config keys:
        path (str): Absolute path or relative to CWD.
        delimiter (str): Column delimiter. Default ",".
        encoding (str): File encoding. Default "utf-8".
        limit (int | None): Max rows to read. Default None (all).

    Output context keys:
        rows (list[dict]): Parsed CSV rows.
        row_count (int): Number of rows read.
        columns (list[str]): Column names from header.
    """
    file_path = step_config.get("path")
    if not file_path:
        raise ValueError("csv_reader step requires 'path' in config")

    delimiter = step_config.get("delimiter", ",")
    encoding  = step_config.get("encoding", "utf-8")
    limit     = step_config.get("limit")

    resolved = Path(file_path).expanduser()
    if not resolved.exists():
        raise FileNotFoundError(f"CSV file not found: {resolved}")

    content = resolved.read_text(encoding=encoding)
    reader  = csv.DictReader(io.StringIO(content), delimiter=delimiter)

    rows: list[dict] = []
    for i, row in enumerate(reader):
        if limit and i >= limit:
            break
        rows.append(dict(row))

    columns = list(rows[0].keys()) if rows else []
    logger.info("[csv_reader] Read %d rows from %s", len(rows), resolved)

    return {
        "rows": rows,
        "row_count": len(rows),
        "columns": columns,
    }
