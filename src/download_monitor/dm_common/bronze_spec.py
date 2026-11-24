"""What the bronze layer copies from the system tables (shared by the pipeline and its tests)."""

from __future__ import annotations

import re

# Never allow a full refresh: the source keeps only its retention window, so a full
# refresh would permanently delete older history from bronze.
TABLE_PROPERTIES = {
    "pipelines.reset.allowed": "false",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
}

BRONZE_TABLES = [
    {
        "name": "bronze_audit",
        "source": "system.access.audit",
        "comment": "Append-only copy of system.access.audit (all services and actions).",
        "cluster_by": ["event_date", "service_name", "action_name"],
        "time_column": "event_date",
        "expect": ("has_event_id", "event_id IS NOT NULL"),
    },
    {
        "name": "bronze_query_history",
        "source": "system.query.history",
        "comment": "Append-only copy of system.query.history (a statement can appear more than once).",
        "cluster_by": ["start_time", "statement_id"],
        "time_column": "start_time",
        "expect": ("has_statement_id", "statement_id IS NOT NULL"),
    },
    {
        "name": "bronze_column_lineage",
        "source": "system.access.column_lineage",
        "comment": "Append-only copy of system.access.column_lineage.",
        "cluster_by": ["event_date", "statement_id"],
        "time_column": "event_date",
        "expect": ("has_source_table", "source_table_full_name IS NOT NULL"),
    },
    {
        "name": "bronze_table_lineage",
        "source": "system.access.table_lineage",
        "comment": "Append-only copy of system.access.table_lineage.",
        "cluster_by": ["event_date", "statement_id"],
        "time_column": "event_date",
        "expect": ("has_source_table", "source_table_full_name IS NOT NULL"),
    },
]

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_start_date(raw: str | None) -> str | None:
    """'' -> None; 'YYYY-MM-DD' -> itself; anything else is rejected (the value ends up in a filter)."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if not _DATE.match(raw):
        raise ValueError(f"bronze_start_date must be YYYY-MM-DD or empty, got {raw!r}")
    return raw
