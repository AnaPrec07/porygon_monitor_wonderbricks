"""SQL for the S3 export (pure string building, tested without a workspace)."""

from __future__ import annotations

import re
from typing import Any, Mapping

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_S3 = re.compile(r"^s3://[A-Za-z0-9._-]+(/[A-Za-z0-9._/=-]*)?$")
SOURCE_VIEW = "suspicious_downloads"
TARGET_TABLE = "suspicious_downloads"

TABLE_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
}


def ident(value: str) -> str:
    if not _IDENT.match(value):
        raise ValueError(f"not a safe identifier: {value!r}")
    return f"`{value}`"


def s3_url(value: str) -> str:
    value = (value or "").strip()
    if not _S3.match(value):
        raise ValueError(f"export location must look like s3://bucket/prefix, got {value!r}")
    return value.rstrip("/")


def salt_expr(scope: str, key: str) -> str:
    for part in (scope, key):
        if not _IDENT.match(part.replace("-", "_").replace(".", "_")):
            raise ValueError(f"not a safe secret name: {part!r}")
    return f"secret('{scope}', '{key}')"


def select_list(export_cfg: Mapping[str, Any], salt: str | None) -> str:
    """Approved columns in order; `hash: true` columns become salted SHA-256 (NULL stays NULL)."""
    parts = []
    for col in export_cfg["columns"]:
        name = ident(col["name"])
        if col.get("hash"):
            if not salt:
                raise ValueError(f"column {col['name']} must be hashed but no salt was provided")
            parts.append(f"sha2(concat({salt}, cast({name} AS STRING)), 256) AS {name}")
        else:
            parts.append(name)
    return ", ".join(parts)


def _window(days: int) -> str:
    if not isinstance(days, int) or days < 1:
        raise ValueError("export_window_days must be a positive integer")
    return f"event_date >= date_sub(current_date(), {days})"


def _names(catalog: str, gold_schema: str, export_schema: str) -> tuple[str, str]:
    return (f"{ident(catalog)}.{ident(gold_schema)}.{ident(SOURCE_VIEW)}",
            f"{ident(catalog)}.{ident(export_schema)}.{ident(TARGET_TABLE)}")


def create_table_sql(export_cfg, catalog, gold_schema, export_schema, location, salt) -> str:
    src, dst = _names(catalog, gold_schema, export_schema)
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    return (f"CREATE TABLE IF NOT EXISTS {dst} USING DELTA PARTITIONED BY (event_date) "
            f"LOCATION '{s3_url(location)}' TBLPROPERTIES ({props}) "
            f"AS SELECT {select_list(export_cfg, salt)} FROM {src} WHERE 1 = 0")


def export_sql(export_cfg, catalog, gold_schema, export_schema, salt) -> str:
    """Idempotent: rewrites the trailing window, so late rows are picked up and re-runs never duplicate."""
    src, dst = _names(catalog, gold_schema, export_schema)
    window = _window(export_cfg["export_window_days"])
    return (f"INSERT INTO {dst} REPLACE WHERE {window} "
            f"SELECT {select_list(export_cfg, salt)} FROM {src} WHERE {window}")
