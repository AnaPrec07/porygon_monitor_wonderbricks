"""Gold-layer transformations as plain DataFrame functions.

The pipeline files (gold/*.py) only read tables and call these functions, so everything
here runs against small local DataFrames in tests. Column names follow the system tables;
`ensure_cols` adds missing optional columns as NULL so a column that does not exist in a
given workspace degrades a signal instead of failing the update.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------- generic helpers

AUDIT_COLS = {
    "event_id": "string", "event_time": "timestamp", "event_date": "date", "workspace_id": "string",
    "service_name": "string", "action_name": "string", "source_ip_address": "string",
    "user_agent": "string", "request_params": "map<string,string>",
    "user_identity": "struct<email:string>",
}
QUERY_COLS = {
    "statement_id": "string", "workspace_id": "string", "executed_by": "string",
    "client_application": "string", "client_driver": "string", "execution_status": "string",
    "statement_type": "string", "statement_text": "string", "produced_rows": "bigint",
    "read_bytes": "bigint", "start_time": "timestamp", "end_time": "timestamp",
    "update_time": "timestamp",
    "query_source": "struct<dashboard_id:string,legacy_dashboard_id:string,genie_space_id:string,notebook_id:string>",
}


def ensure_cols(df: DataFrame, spec: Mapping[str, str]) -> DataFrame:
    for name, ddl in spec.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(ddl))
    return df


def sql_str(value: Any) -> str:
    """Quote a config value as a SQL string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


# ---------------------------------------------------------------- IP handling

_V4 = r"^\d{1,3}(\.\d{1,3}){3}$"


def clean_ip(col: str | Column) -> Column:
    """Twin of classify.strip_port: drop the port, brackets and blanks."""
    c = F.col(col) if isinstance(col, str) else col
    t = F.trim(c)
    cleaned = (
        F.when(t.rlike(r"^\d{1,3}(\.\d{1,3}){3}:\d+$"), F.regexp_extract(t, r"^([^:]+)", 1))
        .when(t.rlike(r"^\[[0-9a-fA-F:.]+\](:\d+)?$"), F.regexp_extract(t, r"^\[([^\]]+)\]", 1))
        .otherwise(t)
    )
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def ip_to_int(ip: Column) -> Column:
    parts = F.split(ip, r"\.")
    valid = ip.rlike(_V4) & (F.array_max(F.transform(parts, lambda x: x.cast("int"))) <= 255)
    value = (
        parts[0].cast("bigint") * 16777216 + parts[1].cast("bigint") * 65536
        + parts[2].cast("bigint") * 256 + parts[3].cast("bigint")
    )
    return F.when(valid, value)


def ranges_array(ranges: DataFrame) -> DataFrame:
    """One-row DataFrame `_ranges` (array of struct(start_int, end_int)) to cross join onto a fact."""
    return ranges.agg(F.collect_list(F.struct("start_int", "end_int")).alias("_ranges"))


def with_ip_status(df: DataFrame, ip_col: str, ranges: DataFrame, out: str = "ip_status") -> DataFrame:
    ip = F.col(ip_col)
    value = ip_to_int(ip)
    in_range = F.exists(F.col("_ranges"), lambda r: (value >= r["start_int"]) & (value <= r["end_int"]))
    status = (
        F.when(ip.isNull(), "unknown")
        .when(ip.isin("0.0.0.0", "127.0.0.1", "::1"), "internal")
        .when(value.isNull(), "unparsed")
        .when(in_range, "trusted")
        .otherwise("untrusted")
    )
    return df.crossJoin(F.broadcast(ranges_array(ranges))).withColumn(out, status).drop("_ranges")


# ---------------------------------------------------------------- reference tables

def ref_ip_ranges(spark: SparkSession, ranges: Iterable[Mapping[str, Any]]) -> DataFrame:
    rows = [(r["cidr"], r["description"], r["owner"], int(r["start_int"]), int(r["end_int"])) for r in ranges]
    return spark.createDataFrame(
        rows, "cidr string, description string, owner string, start_int bigint, end_int bigint"
    )


def _tags(df: DataFrame, tag_name: str, cols: dict[str, str]) -> DataFrame:
    df = ensure_cols(df, {**cols, "tag_name": "string", "tag_value": "string"})
    return df.where(F.lower("tag_name") == tag_name.lower())


def pii_columns(column_tags: DataFrame, tag_name: str) -> DataFrame:
    t = _tags(column_tags, tag_name, {k: "string" for k in
                                      ("catalog_name", "schema_name", "table_name", "column_name")})
    return t.select(
        F.lower("catalog_name").alias("catalog_name"), F.lower("schema_name").alias("schema_name"),
        F.lower("table_name").alias("table_name"), F.lower("column_name").alias("column_name"),
        F.lower(F.concat_ws(".", "catalog_name", "schema_name", "table_name")).alias("table_full_name"),
        "tag_value",
    ).distinct()


def pii_tables(column_tags: DataFrame, table_tags: DataFrame, schema_tags: DataFrame,
               catalog_tags: DataFrame, tag_name: str) -> DataFrame:
    """Tables to treat as PII, with inheritance: a NULL schema/table means 'everything below'."""
    s = "string"
    out = []
    for df, scope, cols in (
        (column_tags, "column", ("catalog_name", "schema_name", "table_name")),
        (table_tags, "table", ("catalog_name", "schema_name", "table_name")),
        (schema_tags, "schema", ("catalog_name", "schema_name")),
        (catalog_tags, "catalog", ("catalog_name",)),
    ):
        t = _tags(df, tag_name, {c: s for c in cols})
        out.append(t.select(
            F.lower("catalog_name").alias("catalog_name"),
            (F.lower("schema_name") if "schema_name" in cols else F.lit(None).cast(s)).alias("schema_name"),
            (F.lower("table_name") if "table_name" in cols else F.lit(None).cast(s)).alias("table_name"),
            F.lit(scope).alias("scope"), "tag_value",
        ))
    u = out[0]
    for o in out[1:]:
        u = u.unionByName(o)
    full = F.when(
        F.col("table_name").isNotNull(),
        F.concat_ws(".", "catalog_name", "schema_name", "table_name"),
    )
    return u.withColumn("table_full_name", full).distinct()


# ---------------------------------------------------------------- download events

def channel_case(cfg: Mapping[str, Any]) -> Column:
    field_sql = {
        "client_application": "lower(client_application)", "client_driver": "lower(client_driver)",
        "dashboard_id": "query_source.dashboard_id", "legacy_dashboard_id": "query_source.legacy_dashboard_id",
        "genie_space_id": "query_source.genie_space_id", "notebook_id": "query_source.notebook_id",
    }

    def cond(c: Mapping[str, Any]) -> str:
        col = field_sql[c["field"]]
        if c.get("not_null"):
            return f"{col} IS NOT NULL"
        return f"instr({col}, {sql_str(str(c['contains']).lower())}) > 0"

    whens = " ".join(
        f"WHEN {' OR '.join(cond(c) for c in r['any'])} THEN {sql_str(r['channel'])}" for r in cfg["rules"]
    )
    return F.expr(f"CASE {whens} ELSE {sql_str(cfg['default_channel'])} END")


def statement_id_from(audit_cols_key: str, keys: list[str]) -> Column:
    return F.coalesce(*[F.col(audit_cols_key)[k] for k in keys])


def explicit_downloads(audit: DataFrame, actions_cfg: Mapping[str, Any]) -> DataFrame:
    audit = ensure_cols(audit, AUDIT_COLS)
    actions = [a for a in actions_cfg["actions"] if a.get("enabled", True)]
    cond = F.lit(False)
    channel = F.lit(None).cast("string")
    for a in actions:
        match = F.col("action_name") == a["action"]
        skip = a.get("ignore_when")
        if skip:
            match = match & ~F.coalesce(F.col("request_params")[skip["param"]] == skip["equals"], F.lit(False))
        cond = cond | match
        channel = F.when(match, a["channel"]).otherwise(channel)
    return (
        audit.where(cond)
        .select(
            F.col("event_id").alias("download_id"), "event_time", F.to_date("event_time").alias("event_date"),
            "workspace_id", F.col("user_identity.email").alias("user_email"),
            F.lit("explicit").alias("download_kind"), "action_name", channel.alias("channel"),
            statement_id_from("request_params", actions_cfg["statement_id_param_keys"]).alias("statement_id"),
            clean_ip("source_ip_address").alias("source_ip"),
            F.lit("direct").alias("ip_attribution"),
        )
    )


def latest_queries(history: DataFrame, channels_cfg: Mapping[str, Any]) -> DataFrame:
    """One row per statement (the latest version), with the client channel."""
    h = ensure_cols(history, QUERY_COLS)
    order = F.coalesce("update_time", "end_time", "start_time").desc_nulls_last()
    w = Window.partitionBy("statement_id").orderBy(order)
    h = h.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return h.select(
        "statement_id", "workspace_id", F.col("executed_by").alias("user_email"), "client_application",
        "client_driver", channel_case(channels_cfg).alias("channel"), "start_time",
        F.col("produced_rows").alias("rows_returned"), "read_bytes", "statement_text",
        "statement_type", "execution_status",
    )


def audit_ip_status(audit: DataFrame, ranges: DataFrame, statement_keys: list[str]) -> DataFrame:
    """Per audit event: user, clean IP, its status and (verbose audit) the statement it belongs to."""
    a = ensure_cols(audit, AUDIT_COLS)
    a = a.select(
        F.col("user_identity.email").alias("user_email"), F.to_date("event_time").alias("event_date"),
        clean_ip("source_ip_address").alias("source_ip"), "service_name",
        statement_id_from("request_params", statement_keys).alias("statement_id"),
    ).where(F.col("source_ip").isNotNull())
    return with_ip_status(a, "source_ip", ranges)


_SEVERITY = {"untrusted": 0, "unparsed": 1, "trusted": 2, "internal": 3, "unknown": 4}


def external_downloads(queries: DataFrame, audit_ips: DataFrame, external_channels: list[str]) -> DataFrame:
    """Results pulled by external clients (Excel, ODBC/JDBC, connectors): every finished SELECT is a download."""
    q = queries.where(
        F.col("channel").isin(external_channels)
        & (F.col("execution_status") == "FINISHED") & (F.col("rows_returned") > 0)
    )
    severity = F.create_map(*[x for k, v in _SEVERITY.items() for x in (F.lit(k), F.lit(v))])
    direct = (
        audit_ips.where((F.col("service_name") == "databrickssql") & F.col("statement_id").isNotNull())
        .withColumn("_sev", severity[F.col("ip_status")])
        .withColumn("_rn", F.row_number().over(Window.partitionBy("statement_id").orderBy("_sev")))
        .where("_rn = 1")
        .select("statement_id", F.col("source_ip").alias("_direct_ip"), F.col("ip_status").alias("_direct_status"))
    )
    inferred = (
        audit_ips.withColumn("_sev", severity[F.col("ip_status")])
        .withColumn("_rn", F.row_number().over(Window.partitionBy("user_email", "event_date").orderBy("_sev")))
        .where("_rn = 1")
        .select("user_email", "event_date", F.col("source_ip").alias("_day_ip"), F.col("ip_status").alias("_day_status"))
    )
    q = q.withColumn("event_date", F.to_date("start_time"))
    j = q.join(direct, "statement_id", "left").join(inferred, ["user_email", "event_date"], "left")
    return j.select(
        F.col("statement_id").alias("download_id"), F.col("start_time").alias("event_time"), "event_date",
        "workspace_id", "user_email", F.lit("external_client").alias("download_kind"),
        F.lit(None).cast("string").alias("action_name"), "channel", "statement_id",
        F.coalesce("_direct_ip", "_day_ip").alias("source_ip"),
        F.when(F.col("_direct_ip").isNotNull(), "verbose_audit")
        .when(F.col("_day_ip").isNotNull(), "inferred").otherwise("none").alias("ip_attribution"),
        F.coalesce("_direct_status", "_day_status", F.lit("unknown")).alias("ip_status"),
    )


# ---------------------------------------------------------------- PII evidence

def _tbl_parts(df: DataFrame) -> DataFrame:
    full = F.lower(F.col("source_table_full_name"))
    p = F.split(full, r"\.")
    return df.withColumn("_full", full).withColumn("_c", p[0]).withColumn("_s", p[1]).withColumn("_t", p[2])


def pii_evidence(downloads: DataFrame, col_lineage: DataFrame, tbl_lineage: DataFrame,
                 pii_cols: DataFrame, pii_tbls: DataFrame, statements: DataFrame) -> DataFrame:
    """Adds pii_evidence (column > table > text > none), pii_columns and pii_tag_values.

    `statements` carries statement_text per statement_id; downloads without a statement can only
    ever have evidence 'none'.
    """
    cl = _tbl_parts(ensure_cols(col_lineage, {"statement_id": "string", "source_table_full_name": "string",
                                              "source_column_name": "string"}))
    col_hits = (
        cl.join(pii_cols, (cl["_full"] == pii_cols["table_full_name"])
                & (F.lower(cl["source_column_name"]) == pii_cols["column_name"]))
        .groupBy(cl["statement_id"])
        .agg(F.collect_set(F.concat_ws(".", "_full", F.lower("source_column_name"))).alias("_cols"),
             F.collect_set("tag_value").alias("_col_tags"))
        .withColumnRenamed("statement_id", "_col_stmt")
    )
    tl = _tbl_parts(ensure_cols(tbl_lineage, {"statement_id": "string", "source_table_full_name": "string"})).distinct()
    cond = ((tl["_c"] == pii_tbls["catalog_name"])
            & (pii_tbls["schema_name"].isNull() | (tl["_s"] == pii_tbls["schema_name"]))
            & (pii_tbls["table_name"].isNull() | (tl["_t"] == pii_tbls["table_name"])))
    tbl_hits = (
        tl.join(pii_tbls, cond).groupBy(tl["statement_id"])
        .agg(F.collect_set("_full").alias("_tbls"), F.collect_set("tag_value").alias("_tbl_tags"))
        .withColumnRenamed("statement_id", "_tbl_stmt")
    )
    named = pii_tbls.where(F.col("table_full_name").isNotNull()).select("table_full_name", "tag_value").distinct()
    text_hits = (
        statements.join(downloads.select("statement_id").distinct(), "statement_id", "left_semi")
        .where(F.col("statement_text").isNotNull())
        .join(named, F.instr(F.lower(F.col("statement_text")), F.col("table_full_name")) > 0)
        .groupBy("statement_id")
        .agg(F.collect_set("tag_value").alias("_txt_tags"))
        .withColumnRenamed("statement_id", "_txt_stmt")
    )
    d = (downloads.join(col_hits, downloads["statement_id"] == col_hits["_col_stmt"], "left")
         .join(tbl_hits, downloads["statement_id"] == tbl_hits["_tbl_stmt"], "left")
         .join(text_hits, downloads["statement_id"] == text_hits["_txt_stmt"], "left"))
    empty = F.array().cast("array<string>")
    tags = F.array_distinct(F.array_union(F.array_union(
        F.coalesce("_col_tags", empty), F.coalesce("_tbl_tags", empty)), F.coalesce("_txt_tags", empty)))
    return (
        d.withColumn("pii_evidence", F.when(F.col("_cols").isNotNull(), "column")
                     .when(F.col("_tbls").isNotNull(), "table")
                     .when(F.col("_txt_tags").isNotNull(), "text").otherwise("none"))
        .withColumn("pii_columns", F.coalesce("_cols", empty))
        .withColumn("pii_tag_values", F.filter(tags, lambda x: x.isNotNull()))
        .drop("_col_stmt", "_tbl_stmt", "_txt_stmt", "_cols", "_col_tags", "_tbls", "_tbl_tags", "_txt_tags")
    )


# ---------------------------------------------------------------- the fact table

def build_fct_data_downloads(audit: DataFrame, history: DataFrame, col_lineage: DataFrame,
                             tbl_lineage: DataFrame, pii_cols: DataFrame, pii_tbls: DataFrame,
                             ranges: DataFrame, cfg: Mapping[str, Any]) -> DataFrame:
    keys = cfg["actions"]["statement_id_param_keys"]
    queries = latest_queries(history, cfg["channels"])
    audit_ips = audit_ip_status(audit, ranges, keys)

    explicit = with_ip_status(explicit_downloads(audit, cfg["actions"]), "source_ip", ranges)
    q = queries.select(
        "statement_id", F.col("channel").alias("_q_channel"), "client_application", "client_driver",
        "rows_returned", "read_bytes", "statement_text",
    )
    explicit = explicit.join(q, "statement_id", "left").withColumn(
        "channel", F.when(F.col("_q_channel").isin("dashboard", "genie"), F.col("_q_channel")).otherwise(F.col("channel"))
    ).drop("_q_channel")

    external = external_downloads(queries, audit_ips, cfg["channels"]["external_client_channels"]).join(
        q.drop("_q_channel").withColumnRenamed("statement_id", "_sid"),
        F.col("statement_id") == F.col("_sid"), "left").drop("_sid")

    cols = ["download_id", "event_time", "event_date", "workspace_id", "user_email", "download_kind",
            "action_name", "channel", "client_application", "client_driver", "statement_id",
            "rows_returned", "read_bytes", "statement_text", "source_ip", "ip_attribution", "ip_status"]
    base = explicit.select(*cols).unionByName(external.select(*cols))

    base = base.withColumn(
        "is_service_principal",
        F.col("user_email").isNotNull() & ~F.col("user_email").contains("@") & (F.col("user_email") != "System-User"),
    )
    stmts = queries.select("statement_id", "statement_text")
    out = pii_evidence(base, col_lineage, tbl_lineage, pii_cols, pii_tbls, stmts)
    return out.withColumn("statement_text_preview", F.substring("statement_text", 1, 200)).drop("statement_text")
