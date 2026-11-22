"""Gold core for the download monitor: reference tables and `fct_data_downloads`.

All joins happen once, in the fact table; the suspicious-activity views, dashboard, alerts and
export are filters over it. The logic lives in dm_common/gold_logic.py (tested on local Spark);
this file only reads tables. No non-deterministic time functions anywhere, so the views can
refresh incrementally. Rolling windows belong in dashboard and alert queries.

Pipeline configuration keys:
  bronze_catalog, bronze_schema   where the dm_bronze tables live
"""

from pyspark import pipelines as dp

from dm_common import gold_logic as G
from dm_common.config_data import CONFIG

INFO = "system.information_schema"


def _bronze(name: str):
    catalog, schema = spark.conf.get("bronze_catalog"), spark.conf.get("bronze_schema")
    return spark.read.table(f"`{catalog}`.`{schema}`.`{name}`")


@dp.materialized_view(
    name="ref_supported_ip_ranges",
    comment="Approved IPv4 ranges (from config/download_monitor/supported_ip_ranges.csv).",
)
def ref_supported_ip_ranges():
    return G.ref_ip_ranges(spark, CONFIG["ip_ranges"])


@dp.materialized_view(
    name="dim_pii_columns",
    comment="Columns carrying the PII governed tag (column level only).",
)
def dim_pii_columns():
    tag = CONFIG["thresholds"]["pii_tag_name"]
    return G.pii_columns(spark.read.table(f"{INFO}.column_tags"), tag)


@dp.materialized_view(
    name="dim_pii_tables",
    comment="Tables treated as PII: a PII column, or a PII tag on the table, schema or catalog (inherited).",
)
def dim_pii_tables():
    tag = CONFIG["thresholds"]["pii_tag_name"]
    read = lambda t: spark.read.table(f"{INFO}.{t}")  # noqa: E731
    return G.pii_tables(read("column_tags"), read("table_tags"), read("schema_tags"), read("catalog_tags"), tag)


@dp.materialized_view(
    name="fct_data_downloads",
    comment="One row per download: explicit downloads (audit) and results pulled by external clients "
            "(query history), with channel, IP status and PII evidence.",
    cluster_by=["event_date", "user_email"],
)
def fct_data_downloads():
    return G.build_fct_data_downloads(
        audit=_bronze("bronze_audit"),
        history=_bronze("bronze_query_history"),
        col_lineage=_bronze("bronze_column_lineage"),
        tbl_lineage=_bronze("bronze_table_lineage"),
        pii_cols=spark.read.table("dim_pii_columns"),
        pii_tbls=spark.read.table("dim_pii_tables"),
        ranges=spark.read.table("ref_supported_ip_ranges"),
        cfg=CONFIG,
    )
