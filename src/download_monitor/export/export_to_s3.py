"""Exports dm_gold.suspicious_downloads to an external Delta (UniForm Iceberg) table on S3.

Reads only the columns listed in config/download_monitor/export_columns.yml, hashes the ones marked
`hash`, and rewrites a trailing window so the job is idempotent. If no S3 location is configured the
task succeeds without doing anything, so the job can be deployed before the export is set up.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # makes dm_common importable

from pyspark.sql import SparkSession  # noqa: E402

from dm_common import export_sql as X  # noqa: E402
from dm_common.config_data import CONFIG  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--gold-schema", required=True)
    p.add_argument("--export-schema", required=True)
    p.add_argument("--s3-url", default="", help="s3://bucket/prefix of the external location; empty skips")
    p.add_argument("--salt-scope", required=True)
    p.add_argument("--salt-key", required=True)
    return p.parse_args()


def main() -> int:
    a = parse_args()
    if not a.s3_url.strip():
        print("dm_export_s3_url is empty: export not configured, nothing to do.")
        return 0
    spark = SparkSession.builder.getOrCreate()
    salt = X.salt_expr(a.salt_scope, a.salt_key)
    cfg = CONFIG["export"]
    create = X.create_table_sql(cfg, a.catalog, a.gold_schema, a.export_schema, a.s3_url, salt)
    export = X.export_sql(cfg, a.catalog, a.gold_schema, a.export_schema, salt)
    spark.sql(create)
    spark.sql(export)
    n = spark.sql(f"SELECT count(*) FROM {X.ident(a.catalog)}.{X.ident(a.export_schema)}.{X.ident(X.TARGET_TABLE)}").first()[0]
    print(f"Export finished: {n} rows in the exported table.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
