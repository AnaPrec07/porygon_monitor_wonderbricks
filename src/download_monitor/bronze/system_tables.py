"""Bronze streaming tables for the download monitor (Lakeflow Spark Declarative Pipeline).

One append-only streaming table per system table, following the system tables guidance:
read as a stream with `skipChangeCommits` (system tables get retention deletes and other
non-append commits that would otherwise fail the stream) and run triggered, not continuous.
Expectations only warn: bronze must never drop audit records.

Pipeline configuration keys:
  bronze_start_date  optional YYYY-MM-DD lower bound for the first backfill (default: copy all)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from dm_common.bronze_spec import BRONZE_TABLES, TABLE_PROPERTIES, parse_start_date


def _register(spec):
    expect_name, expect_sql = spec["expect"]

    @dp.table(
        name=spec["name"],
        comment=spec["comment"],
        cluster_by=spec["cluster_by"],
        table_properties=TABLE_PROPERTIES,
    )
    @dp.expect(expect_name, expect_sql)
    def _bronze():
        df = spark.readStream.option("skipChangeCommits", "true").table(spec["source"])
        start = parse_start_date(spark.conf.get("bronze_start_date", ""))
        if start:
            df = df.where(F.col(spec["time_column"]) >= F.lit(start))
        return df

    _bronze.__name__ = spec["name"]
    return _bronze


for _spec in BRONZE_TABLES:
    _register(_spec)
