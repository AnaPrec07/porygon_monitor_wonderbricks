"""Load the Databricks Wanderbricks sample tables into your own Unity Catalog schema.

Each source table is read from <source_catalog>.<source_schema> and written as a
Delta table to <target_catalog>.<target_schema>, replacing any previous copy so
the job is idempotent. Row counts are compared after each load; the job fails if
any table does not match or can't be loaded.
"""

import argparse
import sys

from pyspark.sql import SparkSession


# todo: just a comment
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--source-catalog", required=True)
    p.add_argument("--source-schema", required=True)
    p.add_argument("--target-catalog", required=True)
    p.add_argument("--target-schema", required=True)
    p.add_argument("--tables", required=True, help="Comma-separated table names")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    failures = []
    for table in tables:
        src = f"`{args.source_catalog}`.`{args.source_schema}`.`{table}`"
        dst = f"`{args.target_catalog}`.`{args.target_schema}`.`{table}`"
        try:
            df = spark.table(src)
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(dst)
            src_n, dst_n = df.count(), spark.table(dst).count()
            if src_n != dst_n:
                raise RuntimeError(f"row count mismatch: source={src_n} target={dst_n}")
            print(f"OK    {table}: {dst_n} rows -> {dst}")
        except Exception as exc:  # keep going so one bad table doesn't hide the rest
            print(f"FAIL  {table}: {exc}", file=sys.stderr)
            failures.append(table)

    if failures:
        print(
            f"{len(failures)} table(s) failed: {', '.join(failures)}", file=sys.stderr
        )
        return 1
    print(f"Loaded {len(tables)} tables.")
    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code != 0:
        sys.exit(exit_code)
