# S3 export

The `export` task of the `download_monitor` job writes `dm_gold.suspicious_downloads` to an **external Delta table
with UniForm (Iceberg)** on an S3 prefix, so another department can read it as Delta or Iceberg from Athena, Glue,
Snowflake, Trino or Spark without a Databricks account. It succeeds without doing anything while
`dm_export_s3_url` is empty.

## One-time setup (admin, outside the bundle)

```sql
-- storage credential (IAM role) created in Catalog Explorer or with the CLI, then:
CREATE EXTERNAL LOCATION IF NOT EXISTS dm_export_location
  URL 's3://<bucket>/security/download_monitor'
  WITH (STORAGE CREDENTIAL <credential>);

GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES ON EXTERNAL LOCATION dm_export_location TO `<service principal>`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.dm_export TO `<service principal>`;
```

```bash
# salt used to hash emails and IPs (never exported, never in git)
databricks secrets create-scope download_monitor
databricks secrets put-secret download_monitor export_salt
```

Then deploy with `--var dm_export_s3_url=s3://<bucket>/security/download_monitor`. The export task fails fast if a
column marked `hash` has no salt.

## What is exported

Only the columns in `config/download_monitor/export_columns.yml` (after editing it, run
`uv run python src/download_monitor/dm_common/build_config.py`). `user_email` and `source_ip` are salted SHA-256
hashes; `statement_text` is never exported. The receiving team and the privacy owner approve this list in PR review.

Each run rewrites the last `export_window_days` (3) days with `INSERT ... REPLACE WHERE`, so late system-table data
is picked up and re-running never duplicates rows. The table is partitioned by `event_date`.

## For the consuming team

- Path: the S3 prefix above. Format: Delta, also readable as Iceberg through UniForm metadata.
- Freshness: hourly, plus up to a few hours of system-table delay.
- They need read access to the prefix (IAM, on their side). Columns are listed in `export_columns.yml`.
- To join the same user across rows use the hashed value; it is stable as long as the salt is.

## Not verified from this repository

UniForm table properties on an external table, `secret()` inside `sha2`, and `REPLACE WHERE` on the partition
column need a first run in `dev`. If Delta Sharing is acceptable to the consumer it is the simpler alternative
(no copy, no S3 permissions): share `dm_gold.suspicious_downloads` with the same column list through a view.
