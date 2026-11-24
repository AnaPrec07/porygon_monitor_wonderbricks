# PR 7: S3 export

**Branch:** `dm/07-s3-export` · **Type:** job task + admin setup · **Depends on:** PR 4

## Why

Deliverable 5: make the suspicious-downloads data available to another department from S3.

## Scope

- Admin prerequisite (documented, done outside the PR or via the bundle if supported): UC storage credential and
  external location on `${var.dm_export_s3_url}`; grants to the export service principal.
- External table `dm_export.suspicious_downloads` at that location, partitioned by `event_date`, Delta with
  UniForm Iceberg enabled.
- `src/download_monitor/export/export_to_s3.py` and an `export` task in `jobs.download_monitor` after `gold`:
  `INSERT ... REPLACE WHERE` on a trailing 3-day window (idempotent), an explicit approved column list from config,
  no `statement_text`, optional hashing of emails and IPs per the PR 0 decision.
- Tests: the column list contains no forbidden columns; the SQL builder is deterministic; job graph updated.
- Short consumer guide in `docs/download_monitor/export.md`: path, schema, refresh cadence, how to read as Delta or
  Iceberg, IAM role the other department needs.

## Acceptance criteria

- The other department (or a test role) reads the data from S3 with their tool.
- Running the export twice produces no duplicate rows; a late row for yesterday is picked up.
- A column outside the approved list cannot reach the export without a config change in git.

## Fallback

If the receiving team can use Delta Sharing, replace the S3 table with a Delta Share, same approved columns.
