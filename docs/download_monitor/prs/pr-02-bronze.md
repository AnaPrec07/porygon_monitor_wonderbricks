# PR 2: Bronze streaming tables

**Branch:** `dm/02-bronze` · **Type:** pipeline · **Depends on:** PR 1

## Why

Deliverable 1: a streaming-table pipeline over the system tables, following the system tables guidance, that keeps
history beyond system-table retention.

## Scope

- `src/download_monitor/bronze/system_tables.py` with four streaming tables:
  `bronze_audit`, `bronze_query_history`, `bronze_column_lineage`, `bronze_table_lineage`.
- `resources/download_monitor/bronze_pipeline.yml`: pipeline `dm_bronze`, serverless, triggered (not continuous),
  catalog `${var.dm_catalog}`, schema `${var.dm_bronze_schema}`, configuration `bronze_start_date`.
- Reads use `readStream` with `skipChangeCommits`; liquid clustering on the join keys; table property
  `pipelines.reset.allowed = false` so a full refresh cannot delete history.
- Warn-only expectations (`@dp.expect`), never `expect_or_drop`, in bronze.
- Grants documented for the run-as service principal (`USE CATALOG`/`USE SCHEMA`/`SELECT` on `system.access` and
  `system.query`).
- Tests: the pipeline file imports its table names and keys from one place that a test checks against
  `docs/download_monitor/PLAN.md` section 4 and the bundle wiring.

## Acceptance criteria (in `dev`)

- First update fills all four tables (use `dm_bronze_start_date` to keep it short); a second update appends only
  new rows (compare row counts and the pipeline event log).
- A full refresh of a bronze table is refused.
- Row counts per day match the source tables for a sample day.
- Findings from PR 0 (column names, lineage keys) are reflected; no assumed column is missing.

## Out of scope

Dedup, joins, PII, any gold logic; the job and schedule (PR 4).
