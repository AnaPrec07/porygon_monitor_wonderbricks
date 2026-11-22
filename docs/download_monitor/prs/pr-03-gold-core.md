# PR 3: Gold core: reference tables and `fct_data_downloads`

**Branch:** `dm/03-gold-core` · **Type:** pipeline · **Depends on:** PR 2

## Why

This is where all the joins happen, once. Everything after this PR is a simple filter over one fact table.

## Scope

- Pipeline `dm_gold` (`resources/download_monitor/gold_pipeline.yml`), serverless, triggered, schema `dm_gold`.
- `src/download_monitor/gold/reference.py` (materialized views):
  `ref_supported_ip_ranges` (from the CSV, with integer range bounds), `ref_thresholds`, `dim_pii_columns`
  (`information_schema.column_tags` plus table, schema and catalog tag inheritance), `dim_pii_tables`.
- `src/download_monitor/gold/downloads.py`:
  - `fct_query_results`: latest row per `statement_id` from `bronze_query_history`, with `channel` and IP
    attribution (`ip_attribution`: direct, verbose_audit, session, inferred, none).
  - `fct_data_downloads`: union of explicit downloads (audit) and external-client results (query history),
    with rows, bytes, `ip_trusted`, `pii_evidence` (column, table, text, none), `pii_columns`, `pii_tag_values`.
- No `current_date()` or `current_timestamp()` in any view (keeps refreshes incremental).
- Service principals and `System-User` flagged, not dropped, using `thresholds.yml`.
- Tests: Python-side logic reuses `classify.py`; SQL is checked with a small local Spark session on fixture rows
  (the repo already depends on `pyspark`) covering each `pii_evidence` level, trusted/untrusted IP, each channel.

## Acceptance criteria (in `dev`)

- The three test downloads from PR 0 appear with the expected channel, IP, rows and PII evidence.
- `dim_pii_columns` count matches what an admin sees (BROWSE granted to the service principal where needed).
- The pipeline event log shows incremental refresh for `fct_data_downloads` on the second update, or the reason it
  cannot be, written into the PR.
- Duplicate `statement_id`s in bronze produce one row in the fact table.

## Out of scope

Rules and risk scores (PR 4), dashboard, alerts, export.
