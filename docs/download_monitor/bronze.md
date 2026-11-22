# Bronze layer

Pipeline `dm_bronze` copies four system tables into streaming tables in `${dm_bronze_schema}`.

| Table | Source |
|---|---|
| `bronze_audit` | `system.access.audit` |
| `bronze_query_history` | `system.query.history` |
| `bronze_column_lineage` | `system.access.column_lineage` |
| `bronze_table_lineage` | `system.access.table_lineage` |

## Grants for the run-as principal

```sql
GRANT USE CATALOG ON CATALOG system TO `<service principal>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.access TO `<service principal>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.query TO `<service principal>`;
```

The `access` and `query` system schemas must be enabled by a metastore admin.

## Operating notes

- **Never full-refresh a bronze table.** The tables set `pipelines.reset.allowed = false`, so the update is
  refused. System tables keep a limited window, so a full refresh would lose the older history.
- **First run:** set `dm_bronze_start_date` (for example `--var dm_bronze_start_date=2026-10-01`) to keep the first
  backfill short, then clear it. Rows older than the date are never copied afterwards.
- Expectations only warn. Check the pipeline event log for `has_event_id` / `has_statement_id` violations.
- Not verified from this repository: that `dp.expect`, `cluster_by` and `root_path` module imports behave as
  assumed on your workspace. Run `databricks bundle validate -t dev` and one update, then check that the second
  update appends only new rows.
