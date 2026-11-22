# Gold layer

Pipeline `dm_gold` builds materialized views in `${dm_gold_schema}` from the bronze tables and from
`system.information_schema` tag tables. The logic is in `src/download_monitor/dm_common/gold_logic.py`
and is tested on local Spark; the pipeline files only read tables.

| View | Grain | Notes |
|---|---|---|
| `ref_supported_ip_ranges` | CIDR | built from `config/download_monitor/supported_ip_ranges.csv` |
| `dim_pii_columns` | column | tag `PII` on a column |
| `dim_pii_tables` | table / schema / catalog | NULL schema or table means "everything below" (inheritance) |
| `fct_data_downloads` | download | explicit downloads (audit) + external-client results (query history) |

## How a row of `fct_data_downloads` is built

| Column | Rule |
|---|---|
| `download_kind` | `explicit` (audit event in `download_actions.yml`) or `external_client` (finished query with rows from a channel in `external_client_channels`) |
| `channel` | explicit: the action's channel, replaced by `dashboard` / `genie` when the joined statement came from one; external: classified from `client_application`, `client_driver`, `query_source` |
| `ip_attribution` | `direct` (audit event carries the IP), `verbose_audit` (a databrickssql event for the statement), `inferred` (the user's worst IP of that day, conservative), `none` |
| `ip_status` | `trusted`, `untrusted`, `internal` (0.0.0.0, loopback), `unparsed` (IPv6 or garbage), `unknown` (no IP) |
| `pii_evidence` | `column` (column lineage hits a PII column) > `table` (table lineage hits a PII table, inherited tags included) > `text` (PII table name appears in the SQL) > `none` |

## Differences from the plan

- `fct_query_results` is not a separate view; its logic is a step inside `fct_data_downloads`.
- Thresholds are read from `config_data.py` (compiled from `thresholds.yml`) instead of a `ref_thresholds` table.
- Config files are compiled into `dm_common/config_data.py` because pipelines cannot reliably read repository
  files. After editing anything in `config/download_monitor/` run
  `uv run python src/download_monitor/dm_common/build_config.py`; a test fails when it is stale.

## Assumptions to confirm on the first run (see discovery.md)

- Columns used: `executed_by`, `client_application`, `client_driver`, `produced_rows`, `read_bytes`,
  `query_source.*` in `system.query.history`; `statement_id`, `source_table_full_name`, `source_column_name` in the
  lineage tables. Missing optional columns become NULL instead of failing the update.
- `information_schema` only shows objects the run-as principal can see: grant `BROWSE` on each catalog with PII tags
  and compare the `dim_pii_columns` count with what an admin sees.
- `text` evidence cross-joins the downloads with the list of PII tables; if that gets slow, drop it from
  `pii_evidence_levels` and from the join.
