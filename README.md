# porygon_monitor_wonderbricks

Databricks Asset Bundle that copies the Databricks **Wanderbricks** sample data
(`samples.wanderbricks`) into your own Unity Catalog schema.

## Layout

- `databricks.yml` – bundle config, variables, `dev` target
- `resources/wanderbricks_etl.yml` – target schema + serverless job
- `src/bronze/load_wanderbricks.py` – ETL: reads each source table, writes a Delta copy, checks row counts

## Deploy and run

```bash
databricks auth login --host https://dbc-7350f2a1-4fb7.cloud.databricks.com
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run wanderbricks_etl -t dev
```

## Configuration

Override at deploy time, e.g. `databricks bundle deploy -t dev --var catalog=my_catalog`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `catalog` | `main` | Target catalog (must exist and you need `CREATE SCHEMA`) |
| `schema` | `wanderbricks` | Target schema (prefixed with your username in `dev` mode) |
| `source_catalog` / `source_schema` | `samples` / `wanderbricks` | Source data |
| `tables` | 12 core tables | Comma-separated tables to load; a missing table is reported and fails the run |
