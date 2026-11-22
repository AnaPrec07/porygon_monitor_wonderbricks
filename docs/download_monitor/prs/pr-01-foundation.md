# PR 1: Foundation: variables, schemas, config and classifier

**Branch:** `dm/01-foundation` · **Type:** bundle config + pure Python · **Depends on:** PR 0

## Why

Everything later needs the same variables, schemas and tunable config. The classification logic (CIDR matching,
port stripping, channel mapping) is pure Python, so it can be fully tested offline before any pipeline exists.

## Scope

- `databricks.yml`: new variables `dm_catalog`, `dm_bronze_schema`, `dm_gold_schema`, `dm_export_schema`,
  `dm_warehouse_id`, `dm_alert_emails`, `dm_export_s3_url`, `dm_pause_status` (default `PAUSED`),
  `dm_bronze_start_date`. Required ones without defaults must not break `bundle validate`: give a documented
  placeholder or make them optional until the PR that uses them.
- `resources/download_monitor/schemas.yml`: schemas `dm_bronze`, `dm_gold`, `dm_export`.
- `config/download_monitor/`: `download_actions.yml`, `client_channels.yml`, `thresholds.yml`,
  `supported_ip_ranges.csv` (header plus a documented example row, replaced with the real list from PR 0).
- `src/download_monitor/common/classify.py`: config loader, `strip_port`, `ip_in_ranges` (IPv4 CIDR; IPv6 returns
  "unparsed"), `classify_channel`.
- `tests/download_monitor/test_classify.py`, `test_config.py`; extend the bundle wiring test to the new resources.

## Acceptance criteria

- `uv run pytest` passes, including cases for `1.2.3.4:5678`, empty, `0.0.0.0`, `127.0.0.1`, IPv6, overlapping
  CIDRs, and one case per `channel`.
- Every CIDR in the CSV parses; every threshold has a type and sane range; every action in
  `download_actions.yml` has a channel.
- `databricks bundle validate -t dev` passes; deploying creates three empty schemas and nothing else.

## Out of scope

Pipelines, jobs, dashboards, alerts.
