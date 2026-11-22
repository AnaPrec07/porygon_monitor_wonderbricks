# PR 5: Dashboard

**Branch:** `dm/05-dashboard` · **Type:** dashboard · **Depends on:** PR 4

## Why

Deliverable 3: an AI/BI dashboard built only from the gold materialized views.

## Scope

- `src/download_monitor/dashboards/download_monitor.lvdash.json` (the "dashboard.json"), generated from a dashboard
  built in the UI with `databricks bundle generate dashboard`, then edited in git.
- `resources/download_monitor/dashboard.yml`: `warehouse_id: ${var.dm_warehouse_id}`, permissions for the security
  team (`CAN_VIEW`), service principal as run-as or embedded credentials per the PR 0 decision.
- Pages: Overview, Triage, Sensitive data, Large downloads, IP addresses, User drill-down, Data quality
  (see PLAN.md section 7). Global filters: date, workspace, channel, user, `ip_trusted`, `pii_evidence`.
- Datasets read only from `dm_gold.*`; rolling time windows live in the dataset SQL.
- `tests/download_monitor/test_dashboard.py`: JSON parses, every dataset query references an existing gold table
  and only columns that exist in the PR 3/4 code, no hard-coded catalog names (parameterised).

## Acceptance criteria

- Deployed from git to `dev`; every page renders without errors on real data.
- Counters match a direct SQL count on `suspicious_downloads` for the same range.
- The Data quality page shows bronze freshness and the share of downloads without IP attribution or lineage.

## Out of scope

Alerts, export, geolocation (PR 8 optional).
