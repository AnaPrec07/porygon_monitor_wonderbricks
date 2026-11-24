# PR 6: Alerts

**Branch:** `dm/06-alerts` · **Type:** alerts · **Depends on:** PR 4

## Why

Deliverable 4: alerts fed from the suspicious materialized views.

## Scope

- `resources/download_monitor/alerts.yml`: SQL alerts v2 with inline `query_text`:
  `dm_pii_download`, `dm_untrusted_ip_download`, `dm_large_download`, `dm_high_risk_download` (`risk_score >= 80`),
  `dm_pipeline_stale` (no new bronze audit data for 6 hours).
- All: `empty_result_state: OK`, subscriptions from `${var.dm_alert_emails}` (or the destination chosen in PR 0),
  hourly schedule a few minutes after the job's usual finish, 2-hour lookback window, 1-hour retrigger interval,
  custom subject and body with the result table and alert URL.
- Confirm the minimum CLI version for alerts v2 in the bundle and in `.github/workflows/deploy.yml` (the andyweaves
  repo needs 0.288.0+ with the direct engine); pin it if needed.
- Tests: every alert query parses and references existing gold columns; every `${var.*}` resolves.

## Acceptance criteria (in `dev`)

- A test download from an IP outside the list fires `dm_untrusted_ip_download` within about two hours, and the next
  quiet hour fires nothing.
- A stopped pipeline fires `dm_pipeline_stale`.
- A zero-row result shows status OK, not an error.

## Out of scope

The runbook text (PR 8), other destinations unless decided in PR 0.
