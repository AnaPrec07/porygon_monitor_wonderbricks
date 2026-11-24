# PR 4: Suspicious-activity views and the orchestration job

**Branch:** `dm/04-suspicious-and-job` · **Type:** pipeline + job · **Depends on:** PR 3

## Why

Delivers the three detections you asked for (sensitive data, large amounts, untrusted IP) as gold materialized
views, and wires bronze, then gold into one job.

## Scope

- `src/download_monitor/gold/suspicious.py`:
  `suspicious_pii_downloads`, `suspicious_large_downloads` (single download, burst per user per hour, daily volume),
  `suspicious_untrusted_ip_downloads`, `suspicious_downloads` (union, `reasons[]`, `risk_score`),
  `daily_download_summary`.
- Rules and thresholds read from `ref_thresholds` and `ref_supported_ip_ranges`, never hard-coded.
- `resources/download_monitor/job.yml`: job `download_monitor`, hourly, `max_concurrent_runs: 1`,
  `pause_status: ${var.dm_pause_status}`, tasks `bronze` then `gold`, `on_failure` email, serverless.
  (The `export` task is added in PR 7.)
- Tests: fixture-based rule tests (a PII download, a 100k-row download, a download from an IP outside the list, a
  clean download that must not appear, a service principal that is excluded); risk score arithmetic; job graph test
  in the style of `tests/test_bundle_wiring.py`.

## Acceptance criteria (in `dev`)

- Each of the three test downloads appears in exactly the suspicious views it should, with the right `reasons`.
- A clean query does not appear in any suspicious view.
- One manual job run goes bronze, then gold, green; the job stays `PAUSED` on deploy.

## After merge

Arm `dm_pause_status = UNPAUSED` in `dev` only, and watch two or three runs before enabling it in `prod`.

## Out of scope

Dashboard, alerts, export.
