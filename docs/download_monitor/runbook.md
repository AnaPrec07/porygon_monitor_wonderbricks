# Runbook: what to do when a download monitor alert fires

First look at the alert's result table, then open the dashboard (**Triage** page, sorted by risk score) and filter
by the user and date in the alert. Everything below uses `dm_gold.fct_data_downloads` unless noted.

```sql
-- the downloads behind an alert (adjust the window and the user)
SELECT * FROM <catalog>.dm_gold.suspicious_downloads
WHERE event_time >= current_timestamp() - INTERVAL 6 HOURS AND user_email = '<user>'
ORDER BY risk_score DESC, event_time DESC;
```

## dm_pii_download

**Meaning:** a download whose source columns are tagged `PII` (column-level lineage evidence).
**Check:** `pii_columns` and `pii_tag_values` show what was taken; `channel` shows how (Excel, dashboard, notebook,
SQL editor). Is the user allowed to see those columns? Is the volume (`rows_returned`) normal for them?
**Usually fine:** analysts whose role requires the data and who download from a trusted IP; scheduled exports.
**Escalate when:** the user has no business reason, the IP is `untrusted`, or it repeats daily.
**Close:** note the decision in the ticket; if the access is legitimate but noisy, discuss an allowlist rather than
raising the threshold.

## dm_untrusted_ip_download

**Meaning:** a download from an IP outside `config/download_monitor/supported_ip_ranges.csv`.
**Check:** `ip_attribution`. `direct` and `verbose_audit` are reliable; **`inferred` takes the user's worst IP of the
day**, so it can blame a download on a login from another place. Look up the IP owner (VPN, home, cloud).
**Usually fine:** a missing range in the list (add it through a PR with owner and description), travel, a new VPN.
**Escalate when:** the IP is unknown and the download is PII or large. Consider revoking tokens and sessions.
**False positives:** IPv6 shows as `unparsed` and is counted as untrusted until IPv6 ranges are supported.

## dm_large_download

**Meaning:** one download of at least `large_download_rows` rows or `large_download_bytes` bytes, more than
`burst_downloads_per_hour` downloads by one user in an hour, or `daily_rows_per_user` rows in a day
(`reasons`: `large_download`, `download_burst`, `daily_volume`).
**Check:** the `statement_text_preview` and `channel`; compare with the user's previous days on the dashboard.
**Usually fine:** reporting jobs run as a person, data migrations. Service principals are flagged
`is_service_principal`; only `System-User` is excluded.
**Tuning:** change `config/download_monitor/thresholds.yml`, rebuild `config_data.py`, open a PR.

## dm_high_risk_download

**Meaning:** risk score of 80 or more, i.e. PII plus an untrusted IP, or all three signals. The most urgent alert.
**Check:** treat as a possible exfiltration until shown otherwise: contact the user, check their other audit events
(`system.access.audit` for the same `user_email` around `event_time`), and the destination of the file if known.

## dm_pipeline_stale

**Meaning:** no new audit events landed in `bronze_audit` for 6 hours. **The other alerts are blind while this fires.**
**Check:** the `download_monitor` job run and the `dm_bronze` pipeline event log; permission changes on
`system.access`; a paused schedule. System tables can also be delayed on the platform side; check the Databricks
status page.
**Never** run a full refresh of a bronze table to fix this (it is blocked on purpose; history would be lost).
