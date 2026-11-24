# Decisions (PR 0)

Defaults used by the code are in the second column. Replace them with the real answer and open a PR if it changes
a config file.

| # | Question | Default in the code | Owner / answer |
|---|---|---|---|
| 1 | Source and format of the approved IP list | `config/download_monitor/supported_ip_ranges.csv`, IPv4 CIDR only | |
| 2 | Thresholds for "large" | 100,000 rows or 1 GiB per download; 1,000,000 rows per user per day; more than 20 downloads per user per hour | |
| 3 | Export consumer, format and allowed columns | Delta/Iceberg on S3, columns listed in `config/download_monitor/export_columns.yml`, emails and IPs hashed | |
| 4 | PII tag name and values | tag name `PII` (case-insensitive), any value | |
| 5 | Alert destination | email to `dm_alert_emails` | |
| 6 | Workspaces and region in scope | everything the metastore sees | |
| 7 | Verbose audit logs | assumed off | |
| 8 | Bronze retention | keep forever | |
| 9 | Are service principals monitored? | yes, flagged `is_service_principal`; only `System-User` is excluded | |
