# Discovery results (PR 0)

> **Status: not run.** These queries need a workspace and admin rights; the session that wrote this repo had neither.
> Run each query, paste the aggregated result under it (counts only, no user data) and tick the box. Until then,
> PRs 1 to 8 are built on the assumptions in the last column and are written so a different answer is a config change.

| # | Question | Assumed until verified | Done |
|---|---|---|---|
| 1 | Which audit actions are downloads, and which `request_params` keys do they carry? | the action list in `config/download_monitor/download_actions.yml` | [ ] |
| 2 | Does a download event carry an id joinable to `system.query.history.statement_id`? | one of `statementId`, `statement_id`, `queryId` in `request_params` (`statement_id_param_keys`) | [ ] |
| 3 | Which `client_application` / `client_driver` values exist (Excel, dashboards, UI)? | the patterns in `config/download_monitor/client_channels.yml` | [ ] |
| 4 | Share of SELECT statements that have column lineage | high enough that `pii_evidence = 'column'` is the main signal | [ ] |
| 5 | Are PII tags visible to the pipeline service principal? | yes, once it has `BROWSE` on each catalog | [ ] |
| 6 | Are verbose audit logs on (databrickssql events with an IP)? | no: IP for external clients is `inferred` | [ ] |
| 7 | Are `system.access` and `system.query` enabled, and what is the retention? | enabled, about 1 year | [ ] |

## Queries

```sql
-- 1. download/export actions and their request_params keys
SELECT service_name, action_name, count(*) n, collect_set(map_keys(request_params)) keys
FROM system.access.audit
WHERE event_date >= current_date() - 30 AND lower(action_name) RLIKE 'download|export|filesget'
GROUP BY ALL ORDER BY n DESC;

-- 2. do downloads carry a statement id?
SELECT action_name, request_params FROM system.access.audit
WHERE action_name = 'downloadQueryResult' AND event_date >= current_date() - 7 LIMIT 20;

-- 3. client applications and drivers
SELECT client_application, client_driver, count(*) n,
       count(query_source.dashboard_id) dashboards, count(query_source.genie_space_id) genie
FROM system.query.history WHERE start_time >= current_date() - 30
GROUP BY ALL ORDER BY n DESC;

-- 4. lineage coverage for reads
SELECT count(*) selects, count(cl.statement_id) with_column_lineage
FROM system.query.history q
LEFT JOIN (SELECT DISTINCT statement_id FROM system.access.column_lineage
           WHERE event_date >= current_date() - 7) cl USING (statement_id)
WHERE q.statement_type = 'SELECT' AND q.start_time >= current_date() - 7;

-- 5. PII tags (run as an admin, then as the pipeline service principal, and compare)
SELECT tag_value, count(*) FROM system.information_schema.column_tags
WHERE lower(tag_name) = 'pii' GROUP BY ALL;

-- 6. verbose audit on?
SELECT action_name, count(*) FROM system.access.audit
WHERE service_name = 'databrickssql' AND event_date >= current_date() - 1 GROUP BY ALL;
```

## Acceptance downloads (reused as end-to-end tests in PRs 3 to 7)

Perform each on a PII-tagged table, then record `event_time`, `statement_id` and user.

| Channel | event_time | statement_id | user |
|---|---|---|---|
| Excel / ODBC | | | |
| Dashboard data download | | | |
| SQL editor CSV download | | | |
