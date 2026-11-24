# PR 0: Discovery and decisions

**Branch:** `dm/00-discovery` · **Type:** docs only · **Depends on:** none

## Why

The plan has several **⚠ verify** items (audit action names, how a download links to a statement, client
application values, lineage coverage, PII tag visibility, verbose audit). Building on a wrong guess would mean
reworking every later PR. This PR records the real answers and the business decisions first.

## Scope

- `docs/download_monitor/discovery.md` with the result (and the date and workspace) of each query below.
- `docs/download_monitor/decisions.md` answering the open questions of PLAN.md section 12.
- Update `PLAN.md` where discovery disproves an assumption.

## Tasks

1. Run the Phase 0 SQL from PLAN.md section 11 as an admin and paste results (counts, not user data).
2. Run query 5 (PII tags) as the future pipeline service principal as well, and compare counts with the admin's.
3. Check the system schemas `access` and `query` are enabled; record the retention actually observed.
4. Check if verbose audit logs are on; if not, ask for them to be enabled before PR 3.
5. Perform three test downloads and note the audit and query-history rows they produce:
   Excel (or any ODBC tool) on a PII-tagged table, a dashboard data download, a SQL editor CSV download.
6. Record decisions: approved IP source and format, thresholds, tag name and values, alert destination, export
   consumer and allowed columns, in-scope workspaces and region, bronze retention.

## Acceptance criteria

- Every **⚠ verify** in PLAN.md is resolved or turned into a named risk with an owner.
- `decisions.md` has an answer (or an owner and a date) for each of the 8 open questions.
- The three test downloads are documented with their `event_time`, `statement_id` and user, so they can be reused
  as acceptance tests in PRs 3 to 7.

## Out of scope

Any code or bundle change.
