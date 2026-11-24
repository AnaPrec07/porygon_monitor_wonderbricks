# PR 8: Docs, runbook and hardening

**Branch:** `dm/08-docs-runbook` · **Type:** docs (+ optional small code) · **Depends on:** PRs 5, 6, 7

## Scope

- `docs/download_monitor/README.md`: architecture, deploy order, variables, how to arm schedules, how to tune IP
  list and thresholds, how to add a channel or download action.
- `docs/download_monitor/runbook.md`: for each alert, what it means, the query to run first, who to contact, how to
  close it, known false positives (service principals, scheduled dashboard refreshes).
- Update the top-level README layout section; mark PLAN.md as implemented and list deviations.
- Apply the `PII` governed tag to the monitor's own gold and export tables and restrict grants to the security team.
- Optional, each as its own small commit: tag history snapshots (SCD2), per-user 30-day baseline for the daily volume
  rule, IPv6 ranges, IP geolocation, untagged-PII candidates from data classification, table-update trigger instead
  of the hourly clock.

## Acceptance criteria

- A new engineer can deploy to `dev` and change the approved IP list using only the docs.
- Every alert links to its runbook section.
- The programme's definition of done (see the PR index) is demonstrated end to end and noted in the PR.
