# Download monitor: pull request sequence

The [plan](../PLAN.md) split into small PRs, to be merged **one after another** in this order. Each PR is
deployable on its own, leaves `main` green (`uv run pytest`, `databricks bundle validate`), and ships nothing that
the next PR has to undo. Every job and schedule is `PAUSED` by default, so merging never changes runtime behaviour
until you arm it.

| # | PR | Needs a workspace? | Depends on |
|---|---|---|---|
| 0 | [Discovery and decisions](pr-00-discovery.md) | yes (admin, SQL only) | none |
| 1 | [Foundation: variables, schemas, config, classifier](pr-01-foundation.md) | no | 0 |
| 2 | [Bronze streaming tables](pr-02-bronze.md) | yes (deploy to dev) | 1 |
| 3 | [Gold core: reference tables and `fct_data_downloads`](pr-03-gold-core.md) | yes | 2 |
| 4 | [Suspicious-activity views and the orchestration job](pr-04-suspicious-and-job.md) | yes | 3 |
| 5 | [Dashboard](pr-05-dashboard.md) | yes | 4 |
| 6 | [Alerts](pr-06-alerts.md) | yes | 4 (5 is optional) |
| 7 | [S3 export](pr-07-s3-export.md) | yes (admin for the external location) | 4 |
| 8 | [Docs, runbook and hardening](pr-08-docs-runbook.md) | no | 5, 6, 7 |

PRs 5, 6 and 7 only depend on PR 4, so they can be reviewed in parallel, but merge them in the order above.

## Rules for every PR

- Branch name `dm/<number>-<slug>`, one PR per file in this folder, based on the latest `main`.
- Keep Databricks facts in `config/download_monitor/`, not in code. Anything marked **⚠ verify** in the plan is
  settled by PR 0 and written down there.
- Offline tests for everything that can be tested offline; extend `tests/test_bundle_wiring.py`-style reference
  checks for each new resource file.
- Update the README table and `docs/download_monitor/` where behaviour changes.
- Run `databricks bundle validate -t dev` and deploy to `dev` before asking for review (CI cannot reach the
  workspace). Paste the result in the PR description.
- Do not enable schedules in the PR. Arm them after merge, once, in PR 4 / PR 6 / PR 7's "after merge" step.
- Prod deploys happen from `main` via `.github/workflows/deploy.yml`, so anything merged is deployed to prod:
  keep resources paused and idempotent.

## Definition of done for the whole programme

The three test downloads from PR 0 (Excel, dashboard, SQL editor CSV) appear in `fct_data_downloads` with the right
channel, IP, rows and PII evidence, trigger the matching alert, show in the dashboard, and arrive in the S3 export.
