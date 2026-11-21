# Databricks building blocks used or relevant here

A generic reference, not a how-to. Each row says when a mechanism is the right choice and when it stops working.
Limits change, so treat any number as something to re-check in the Databricks docs.

## Used by the code in this repo

| Mechanism | Best use when | Breaks when |
|---|---|---|
| **Job with `depends_on` tasks** | steps share one run, one repair, one set of permissions | the graph grows past what one run page can show, or hits the per-job task limit |
| **Pipeline task** (Lakeflow Spark Declarative Pipelines) | the step is declarative table logic with expectations | you need arbitrary code, or several pipeline updates must interleave with other work in a single task |
| **Run Job task** | reusing an existing job unchanged as a step, or nesting a parent over leaf jobs | nesting gets deep, you need cycles, or retry behaviour inside the child matters for the parent |
| **For each task** | the same step over many inputs, in parallel, with per-iteration visibility and repair | you need nested loops, downstream tasks depending on a single nested task, or very large inline input lists |
| **Schedule (cron)** | work is due at a fixed time and data timing is predictable | upstream data arrives late or irregularly: it runs on stale or partial data |
| **Table update trigger** | a job should run when specific tables change; fan-in with `ALL_UPDATED` | the dependency is on a business date, the table is a view or non-Unity-Catalog table, the run-as principal lacks read access (it fails quietly), or you watch more tables than the limit |
| **Trigger debounce** (`wait_after_last_change_seconds`, `min_time_between_triggers_seconds`) | a burst of table writes should produce one run | latency matters more than run count |
| **Retries** (`max_retries`) | transient failures: cluster start, network, throttling | the failure is deterministic or the task is not idempotent: you repeat the damage |
| **Queueing and `max_concurrent_runs`** | a trigger can fire while a run is active and the run should not be dropped | you need strict real-time response, or a backlog could build indefinitely |
| **Bundle variables** | one definition across targets and across generated jobs | values must be known at runtime rather than deploy time |
| **Bundle `include` globs** | splitting resources into files per layer or domain | two files define the same resource key |
| **Python for bundles** (`load_resources`) | many similar jobs, shared policy, jobs must stay inside bundle state | the team cannot own a Python build step, or the CLI is below the required version |
| **`Job.from_dict` schema** | validating generated job dicts offline | it checks shape, not whether the workspace accepts the settings |
| **Pause status on schedules and triggers** | deploying orchestration without activating it | someone unpauses two alternatives writing the same tables |

## Worth knowing, not used here

| Mechanism | Best use when | Breaks when |
|---|---|---|
| **File arrival trigger** | a job should start when files land in a volume or external location | the path needs wildcards, or unrelated file updates can time out the check |
| **Auto Loader** (with file events) | incremental, exactly-once ingestion of files from cloud storage | the stream is idle for long enough to invalidate its position, or the external location is edited |
| **Job completion trigger** (Beta) | start a job after other jobs finish, without editing them | completions must match a business date or batch (it does not), or the preview is not enabled in every involved workspace |
| **SQL condition on a trigger** (Beta) | run only if a query says the data is ready | the check cannot see which event fired it, or the preview is off |
| **Condition (if/else) task and `run_if`** | branching or skipping based on upstream results or values | the logic is complex enough to belong inside the task code |
| **Repair run** | re-run only failed tasks and their dependents after a failure | tasks are not idempotent: repair re-runs from the start of each failed task |
| **Continuous jobs** | always-on streaming-style processing | you want cost control or batch semantics |
| **Task values and job parameters** | passing small values between tasks and runs (for example a run date) | payloads approach the documented size limits |
| **`bundle generate job`** | adopting an existing UI-built job into a bundle | the job relies on settings the bundle schema does not model |
| **Databricks SDK or Terraform generation** | job definitions are produced at runtime or by another system | you need drift detection inside the bundle: you own reconciliation |
| **System tables** (job and task timelines, lineage) | cross-job observability, SLA checks, missing-run detection | you need real-time alerting rather than periodic queries |
| **Notifications and health rules** | alert on failure, duration or backlog | alert fatigue; nobody owns the channel |
| **Run-as and permissions** | separating who deploys from who runs, per-team access | a trigger's run-as principal lacks read access to what it watches |
| **Pipeline expectations** (`expect_or_drop`, `expect_or_fail`) | row-level quality rules on declarative tables | the rule needs cross-table or cross-run logic |
| **dbt, SQL and notebook tasks** | the step is already in that form | you are mixing many types in one chain and need uniform parameters |
