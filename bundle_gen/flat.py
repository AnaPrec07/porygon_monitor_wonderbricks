"""Pattern 5: generate flat-DAG orchestration jobs from config.

Each workflow in `config/orchestration/workflows.yml` selects a subset of the
graph in nodes.yml and becomes one job `gen_<name>`. Returns plain dicts shaped
like the Jobs API / bundle YAML so they can be unit-tested and passed to
`databricks.bundles.jobs.Job.from_dict`.
"""

from __future__ import annotations

from typing import Any, Mapping

from .catalog import Catalog, CatalogError, Node

PAUSE_REF = "${var.config_generated_pause_status}"
MAX_TASKS_PER_JOB = 1000  # Databricks hard limit on tasks in one job
_SELECT_KEYS = {"all", "layers", "nodes", "downstream_of", "include_roots"}
_WORKFLOW_KEYS = {
    "name", "description", "select", "schedule", "retries",
    "notify_on_failure", "max_concurrent_runs",
}


def select_nodes(catalog: Catalog, selector: Mapping[str, Any], where: str = "select") -> list[str]:
    """Resolve a selector to node ids, in topological order."""
    if unknown := set(selector) - _SELECT_KEYS:
        raise CatalogError(f"{where}: unknown selector keys {sorted(unknown)}")

    def known(ids: list[str], key: str) -> list[str]:
        if missing := [i for i in ids if i not in catalog.nodes]:
            raise CatalogError(f"{where}.{key}: unknown nodes {missing}")
        return ids

    selected: set[str] = set()
    if selector.get("all"):
        selected |= set(catalog.nodes)
    for layer in selector.get("layers") or []:
        matches = {n.id for n in catalog.nodes.values() if n.layer == layer}
        if not matches:
            raise CatalogError(f"{where}.layers: no nodes in layer '{layer}'")
        selected |= matches
    selected |= set(known(list(selector.get("nodes") or []), "nodes"))
    if roots := known(list(selector.get("downstream_of") or []), "downstream_of"):
        selected |= catalog.descendants(roots, selector.get("include_roots", True))

    if not selected:
        raise CatalogError(f"{where}: selects no nodes")
    return catalog.topological_order(only=selected)


def pipeline_or_job_task(node: Node, depends_on: list[str], retries: Mapping[str, Any]) -> dict[str, Any]:
    """One task for a node in a flat DAG (ingest -> Run Job, pipeline -> pipeline task)."""
    task: dict[str, Any] = {"task_key": node.id}
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    if node.kind == "ingest":
        task["run_job_task"] = {"job_id": f"${{resources.jobs.{node.job}.id}}"}
    else:
        task["pipeline_task"] = {"pipeline_id": f"${{resources.pipelines.{node.pipeline}.id}}"}
        task.update(retries)  # retries only on pipeline tasks
    return task


def build_workflow_job(
    catalog: Catalog, workflow: Mapping[str, Any], defaults: Mapping[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Return (resource key, job dict) for one workflow."""
    name = workflow.get("name")
    if not name:
        raise CatalogError("workflow without a 'name'")
    where = f"workflow '{name}'"
    if unknown := set(workflow) - _WORKFLOW_KEYS:
        raise CatalogError(f"{where}: unknown keys {sorted(unknown)}")

    order = select_nodes(catalog, workflow.get("select") or {}, f"{where}.select")
    if len(order) > MAX_TASKS_PER_JOB:
        raise CatalogError(f"{where}: {len(order)} tasks exceeds the {MAX_TASKS_PER_JOB}-task job limit")

    retries = {**(defaults.get("retries") or {}), **(workflow.get("retries") or {})}
    chosen = set(order)
    tasks = [
        pipeline_or_job_task(
            catalog.nodes[i],
            [d for d in catalog.nodes[i].depends_on if d in chosen],
            retries,
        )
        for i in order
    ]

    key = f"gen_{name}"
    job: dict[str, Any] = {
        "name": key,
        "description": workflow.get("description", f"Generated from config: {name}"),
        "tags": {"layer": "orchestration", "pattern": "config_generated"},
        "max_concurrent_runs": workflow.get("max_concurrent_runs", defaults.get("max_concurrent_runs", 1)),
        "queue": {"enabled": True},
        "performance_target": "STANDARD",
        "tasks": tasks,
    }
    if sched := workflow.get("schedule"):
        job["schedule"] = {
            "quartz_cron_expression": sched["cron"],
            "timezone_id": sched.get("timezone", "UTC"),
            "pause_status": PAUSE_REF,
        }
    if emails := workflow.get("notify_on_failure"):
        job["email_notifications"] = {"on_failure": list(emails)}
    return key, job


def build_workflow_jobs(catalog: Catalog, config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    defaults = config.get("defaults") or {}
    jobs: dict[str, dict[str, Any]] = {}
    for workflow in config.get("workflows") or []:
        key, job = build_workflow_job(catalog, workflow, defaults)
        if key in jobs:
            raise CatalogError(f"duplicate workflow name '{workflow.get('name')}'")
        jobs[key] = job
    return jobs
