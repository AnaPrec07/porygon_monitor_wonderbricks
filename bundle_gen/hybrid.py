"""Pattern 6: hybrid control plane.

One generated parent job per domain (`hyb_<domain>`):

* edges inside a domain  -> `depends_on` between tasks of that job,
* repeated work (bronze) -> a For each task over the table list,
* edges between domains  -> a table-update trigger on the downstream job, with the
  watched tables derived from the `reads` of the domain's entry nodes.

Pure functions returning plain dicts, like `flat.py`.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from .catalog import Catalog, CatalogError, Node, table_ref
from .flat import MAX_TASKS_PER_JOB

PAUSE_REF = "${var.hybrid_pause_status}"

# Relative to resources/__init__.py, where the resources are registered
# (the CLI resolves relative paths against the registering file).
INGEST_SCRIPT = "../src/bronze/load_wanderbricks.py"
DEFAULT_INGEST_CONCURRENCY = 4
_ENTRY_KEYS = {
    "via", "condition", "wait_after_last_change_seconds", "min_time_between_triggers_seconds",
}
_DOMAIN_KEYS = {"name", "schedule", "entry", "ingest", "max_concurrent_runs"}


def entry_tables(catalog: Catalog, domain: str) -> list[str]:
    """Tables to watch to start `domain`: reads of nodes with an upstream in another domain."""
    refs: list[str] = []
    for node in catalog.domain_nodes(domain):
        if catalog.external_dependencies(node):
            if not node.reads:
                raise CatalogError(
                    f"node '{node.id}' has a cross-domain dependency but declares no "
                    "'reads', so there is nothing to trigger on"
                )
            refs.extend(r for r in node.reads if r not in refs)
    return [table_ref(r) for r in refs]


def upstream_domains(catalog: Catalog, domain: str) -> list[str]:
    return list(dict.fromkeys(
        catalog.nodes[d].domain
        for n in catalog.domain_nodes(domain)
        for d in catalog.external_dependencies(n)
    ))


def _check_domain_graph(catalog: Catalog) -> None:
    """The domain-level graph must be acyclic even if the node graph is."""
    remaining = catalog.domains()
    done: list[str] = []
    while remaining:
        ready = [d for d in remaining if all(u in done for u in upstream_domains(catalog, d))]
        if not ready:
            raise CatalogError(f"domains depend on each other in a cycle: {', '.join(remaining)}")
        done.append(ready[0])
        remaining.remove(ready[0])


def ingest_task(node: Node, tables: list[str], concurrency: int, depends_on: list[str]) -> dict[str, Any]:
    """For each over the table list, reusing the existing bronze loader unchanged."""
    task: dict[str, Any] = {
        "task_key": node.id,
        "for_each_task": {
            "inputs": json.dumps(tables),
            "concurrency": concurrency,
            "task": {
                "task_key": f"{node.id}_table",
                "environment_key": "default",
                "spark_python_task": {
                    "python_file": INGEST_SCRIPT,
                    "parameters": [
                        "--source-catalog", "${var.source_catalog}",
                        "--source-schema", "${var.source_schema}",
                        "--target-catalog", "${var.catalog}",
                        "--target-schema", "${var.bronze_schema}",
                        "--tables", "{{input}}",
                    ],
                },
            },
        },
    }
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    return task


def pipeline_task(node: Node, depends_on: list[str], retries: Mapping[str, Any]) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": node.id,
        "pipeline_task": {"pipeline_id": f"${{resources.pipelines.{node.pipeline}.id}}"},
        **retries,
    }
    if depends_on:
        task["depends_on"] = [{"task_key": d} for d in depends_on]
    return task


def build_domain_jobs(
    catalog: Catalog, config: Mapping[str, Any], tables: list[str]
) -> dict[str, dict[str, Any]]:
    defaults = config.get("defaults") or {}
    configured = {d.get("name"): d for d in config.get("domains") or []}
    present = set(catalog.domains())
    if missing := sorted(present - set(configured)):
        raise CatalogError(f"domains in nodes.yml without config in domains.yml: {missing}")
    if extra := sorted(set(configured) - present):
        raise CatalogError(f"domains.yml configures unknown domains: {extra}")
    _check_domain_graph(catalog)

    retries = {"max_retries": 1, "min_retry_interval_millis": 60000}
    jobs: dict[str, dict[str, Any]] = {}

    for domain in catalog.domains():
        cfg = configured[domain]
        where = f"domain '{domain}'"
        if unknown := set(cfg) - _DOMAIN_KEYS:
            raise CatalogError(f"{where}: unknown keys {sorted(unknown)}")

        is_root = not upstream_domains(catalog, domain)
        if is_root and not cfg.get("schedule"):
            raise CatalogError(f"{where}: has no upstream domain, so it needs a 'schedule'")
        if is_root and cfg.get("entry"):
            raise CatalogError(f"{where}: has no upstream domain, so 'entry' would never fire")
        if not is_root and not cfg.get("entry"):
            raise CatalogError(
                f"{where}: depends on {upstream_domains(catalog, domain)} so it needs an 'entry' trigger"
            )
        if not is_root and cfg.get("schedule"):
            raise CatalogError(f"{where}: use either 'schedule' or 'entry', not both")

        nodes = catalog.domain_nodes(domain)
        order = catalog.topological_order(only=[n.id for n in nodes])
        if len(order) > MAX_TASKS_PER_JOB:
            raise CatalogError(f"{where}: {len(order)} tasks exceeds the {MAX_TASKS_PER_JOB}-task limit")
        inside = set(order)

        tasks = []
        has_ingest = False
        for node_id in order:
            node = catalog.nodes[node_id]
            deps = [d for d in node.depends_on if d in inside]  # cross-domain edges become the trigger
            if node.kind == "ingest":
                has_ingest = True
                conc = (cfg.get("ingest") or {}).get("concurrency", DEFAULT_INGEST_CONCURRENCY)
                tasks.append(ingest_task(node, tables, conc, deps))
            else:
                tasks.append(pipeline_task(node, deps, retries))

        key = f"hyb_{domain}"
        job: dict[str, Any] = {
            "name": key,
            "description": f"Hybrid control plane: parent job for the '{domain}' domain.",
            "tags": {"layer": "orchestration", "pattern": "hybrid", "domain": domain},
            "max_concurrent_runs": cfg.get("max_concurrent_runs", defaults.get("max_concurrent_runs", 1)),
            "queue": {"enabled": True},
            "performance_target": "STANDARD",
            "tasks": tasks,
        }
        if has_ingest:
            job["environments"] = [{"environment_key": "default", "spec": {"environment_version": "3"}}]

        if is_root:
            sched = cfg["schedule"]
            job["schedule"] = {
                "quartz_cron_expression": sched["cron"],
                "timezone_id": sched.get("timezone", "UTC"),
                "pause_status": PAUSE_REF,
            }
        else:
            entry = cfg["entry"]
            if unknown := set(entry) - _ENTRY_KEYS:
                raise CatalogError(f"{where}.entry: unknown keys {sorted(unknown)}")
            if entry.get("via", "table_update") != "table_update":
                raise CatalogError(f"{where}.entry.via: only 'table_update' is generated")
            update: dict[str, Any] = {
                "table_names": entry_tables(catalog, domain),
                "condition": entry.get("condition", "ALL_UPDATED"),
            }
            for k in ("wait_after_last_change_seconds", "min_time_between_triggers_seconds"):
                if k in entry:
                    update[k] = entry[k]
            job["trigger"] = {"pause_status": PAUSE_REF, "table_update": update}

        jobs[key] = job
    return jobs
