"""Entry point for Python-defined bundle resources (see `python:` in databricks.yml).

The Databricks CLI imports `resources:load_resources` at deploy time and merges
the returned jobs with those declared in YAML. Generated jobs:

  gen_<workflow>   pattern 5, one per workflow in config/orchestration/workflows.yml
  hyb_<domain>     pattern 6, one per domain in config/orchestration/domains.yml

Resources are registered here (not inside bundle_gen) on purpose: the CLI
resolves relative paths such as the bronze script against the file that
registers the resource.
"""

from pathlib import Path

from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job

from bundle_gen import build_domain_jobs, build_workflow_jobs, load_catalog, load_yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "orchestration"


def bronze_tables(bundle: Bundle) -> list[str]:
    raw = bundle.variables.get("tables")
    if not isinstance(raw, str) or "${" in raw:
        raise ValueError(
            "bundle variable 'tables' must resolve to a comma-separated list of "
            f"table names to generate the For each ingestion task, got {raw!r}"
        )
    return [t.strip() for t in raw.split(",") if t.strip()]


def load_resources(bundle: Bundle) -> Resources:
    resources = Resources()
    catalog = load_catalog(CONFIG_DIR / "nodes.yml")

    workflows = build_workflow_jobs(catalog, load_yaml(CONFIG_DIR / "workflows.yml"))
    domains = build_domain_jobs(catalog, load_yaml(CONFIG_DIR / "domains.yml"), bronze_tables(bundle))

    for key, job in {**workflows, **domains}.items():
        resources.add_resource(key, Job.from_dict(job))
    return resources
