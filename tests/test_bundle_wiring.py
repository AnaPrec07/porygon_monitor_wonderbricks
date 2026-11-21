"""Offline stand-in for `databricks bundle validate`, plus the CLI's own build entrypoint.

The Databricks CLI cannot be installed in every environment, so these checks cover
what can be proven without a workspace: every ${var.*} and ${resources.*} reference
used by generated or hand-written orchestration resolves, and `resources:load_resources`
runs through the same `databricks.bundles.build` entrypoint the CLI calls.
"""

import json
import subprocess
import sys

import pytest

from tests.helpers import ROOT, bundle_variables, read_yaml, refs_in, yaml_resources

DEFAULT_TABLES = bundle_variables()["tables"]["default"]


def run_build(tmp_path, tables=DEFAULT_TABLES):
    variables = {name: {"value": spec.get("default")} for name, spec in bundle_variables().items()}
    variables["tables"] = {"value": tables}
    inp = tmp_path / "in.json"
    inp.write_text(json.dumps({
        "bundle": {"name": "porygon_monitor_wonderbricks", "target": "prod"},
        "variables": variables,
        "python": read_yaml(ROOT / "databricks.yml")["python"],
    }))
    out, diag = tmp_path / "out.json", tmp_path / "diag.json"
    proc = subprocess.run(
        [sys.executable, "-m", "databricks.bundles.build", "--phase", "load_resources",
         "--input", str(inp), "--output", str(out), "--diagnostics", str(diag)],
        cwd=ROOT, capture_output=True, text=True,
    )
    return proc, out, diag


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    proc, out, diag = run_build(tmp_path_factory.mktemp("build"))
    assert proc.returncode == 0, proc.stderr + (diag.read_text() if diag.exists() else "")
    return json.loads(out.read_text())["resources"]["jobs"]


def test_databricks_yml_declares_python_resources():
    py = read_yaml(ROOT / "databricks.yml")["python"]
    assert py["resources"] == ["resources:load_resources"]
    assert py["venv_path"] == ".venv"


def test_cli_entrypoint_generates_expected_jobs(built):
    assert set(built) == {
        "gen_medallion_full", "gen_silver_and_gold", "gen_ratings_chain", "gen_gold_only",
        "hyb_ingestion", "hyb_property_quality", "hyb_user_behaviour", "hyb_executive_kpis",
    }
    tables = json.loads(built["hyb_ingestion"]["tasks"][0]["for_each_task"]["inputs"])
    assert tables == DEFAULT_TABLES.split(",")


def test_tables_variable_must_be_resolved(tmp_path):
    proc, _, diag = run_build(tmp_path, tables="${var.something_else}")
    assert proc.returncode != 0 or "Failed to load resources" in diag.read_text()


def test_all_references_resolve(built):
    """Every ${var.x} / ${resources.type.key.attr} in orchestration resources points at something real."""
    variables = set(bundle_variables())
    jobs_declared = set(yaml_resources("jobs")) | set(built)
    pipelines_declared = set(yaml_resources("pipelines"))
    schemas_declared = set(yaml_resources("schemas"))

    yaml_orch = read_yaml(ROOT / "resources/orchestration/event_driven.yml")["resources"]["jobs"]
    refs = refs_in(built) | refs_in(yaml_orch)
    assert refs, "expected references to check"

    for ref in refs:
        if ref.startswith("var."):
            assert ref[4:] in variables, f"undefined variable in {ref}"
        elif ref.startswith("resources."):
            _, kind, key, attr = ref.split(".", 3)
            declared = {"jobs": jobs_declared, "pipelines": pipelines_declared, "schemas": schemas_declared}[kind]
            assert key in declared, f"{ref}: no such {kind[:-1]} '{key}'"
        else:
            pytest.fail(f"unexpected reference type: {ref}")


def test_generated_keys_do_not_collide_with_yaml_jobs(built):
    assert not set(built) & set(yaml_resources("jobs"))


def test_python_generated_paths_exist():
    from bundle_gen.hybrid import INGEST_SCRIPT

    assert (ROOT / INGEST_SCRIPT).resolve().is_file()
