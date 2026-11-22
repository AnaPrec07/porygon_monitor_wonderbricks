import ast

from tests.helpers import ROOT, refs_in, yaml_resources

CORE = ROOT / "src" / "download_monitor" / "gold" / "core.py"
NON_DETERMINISTIC = ("current_date", "current_timestamp", "now()", "rand(")


def decorated_views(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = {}
    for fn in [n for n in tree.body if isinstance(n, ast.FunctionDef)]:
        for d in fn.decorator_list:
            if isinstance(d, ast.Call) and getattr(d.func, "attr", "") == "materialized_view":
                out[next(k.value.value for k in d.keywords if k.arg == "name")] = fn.name
    return out


def test_core_views_registered():
    views = decorated_views(CORE)
    assert set(views) == {"ref_supported_ip_ranges", "dim_pii_columns", "dim_pii_tables", "fct_data_downloads"}
    assert all(name == fn for name, fn in views.items())


def test_no_non_deterministic_functions_in_gold_code():
    for path in (ROOT / "src" / "download_monitor").glob("**/*.py"):
        if "gold" in path.parts or path.name == "gold_logic.py":
            src = path.read_text(encoding="utf-8")
            assert not [f for f in NON_DETERMINISTIC if f in src], (path, "breaks incremental refresh")


def test_pipeline_resource():
    p = yaml_resources("pipelines")["dm_gold"]
    assert p["serverless"] is True and p["continuous"] is False
    assert (ROOT / "resources" / "download_monitor" / p["libraries"][0]["file"]["path"]).resolve().exists()
    assert p["configuration"] == {"bronze_catalog": "${var.dm_catalog}", "bronze_schema": "${var.dm_bronze_schema}"}
    assert {"var.dm_catalog", "var.dm_bronze_schema", "resources.schemas.dm_gold.name"} == refs_in(p)
