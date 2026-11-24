import ast

import pytest

from dm_common.bronze_spec import BRONZE_TABLES, TABLE_PROPERTIES, parse_start_date
from tests.helpers import ROOT, refs_in, yaml_resources

PIPELINE_FILE = ROOT / "src" / "download_monitor" / "bronze" / "system_tables.py"


def test_tables_are_unique_system_table_copies():
    names = [t["name"] for t in BRONZE_TABLES]
    assert len(names) == len(set(names)) == 4
    for t in BRONZE_TABLES:
        assert t["source"].startswith(("system.access.", "system.query."))
        assert t["time_column"] in t["cluster_by"] or t["time_column"] == "start_time"
        assert t["name"].startswith("bronze_")


def test_full_refresh_is_blocked():
    assert TABLE_PROPERTIES["pipelines.reset.allowed"] == "false"


def test_stream_options_in_pipeline_code():
    src = PIPELINE_FILE.read_text(encoding="utf-8")
    tree = ast.parse(src)
    assert "skipChangeCommits" in src and "readStream" in src
    assert "expect_or_drop" not in src, "bronze must not drop records"
    assert "dp.expect(" in src and "dp.table(" in src
    assert any(isinstance(n, ast.For) for n in tree.body), "tables are registered from BRONZE_TABLES"


@pytest.mark.parametrize("raw,expected", [("", None), (None, None), ("2026-10-01", "2026-10-01")])
def test_start_date_ok(raw, expected):
    assert parse_start_date(raw) == expected


@pytest.mark.parametrize("raw", ["2026-1-1", "now()", "2026-10-01' OR 1=1"])
def test_start_date_rejects_garbage(raw):
    with pytest.raises(ValueError):
        parse_start_date(raw)


def test_pipeline_resource():
    p = yaml_resources("pipelines")["dm_bronze"]
    assert p["serverless"] is True and p["continuous"] is False
    assert p["schema"] == "${resources.schemas.dm_bronze.name}"
    assert (ROOT / "resources" / "download_monitor" / p["libraries"][0]["file"]["path"]).resolve().exists()
    assert (ROOT / "resources" / "download_monitor" / p["root_path"]).resolve().is_dir()
    assert refs_in(p) == {"var.dm_catalog", "resources.schemas.dm_bronze.name", "var.dm_bronze_start_date"}
