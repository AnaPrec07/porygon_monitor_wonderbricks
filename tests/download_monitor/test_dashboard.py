import json
import re

import pytest

from dashboards import build_dashboard as B
from dm_common import gold_logic as G, suspicious_logic as S
from dm_common.config_data import CONFIG
from tests.download_monitor.test_gold_core import RANGES_ROWS, make_world
from tests.helpers import ROOT, refs_in, yaml_resources

FILE = ROOT / "src" / "download_monitor" / "dashboards" / "download_monitor.lvdash.json"


def test_json_is_up_to_date_and_parses():
    assert FILE.read_text(encoding="utf-8") == B.render(), "run build_dashboard.py"
    d = json.loads(FILE.read_text(encoding="utf-8"))
    assert [p["displayName"] for p in d["pages"]] == [
        "Overview", "Triage", "Sensitive data", "Large downloads", "IP addresses", "User drill-down", "Data quality"]


def test_widget_names_unique_and_datasets_exist():
    d = json.loads(FILE.read_text(encoding="utf-8"))
    names, datasets = [], {x["name"] for x in d["datasets"]}
    for page in d["pages"]:
        for item in page["layout"]:
            w = item["widget"]
            names.append((page["name"], w["name"]))
            assert {q["query"]["datasetName"] for q in w["queries"]} <= datasets
            pos = item["position"]
            assert pos["x"] + pos["width"] <= 6 and pos["width"] > 0 and pos["height"] > 0
    assert len(names) == len(set(names))


@pytest.fixture(scope="module")
def views(spark):
    a, history, cl, tl, cols, tbls = make_world(spark)
    ranges = G.ref_ip_ranges(spark, RANGES_ROWS)
    fct = G.build_fct_data_downloads(a, history, cl, tl, cols, tbls, ranges, CONFIG)
    flagged = S.with_flags(fct, CONFIG["thresholds"])
    fct.createOrReplaceTempView("fct_data_downloads")
    S.suspicious_downloads(flagged).createOrReplaceTempView("suspicious_downloads")
    S.daily_summary(flagged).createOrReplaceTempView("daily_download_summary")
    return spark


def test_every_dataset_query_runs_and_every_widget_field_exists(views):
    d = json.loads(FILE.read_text(encoding="utf-8"))
    columns = {}
    for ds in d["datasets"]:
        columns[ds["name"]] = set(views.sql("\n".join(ds["queryLines"])).columns)
    for page in d["pages"]:
        for item in page["layout"]:
            for q in item["widget"]["queries"]:
                cols = columns[q["query"]["datasetName"]]
                for f in q["query"]["fields"]:
                    assert B.referenced_columns(f["expression"]) <= cols, (page["name"], item["widget"]["name"], f)


def test_dashboard_reads_only_gold_views():
    allowed = {"fct_data_downloads", "suspicious_downloads", "daily_download_summary"}
    for _, q in B.DATASETS.values():
        assert set(re.findall(r"\bFROM\s+(\w+)", q)) and set(re.findall(r"\bFROM\s+(\w+)", q)) <= allowed


def test_resource():
    r = yaml_resources("dashboards")["download_monitor"]
    assert (ROOT / "resources" / "download_monitor" / r["file_path"]).resolve() == FILE
    assert refs_in(r) == {"var.dm_warehouse_id", "var.dm_catalog", "resources.schemas.dm_gold.name"}
