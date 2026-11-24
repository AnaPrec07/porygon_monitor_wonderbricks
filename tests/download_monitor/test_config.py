import subprocess
import sys

from dm_common import build_config
from dm_common.classify import cidr_bounds
from dm_common.config_data import CONFIG
from tests.helpers import ROOT, bundle_variables, read_yaml, yaml_resources


def test_config_data_is_up_to_date():
    assert build_config.OUT.read_text(encoding="utf-8") == build_config.render(build_config.load_config()), (
        "config_data.py is stale: run `uv run python src/download_monitor/dm_common/build_config.py`"
    )


def test_ip_ranges_parse_and_are_well_formed():
    assert CONFIG["ip_ranges"], "need at least one approved range"
    for r in CONFIG["ip_ranges"]:
        lo, hi = cidr_bounds(r["cidr"])
        assert lo <= hi and (r["start_int"], r["end_int"]) == (lo, hi)
        assert r["owner"] and r["description"]


def test_thresholds_are_sane():
    t = CONFIG["thresholds"]
    for key in ("large_download_rows", "large_download_bytes", "daily_rows_per_user", "burst_downloads_per_hour"):
        assert isinstance(t[key], int) and t[key] > 0
    assert t["daily_rows_per_user"] >= t["large_download_rows"]
    assert set(t["pii_evidence_levels"]) <= {"column", "table", "text"}
    assert 0 < t["high_risk_score"] <= 100
    assert set(t["risk_weights"]) == {"pii", "untrusted_ip", "large"}


def test_actions_have_channels_and_unique_names():
    names = [a["action"] for a in CONFIG["actions"]["actions"]]
    assert len(names) == len(set(names))
    assert all(a["channel"] for a in CONFIG["actions"]["actions"])
    assert CONFIG["actions"]["statement_id_param_keys"]


def test_channel_rules():
    ch = CONFIG["channels"]
    channels = {r["channel"] for r in ch["rules"]}
    assert set(ch["external_client_channels"]) <= channels
    for rule in ch["rules"]:
        for c in rule["any"]:
            assert c.get("not_null") or c.get("contains"), c


def test_export_columns_are_unique_and_exclude_statement_text():
    cols = [c["name"] for c in CONFIG["export"]["columns"]]
    assert len(cols) == len(set(cols))
    assert "statement_text" not in cols and "statement_text_preview" not in cols
    assert CONFIG["export"]["export_window_days"] >= 1


def test_download_monitor_variables_have_defaults_and_resolve():
    variables = bundle_variables()
    dm = {k: v for k, v in variables.items() if k.startswith("dm_")}
    assert {"dm_catalog", "dm_bronze_schema", "dm_gold_schema", "dm_export_schema", "dm_warehouse_id",
            "dm_alert_emails", "dm_export_s3_url", "dm_pause_status", "dm_bronze_start_date"} <= set(dm)
    assert all("default" in v for v in dm.values())
    assert dm["dm_pause_status"]["default"] == "PAUSED"


def test_schemas_declared():
    schemas = yaml_resources("schemas")
    assert {"dm_bronze", "dm_gold", "dm_export"} <= set(schemas)
