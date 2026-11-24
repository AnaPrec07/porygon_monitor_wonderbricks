import re

import pytest

from dm_common.config_data import CONFIG
from tests.helpers import ROOT, read_yaml, refs_in

ALERTS = read_yaml(ROOT / "resources" / "download_monitor" / "alerts.yml")["resources"]["alerts"]
NOW = "TIMESTAMP '2026-11-01 11:00:00'"
EXPECTED_TOTAL = {  # on the fixture world (tests/download_monitor/test_gold_core.py)
    "dm_pii_download": 1, "dm_untrusted_ip_download": 2, "dm_large_download": 0, "dm_high_risk_download": 1,  # e_dl: table-level PII (50) + untrusted IP (30)
}


def runnable(sql):
    sql = re.sub(r"\$\{var\.dm_(catalog|gold_schema|bronze_schema)\}\.", "", sql)
    return sql.replace("current_timestamp()", NOW)


def test_alert_set_and_common_settings():
    assert set(ALERTS) == {"dm_pii_download", "dm_untrusted_ip_download", "dm_large_download",
                           "dm_high_risk_download", "dm_pipeline_stale"}
    for name, a in ALERTS.items():
        ev = a["evaluation"]
        assert ev["empty_result_state"] == "OK" and ev["notification"]["retrigger_seconds"] == 3600
        assert ev["notification"]["subscriptions"] == [{"user_email": "${var.dm_alert_emails}"}]
        assert ev["comparison_operator"] == "GREATER_THAN"
        assert a["schedule"]["pause_status"] == "${var.dm_pause_status}"
        assert a["warehouse_id"] == "${var.dm_warehouse_id}"
        assert a["display_name"].startswith("[download monitor]")
        assert "{{QUERY_RESULT_TABLE}}" in a["custom_description"]


def test_threshold_and_window():
    for name, a in ALERTS.items():
        want = 6 if name == "dm_pipeline_stale" else 0
        assert a["evaluation"]["threshold"]["value"]["double_value"] == want
        if name != "dm_pipeline_stale":
            assert "INTERVAL 2 HOURS" in a["query_text"]


def test_high_risk_threshold_matches_config():
    q = ALERTS["dm_high_risk_download"]["query_text"]
    assert f"risk_score >= {CONFIG['thresholds']['high_risk_score']}" in q


def test_variables_resolve():
    declared = set(read_yaml(ROOT / "databricks.yml")["variables"])
    assert {r[4:] for r in refs_in(ALERTS) if r.startswith("var.")} <= declared


@pytest.mark.parametrize("name", sorted(ALERTS))
def test_query_runs_and_returns_the_source_column(gold_views, name):
    a = ALERTS[name]
    rows = gold_views.sql(runnable(a["query_text"])).collect()
    assert len(rows) == 1, "an alert query must return exactly one summary row"
    row = rows[0].asDict()
    assert a["evaluation"]["source"]["name"] in row
    if name in EXPECTED_TOTAL:
        assert row["total"] == EXPECTED_TOTAL[name]
    else:
        assert row["hours_since_last_audit"] is not None


def test_every_alert_has_a_runbook_section():
    runbook = (ROOT / "docs" / "download_monitor" / "runbook.md").read_text(encoding="utf-8")
    for name, a in ALERTS.items():
        assert f"## {name}\n" in runbook, name
        assert f"runbook.md#{name}" in a["custom_description"]
