from datetime import datetime

import pytest

from dm_common import suspicious_logic as S
from dm_common.config_data import CONFIG
from tests.helpers import ROOT, edges, refs_in, yaml_resources

T = CONFIG["thresholds"]
H = datetime(2026, 11, 1, 10, 5)

SCHEMA = ("download_id string, user_email string, event_time timestamp, event_date date, workspace_id string,"
          " channel string, download_kind string, rows_returned bigint, read_bytes bigint, ip_status string,"
          " pii_evidence string")


def fct(spark, rows):
    out = [(i, u, t, t.date(), "w1", ch, "explicit", r, b, ip, pii) for (i, u, t, ch, r, b, ip, pii) in rows]
    return spark.createDataFrame(out, SCHEMA)


def flagged(spark, rows):
    return {r["download_id"]: r for r in S.with_flags(fct(spark, rows), T).collect()}


def test_each_rule_and_clean_row(spark):
    got = flagged(spark, [
        ("clean", "a@x.com", H, "excel", 10, 100, "trusted", "none"),
        ("pii_col", "a@x.com", H, "excel", 10, 100, "trusted", "column"),
        ("pii_tbl", "a@x.com", H, "excel", 10, 100, "trusted", "table"),
        ("pii_txt", "a@x.com", H, "excel", 10, 100, "trusted", "text"),      # text is review-only
        ("ip", "a@x.com", H, "excel", 10, 100, "untrusted", "none"),
        ("ipv6", "a@x.com", H, "excel", 10, 100, "unparsed", "none"),
        ("internal", "a@x.com", H, "excel", 10, 100, "internal", "none"),
        ("unknown", "a@x.com", H, "excel", 10, 100, "unknown", "none"),
        ("big", "b@x.com", H, "excel", T["large_download_rows"], 100, "trusted", "none"),
        ("bytes", "c@x.com", H, "excel", 10, T["large_download_bytes"], "trusted", "none"),
        ("norows", "d@x.com", H, "notebook", None, None, "trusted", "none"),
        ("sysuser", "System-User", H, "excel", 10**9, 100, "untrusted", "column"),   # excluded
    ])
    assert "sysuser" not in got
    assert not got["clean"]["flag_any"] and got["clean"]["reasons"] == [] and got["clean"]["risk_score"] == 0
    assert got["pii_col"]["flag_pii"] and got["pii_tbl"]["flag_pii"] and not got["pii_txt"]["flag_pii"]
    assert got["ip"]["flag_untrusted_ip"] and got["ipv6"]["flag_untrusted_ip"]
    assert not got["internal"]["flag_any"] and not got["unknown"]["flag_any"]
    assert got["big"]["flag_large_single"] and got["bytes"]["flag_large_single"]
    assert not got["norows"]["flag_any"]


def test_risk_score_is_additive_and_capped(spark):
    got = flagged(spark, [
        ("all3", "a@x.com", H, "excel", T["large_download_rows"], 1, "untrusted", "column"),
        ("pii_ip", "b@x.com", H, "excel", 1, 1, "untrusted", "column"),
        ("pii", "c@x.com", H, "excel", 1, 1, "trusted", "column"),
    ])
    w = T["risk_weights"]
    assert got["all3"]["risk_score"] == min(100, w["pii"] + w["untrusted_ip"] + w["large"])
    assert got["all3"]["reasons"] == ["pii_data", "untrusted_ip", "large_download"]
    assert got["pii_ip"]["risk_score"] == w["pii"] + w["untrusted_ip"]
    assert got["pii"]["risk_score"] == w["pii"] < T["high_risk_score"]


def test_burst_per_user_hour(spark):
    n = T["burst_downloads_per_hour"]
    rows = [(f"u{i}", "burst@x.com", H, "notebook", 1, 1, "trusted", "none") for i in range(n + 1)]
    rows += [(f"v{i}", "ok@x.com", H, "notebook", 1, 1, "trusted", "none") for i in range(n)]       # exactly at limit
    rows += [("w0", "burst@x.com", datetime(2026, 11, 1, 12, 0), "notebook", 1, 1, "trusted", "none")]  # other hour
    got = flagged(spark, rows)
    assert all(got[f"u{i}"]["flag_burst"] for i in range(n + 1))
    assert not any(got[f"v{i}"]["flag_burst"] for i in range(n))
    assert not got["w0"]["flag_burst"]


def test_daily_volume_per_user(spark):
    half = T["daily_rows_per_user"] // 2
    got = flagged(spark, [
        ("d1", "vol@x.com", datetime(2026, 11, 1, 9), "excel", half - 1, 1, "trusted", "none"),
        ("d2", "vol@x.com", datetime(2026, 11, 1, 15), "excel", half + 1, 1, "trusted", "none"),
        ("d3", "vol@x.com", datetime(2026, 11, 2, 9), "excel", 5, 1, "trusted", "none"),   # next day
    ])
    assert got["d1"]["flag_daily_volume"] and got["d2"]["flag_daily_volume"] and not got["d3"]["flag_daily_volume"]


def test_views_and_summary(spark):
    f = S.with_flags(fct(spark, [
        ("a", "a@x.com", H, "excel", 1, 1, "trusted", "column"),
        ("b", "b@x.com", H, "excel", 1, 1, "trusted", "none"),
        ("c", "c@x.com", H, "dashboard", 1, 1, "untrusted", "none"),
    ]), T)
    assert {r["download_id"] for r in S.suspicious_downloads(f).collect()} == {"a", "c"}
    rows = {r["channel"]: r for r in S.daily_summary(f).collect()}
    assert rows["excel"]["downloads"] == 2 and rows["excel"]["suspicious_downloads"] == 1
    assert rows["dashboard"]["untrusted_ip_downloads"] == 1


def test_gold_pipeline_registers_suspicious_views():
    p = yaml_resources("pipelines")["dm_gold"]
    paths = [l["file"]["path"] for l in p["libraries"]]
    assert any(x.endswith("gold/suspicious.py") for x in paths)
    src = (ROOT / "src" / "download_monitor" / "gold" / "suspicious.py").read_text()
    for name in ("suspicious_pii_downloads", "suspicious_large_downloads", "suspicious_untrusted_ip_downloads",
                 "suspicious_downloads", "daily_download_summary"):
        assert f'name="{name}"' in src


def test_job_graph_and_defaults():
    job = yaml_resources("jobs")["download_monitor"]
    assert edges(job["tasks"]) == {("bronze", "gold"), ("gold", "export")}
    assert job["schedule"]["pause_status"] == "${var.dm_pause_status}"
    assert job["max_concurrent_runs"] == 1
    assert job["email_notifications"]["on_failure"]
    assert {"resources.pipelines.dm_bronze.id", "resources.pipelines.dm_gold.id"} <= refs_in(job)
    assert {"dm_bronze", "dm_gold"} <= set(yaml_resources("pipelines"))
