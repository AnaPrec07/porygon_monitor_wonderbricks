import ast

import pytest

from dm_common import export_sql as X
from dm_common.config_data import CONFIG
from tests.helpers import ROOT, read_yaml, refs_in, yaml_resources

CFG = CONFIG["export"]
SALT = X.salt_expr("download_monitor", "export_salt")


def test_select_list_hashes_marked_columns_only():
    sel = X.select_list(CFG, SALT)
    assert "sha2(concat(secret('download_monitor', 'export_salt'), cast(`user_email` AS STRING)), 256) AS `user_email`" in sel
    assert "sha2(concat(secret('download_monitor', 'export_salt'), cast(`source_ip` AS STRING)), 256) AS `source_ip`" in sel
    assert sel.count("sha2(") == 2
    assert "statement_text" not in sel


def test_hash_without_salt_is_refused():
    with pytest.raises(ValueError):
        X.select_list(CFG, None)


def test_export_is_idempotent_replace_where():
    sql = X.export_sql(CFG, "main", "dm_gold", "dm_export", SALT)
    assert sql.startswith("INSERT INTO `main`.`dm_export`.`suspicious_downloads` REPLACE WHERE event_date >= date_sub(current_date(), 3)")
    assert sql.endswith("WHERE event_date >= date_sub(current_date(), 3)")
    assert "`main`.`dm_gold`.`suspicious_downloads`" in sql


def test_create_table_is_external_uniform_and_partitioned():
    sql = X.create_table_sql(CFG, "main", "dm_gold", "dm_export", "s3://bucket/sec/dm/", SALT)
    assert "LOCATION 's3://bucket/sec/dm'" in sql and "PARTITIONED BY (event_date)" in sql
    assert "'delta.universalFormat.enabledFormats' = 'iceberg'" in sql and sql.endswith("WHERE 1 = 0")


@pytest.mark.parametrize("bad", ["", "http://x", "s3://b/p'; DROP", "s3:/b", "b/p"])
def test_bad_locations_rejected(bad):
    with pytest.raises(ValueError):
        X.s3_url(bad)


@pytest.mark.parametrize("bad", ["a b", "a;b", "a`b", ""])
def test_unsafe_identifiers_rejected(bad):
    with pytest.raises(ValueError):
        X.ident(bad)


def test_exported_columns_exist_in_the_suspicious_view(gold_views):
    available = set(gold_views.table("suspicious_downloads").columns)
    assert [c["name"] for c in CFG["columns"] if c["name"] not in available] == []
    # and the select list is valid SQL on the same data (salt replaced by a literal)
    rows = gold_views.sql(f"SELECT {X.select_list(CFG, repr('pepper'))} FROM suspicious_downloads").collect()
    assert rows and all(len(r["user_email"]) == 64 for r in rows if r["user_email"])
    assert not any("@" in (r["user_email"] or "") for r in rows)


def test_script_skips_when_unconfigured_and_is_wired_into_the_job():
    src = (ROOT / "src" / "download_monitor" / "export" / "export_to_s3.py").read_text()
    ast.parse(src)
    assert "return 0" in src and "export not configured" in src
    job = yaml_resources("jobs")["download_monitor"]
    task = next(t for t in job["tasks"] if t["task_key"] == "export")
    assert task["depends_on"] == [{"task_key": "gold"}]
    assert (ROOT / "resources" / "download_monitor" / task["spark_python_task"]["python_file"]).resolve().exists()
    params = task["spark_python_task"]["parameters"]
    assert params[params.index("--s3-url") + 1] == "${var.dm_export_s3_url}"
    declared = set(read_yaml(ROOT / "databricks.yml")["variables"])
    assert {r[4:] for r in refs_in(job) if r.startswith("var.")} <= declared
