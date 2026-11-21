import copy
import json

import pytest
from databricks.bundles.jobs import Job

from bundle_gen import CatalogError, build_domain_jobs, load_catalog
from bundle_gen.hybrid import entry_tables
from tests.helpers import CONFIG, edges, read_yaml

TABLES = ["bookings", "properties", "users"]


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONFIG / "nodes.yml")


@pytest.fixture(scope="module")
def cfg():
    return read_yaml(CONFIG / "domains.yml")


@pytest.fixture(scope="module")
def jobs(catalog, cfg):
    return build_domain_jobs(catalog, cfg, TABLES)


def test_one_parent_job_per_domain(jobs):
    assert set(jobs) == {"hyb_ingestion", "hyb_property_quality", "hyb_user_behaviour", "hyb_executive_kpis"}


def test_intra_domain_edges_are_depends_on(jobs):
    assert edges(jobs["hyb_property_quality"]["tasks"]) == {
        ("silver_average_rating", "gold_property_type_ratings")
    }
    assert edges(jobs["hyb_user_behaviour"]["tasks"]) == {
        ("silver_user_categories", "gold_user_category_summary")
    }
    # the cross-domain node has NO depends_on: its upstreams live in other jobs
    assert edges(jobs["hyb_executive_kpis"]["tasks"]) == set()


def test_cross_domain_edges_are_table_update_triggers(jobs):
    def watched(key):
        t = jobs[key]["trigger"]["table_update"]
        return t["condition"], t["table_names"]

    assert watched("hyb_property_quality") == (
        "ALL_UPDATED",
        ["${var.catalog}.${var.bronze_schema}.properties", "${var.catalog}.${var.bronze_schema}.reviews"],
    )
    assert watched("hyb_executive_kpis") == (
        "ALL_UPDATED",
        ["${var.catalog}.${var.silver_schema}.average_rating", "${var.catalog}.${var.silver_schema}.user_categories"],
    )
    assert "trigger" not in jobs["hyb_ingestion"]  # root: on the clock
    assert "schedule" in jobs["hyb_ingestion"]
    for k in ("hyb_property_quality", "hyb_user_behaviour", "hyb_executive_kpis"):
        assert "schedule" not in jobs[k]
        assert jobs[k]["trigger"]["pause_status"] == "${var.hybrid_pause_status}"


def test_ingestion_is_a_for_each_over_the_table_list(jobs):
    (task,) = jobs["hyb_ingestion"]["tasks"]
    fe = task["for_each_task"]
    assert json.loads(fe["inputs"]) == TABLES
    assert fe["concurrency"] == 4
    assert fe["task"]["spark_python_task"]["parameters"][-2:] == ["--tables", "{{input}}"]
    assert jobs["hyb_ingestion"]["environments"][0]["environment_key"] == fe["task"]["environment_key"]


@pytest.mark.parametrize("key", ["hyb_ingestion", "hyb_property_quality", "hyb_user_behaviour", "hyb_executive_kpis"])
def test_jobs_are_valid_per_databricks_bundles_schema(jobs, key):
    assert Job.from_dict(jobs[key]).as_dict()["tasks"]


def test_entry_tables_come_from_entry_node_reads_only(catalog):
    # silver_user_categories is the domain's entry node (upstream is bronze, another domain);
    # gold_user_category_summary reads silver but its upstream is in-domain, so it adds nothing.
    assert entry_tables(catalog, "user_behaviour") == [
        "${var.catalog}.${var.bronze_schema}.users",
        "${var.catalog}.${var.bronze_schema}.page_views",
        "${var.catalog}.${var.bronze_schema}.bookings",
    ]


def broken(cfg, fn):
    data = copy.deepcopy(cfg)
    fn(data)
    return data


def test_config_errors(catalog, cfg):
    with pytest.raises(CatalogError, match="needs a 'schedule'"):
        build_domain_jobs(catalog, broken(cfg, lambda d: d["domains"][0].pop("schedule")), TABLES)
    with pytest.raises(CatalogError, match="needs an 'entry' trigger"):
        build_domain_jobs(catalog, broken(cfg, lambda d: d["domains"][1].pop("entry")), TABLES)
    with pytest.raises(CatalogError, match="not both"):
        build_domain_jobs(
            catalog,
            broken(cfg, lambda d: d["domains"][1].update(schedule={"cron": "0 0 6 * * ?"})),
            TABLES,
        )
    with pytest.raises(CatalogError, match="without config"):
        build_domain_jobs(catalog, broken(cfg, lambda d: d["domains"].pop()), TABLES)
    with pytest.raises(CatalogError, match="only 'table_update'"):
        build_domain_jobs(
            catalog, broken(cfg, lambda d: d["domains"][1]["entry"].update(via="file_arrival")), TABLES
        )
