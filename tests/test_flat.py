import pytest
from databricks.bundles.jobs import Job

from bundle_gen import CatalogError, build_workflow_jobs, load_catalog
from bundle_gen.flat import select_nodes
from tests.helpers import CONFIG, edges, read_yaml, ROOT


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONFIG / "nodes.yml")


@pytest.fixture(scope="module")
def jobs(catalog):
    return build_workflow_jobs(catalog, read_yaml(CONFIG / "workflows.yml"))


def test_medallion_full_matches_hand_written_baseline(jobs):
    """The generated full workflow is the same graph as medallion_orchestrator."""
    baseline = read_yaml(ROOT / "resources/orchestration/medallion_job.yml")
    base_tasks = baseline["resources"]["jobs"]["medallion_orchestrator"]["tasks"]
    gen_tasks = jobs["gen_medallion_full"]["tasks"]
    assert {t["task_key"] for t in gen_tasks} == {t["task_key"] for t in base_tasks}
    assert edges(gen_tasks) == edges(base_tasks)
    # same underlying resources, too
    def targets(tasks):
        return {
            t["task_key"]: next(iter(t.get(k) for k in ("run_job_task", "pipeline_task") if t.get(k)))
            for t in tasks
        }
    assert targets(gen_tasks) == targets(base_tasks)


def test_subset_workflows_keep_only_internal_edges(jobs):
    gold = jobs["gen_gold_only"]["tasks"]
    assert {t["task_key"] for t in gold} == {
        "gold_property_type_ratings", "gold_user_category_summary", "gold_engagement_kpis",
    }
    assert edges(gold) == set()  # their upstreams are silver, outside the selection

    chain = jobs["gen_ratings_chain"]["tasks"]
    assert edges(chain) == {
        ("silver_average_rating", "gold_property_type_ratings"),
        ("silver_average_rating", "gold_engagement_kpis"),
    }

    silver_gold = jobs["gen_silver_and_gold"]["tasks"]
    assert "bronze_ingestion" not in {t["task_key"] for t in silver_gold}


def test_only_the_full_workflow_has_a_schedule_and_it_is_pausable(jobs):
    assert jobs["gen_medallion_full"]["schedule"]["pause_status"] == "${var.config_generated_pause_status}"
    assert all("schedule" not in j for k, j in jobs.items() if k != "gen_medallion_full")


def test_retries_only_on_pipeline_tasks(jobs):
    for task in jobs["gen_medallion_full"]["tasks"]:
        assert ("max_retries" in task) == ("pipeline_task" in task)


@pytest.mark.parametrize("key", ["gen_medallion_full", "gen_silver_and_gold", "gen_ratings_chain", "gen_gold_only"])
def test_jobs_are_valid_per_databricks_bundles_schema(jobs, key):
    job = Job.from_dict(jobs[key])  # raises on unknown / badly typed fields
    assert job.as_dict()["tasks"]


def test_selector_errors(catalog):
    with pytest.raises(CatalogError, match="unknown nodes"):
        select_nodes(catalog, {"nodes": ["nope"]})
    with pytest.raises(CatalogError, match="selects no nodes"):
        select_nodes(catalog, {})
    with pytest.raises(CatalogError, match="unknown selector keys"):
        select_nodes(catalog, {"layer": ["gold"]})  # typo for 'layers'
    with pytest.raises(CatalogError, match="no nodes in layer"):
        select_nodes(catalog, {"layers": ["platinum"]})


def test_notifications_are_config_driven(catalog):
    cfg = {"workflows": [{"name": "x", "select": {"all": True}, "notify_on_failure": ["ops@example.com"]}]}
    job = build_workflow_jobs(catalog, cfg)["gen_x"]
    assert job["email_notifications"] == {"on_failure": ["ops@example.com"]}
    assert Job.from_dict(job)
