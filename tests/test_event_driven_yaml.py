"""Pattern 4 has no central graph, so rebuild it from the triggers and compare.

This test is the price of the pattern made visible: the only place the event-driven
dependencies exist is the table lists inside each job's trigger, and nothing but a
test like this notices when they drift from the intended graph.
"""

import pytest

from bundle_gen import load_catalog
from tests.helpers import CONFIG, ROOT, read_yaml, substitute, yaml_resources


@pytest.fixture(scope="module")
def catalog():
    return load_catalog(CONFIG / "nodes.yml")


@pytest.fixture(scope="module")
def evt_jobs():
    jobs = read_yaml(ROOT / "resources/orchestration/event_driven.yml")["resources"]["jobs"]
    return {k: v for k, v in jobs.items() if k.startswith("evt_")}


def expected_tables(catalog, node):
    return sorted(
        f"main.{ref.split('.')[0]}.{ref.split('.', 1)[1]}" for ref in node.reads
    )


def test_every_node_has_exactly_one_event_job(catalog, evt_jobs):
    assert set(evt_jobs) == {f"evt_{n}" for n in catalog.nodes}


def test_each_trigger_watches_exactly_what_the_node_reads(catalog, evt_jobs):
    for node in catalog.nodes.values():
        if node.kind != "pipeline":
            continue
        job = evt_jobs[f"evt_{node.id}"]
        watched = sorted(substitute(t) for t in job["trigger"]["table_update"]["table_names"])
        assert watched == expected_tables(catalog, node), node.id
        assert job["trigger"]["table_update"]["condition"] == "ALL_UPDATED"


def test_each_job_runs_the_right_pipeline(catalog, evt_jobs):
    for node in catalog.nodes.values():
        if node.kind == "pipeline":
            (task,) = evt_jobs[f"evt_{node.id}"]["tasks"]
            assert task["pipeline_task"]["pipeline_id"] == f"${{resources.pipelines.{node.pipeline}.id}}"
            assert node.pipeline in yaml_resources("pipelines")


def test_only_the_root_runs_on_a_schedule(evt_jobs):
    scheduled = {k for k, j in evt_jobs.items() if "schedule" in j}
    assert scheduled == {"evt_bronze_ingestion"}
    assert all(("trigger" in j) != ("schedule" in j) for j in evt_jobs.values())
