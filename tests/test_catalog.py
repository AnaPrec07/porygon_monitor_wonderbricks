import copy

import pytest

from bundle_gen import CatalogError, load_catalog, parse_catalog
from tests.helpers import CONFIG, read_yaml


@pytest.fixture(scope="module")
def raw():
    return read_yaml(CONFIG / "nodes.yml")


def test_real_catalog_loads_and_orders():
    catalog = load_catalog(CONFIG / "nodes.yml")
    order = catalog.topological_order()
    assert order[0] == "bronze_ingestion"
    assert order.index("silver_average_rating") < order.index("gold_property_type_ratings")
    assert order.index("silver_user_categories") < order.index("gold_engagement_kpis")
    assert set(catalog.domains()) == {
        "ingestion", "property_quality", "user_behaviour", "executive_kpis",
    }


def test_descendants():
    catalog = load_catalog(CONFIG / "nodes.yml")
    assert catalog.descendants(["silver_average_rating"]) == {
        "silver_average_rating", "gold_property_type_ratings", "gold_engagement_kpis",
    }
    assert "silver_average_rating" not in catalog.descendants(
        ["silver_average_rating"], include_roots=False
    )


def mutated(raw, fn):
    data = copy.deepcopy(raw)
    fn(data["nodes"])
    return data


def test_cycle_is_rejected(raw):
    bad = mutated(raw, lambda n: n[0].__setitem__("depends_on", ["gold_engagement_kpis"]))
    # also trips the layer-order rule; both must be reported
    with pytest.raises(CatalogError, match="later layer"):
        parse_catalog(bad)


def test_pure_cycle_is_rejected(raw):
    def f(nodes):
        by = {n["id"]: n for n in nodes}
        by["silver_average_rating"]["depends_on"] = ["silver_user_categories"]
        by["silver_user_categories"]["depends_on"] = ["silver_average_rating"]
    with pytest.raises(CatalogError, match="cycle"):
        parse_catalog(mutated(raw, f))


def test_unknown_dependency_and_duplicate_id(raw):
    def f(nodes):
        nodes[1]["depends_on"] = ["nope"]
        nodes.append(dict(nodes[2]))
    with pytest.raises(CatalogError) as exc:
        parse_catalog(mutated(raw, f))
    msg = str(exc.value)
    assert "unknown dependency 'nope'" in msg and "duplicate id" in msg  # reported together


def test_read_without_upstream_writer_is_rejected(raw):
    def f(nodes):
        by = {n["id"]: n for n in nodes}
        by["gold_property_type_ratings"]["reads"] = ["silver.user_categories"]
    with pytest.raises(CatalogError, match="no upstream node writes it"):
        parse_catalog(mutated(raw, f))


def test_kind_requirements(raw):
    def f(nodes):
        by = {n["id"]: n for n in nodes}
        del by["silver_average_rating"]["pipeline"]
    with pytest.raises(CatalogError, match="needs 'pipeline'"):
        parse_catalog(mutated(raw, f))
