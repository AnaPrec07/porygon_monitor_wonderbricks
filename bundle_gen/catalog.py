"""The medallion graph as data: load, validate and query `config/orchestration/nodes.yml`.

Pure Python (no Databricks imports) so it can be unit-tested and reused by both
generators. All problems found while parsing are reported together in one
`CatalogError`, so a bad config fails CI with a complete list, not one error at a
time.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

LAYER_RANK = {"bronze": 0, "silver": 1, "gold": 2}
KINDS = ("ingest", "pipeline")
_NODE_KEYS = {
    "id", "layer", "domain", "kind", "job", "pipeline", "depends_on", "reads", "writes",
}


class CatalogError(ValueError):
    """The graph config is invalid. The message lists every problem found."""


@dataclass(frozen=True)
class Node:
    id: str
    layer: str
    domain: str
    kind: str
    job: str | None = None
    pipeline: str | None = None
    depends_on: tuple[str, ...] = ()
    reads: tuple[str, ...] = ()
    writes: tuple[str, ...] = ()


@dataclass(frozen=True)
class Catalog:
    """Nodes in declaration order, keyed by id."""

    nodes: Mapping[str, Node]

    def domains(self) -> list[str]:
        """Domain names in order of first appearance."""
        return list(dict.fromkeys(n.domain for n in self.nodes.values()))

    def domain_nodes(self, domain: str) -> list[Node]:
        return [n for n in self.nodes.values() if n.domain == domain]

    def topological_order(self, only: Iterable[str] | None = None) -> list[str]:
        """Deterministic topological order (declaration order breaks ties).

        With `only`, orders just that subset and ignores edges leaving it.
        """
        keep = set(self.nodes if only is None else only)
        remaining = [i for i in self.nodes if i in keep]
        done: list[str] = []
        while remaining:
            ready = [
                i for i in remaining
                if all(d in done for d in self.nodes[i].depends_on if d in keep)
            ]
            if not ready:
                raise CatalogError(f"dependency cycle among: {', '.join(remaining)}")
            done.append(ready[0])
            remaining.remove(ready[0])
        return done

    def ancestors(self, node_id: str) -> set[str]:
        seen: set[str] = set()
        stack = list(self.nodes[node_id].depends_on)
        while stack:
            cur = stack.pop()
            if cur not in seen:
                seen.add(cur)
                stack.extend(self.nodes[cur].depends_on)
        return seen

    def descendants(self, roots: Iterable[str], include_roots: bool = True) -> set[str]:
        roots = set(roots)
        out = set(roots) if include_roots else set()
        grew = True
        while grew:
            grew = False
            for n in self.nodes.values():
                if n.id not in out and any(d in roots | out for d in n.depends_on):
                    out.add(n.id)
                    grew = True
        return out if include_roots else out - roots

    def external_dependencies(self, node: Node) -> list[str]:
        """Dependencies that live in a different domain."""
        return [d for d in node.depends_on if self.nodes[d].domain != node.domain]


def _as_tuple(value: Any) -> tuple[str, ...]:
    return tuple(value or ())


def parse_catalog(raw: Mapping[str, Any]) -> Catalog:
    errors: list[str] = []
    nodes: dict[str, Node] = {}

    for i, entry in enumerate(raw.get("nodes") or []):
        where = f"nodes[{i}]"
        node_id = entry.get("id")
        if not node_id:
            errors.append(f"{where}: missing 'id'")
            continue
        where = f"node '{node_id}'"
        if node_id in nodes:
            errors.append(f"{where}: duplicate id")
            continue
        if unknown := set(entry) - _NODE_KEYS:
            errors.append(f"{where}: unknown keys {sorted(unknown)}")
        if entry.get("layer") not in LAYER_RANK:
            errors.append(f"{where}: layer must be one of {list(LAYER_RANK)}")
        if entry.get("kind") not in KINDS:
            errors.append(f"{where}: kind must be one of {list(KINDS)}")
        if not entry.get("domain"):
            errors.append(f"{where}: missing 'domain'")
        if entry.get("kind") == "pipeline" and not entry.get("pipeline"):
            errors.append(f"{where}: kind 'pipeline' needs 'pipeline'")
        if entry.get("kind") == "ingest" and not entry.get("job"):
            errors.append(f"{where}: kind 'ingest' needs 'job'")
        nodes[node_id] = Node(
            id=node_id,
            layer=entry.get("layer", ""),
            domain=entry.get("domain", ""),
            kind=entry.get("kind", ""),
            job=entry.get("job"),
            pipeline=entry.get("pipeline"),
            depends_on=_as_tuple(entry.get("depends_on")),
            reads=_as_tuple(entry.get("reads")),
            writes=_as_tuple(entry.get("writes")),
        )

    if not nodes:
        errors.append("no nodes defined")

    for n in nodes.values():
        for dep in n.depends_on:
            if dep == n.id:
                errors.append(f"node '{n.id}': depends on itself")
            elif dep not in nodes:
                errors.append(f"node '{n.id}': unknown dependency '{dep}'")
            elif (
                n.layer in LAYER_RANK
                and nodes[dep].layer in LAYER_RANK
                and LAYER_RANK[nodes[dep].layer] > LAYER_RANK[n.layer]
            ):
                errors.append(
                    f"node '{n.id}' ({n.layer}) depends on later layer "
                    f"'{dep}' ({nodes[dep].layer})"
                )
        for ref in (*n.reads, *n.writes):
            layer, _, table = ref.partition(".")
            if layer not in LAYER_RANK or not table:
                errors.append(f"node '{n.id}': '{ref}' must look like <layer>.<table>")

    catalog = Catalog(nodes)
    if not errors:  # cycle and lineage checks need a structurally sound graph
        try:
            catalog.topological_order()
        except CatalogError as exc:
            errors.append(str(exc))
        else:
            errors.extend(_lineage_errors(catalog))

    if errors:
        raise CatalogError("invalid orchestration graph:\n  - " + "\n  - ".join(errors))
    return catalog


def _lineage_errors(catalog: Catalog) -> list[str]:
    """Every table a node reads must be written by one of its upstream nodes."""
    errors = []
    for n in catalog.nodes.values():
        produced = {
            w
            for a in catalog.ancestors(n.id)
            for w in catalog.nodes[a].writes
        }
        for ref in n.reads:
            layer = ref.split(".", 1)[0]
            if ref not in produced and f"{layer}.*" not in produced:
                errors.append(
                    f"node '{n.id}' reads '{ref}' but no upstream node writes it"
                )
    return errors


def load_yaml(path: Path | str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_catalog(path: Path | str) -> Catalog:
    return parse_catalog(load_yaml(path))


def table_ref(ref: str) -> str:
    """`silver.average_rating` -> `${var.catalog}.${var.silver_schema}.average_rating`."""
    layer, _, table = ref.partition(".")
    return f"${{var.catalog}}.${{var.{layer}_schema}}.{table}"
