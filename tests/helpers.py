"""Shared helpers for the orchestration tests."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterator

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config" / "orchestration"
REF = re.compile(r"\$\{([^}]+)\}")


def read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def bundle_variables() -> dict[str, Any]:
    return read_yaml(ROOT / "databricks.yml")["variables"]


def yaml_resources(kind: str) -> dict[str, Any]:
    """All `resources.<kind>` entries declared in resources/**/*.yml."""
    out: dict[str, Any] = {}
    for path in sorted((ROOT / "resources").rglob("*.yml")):
        out.update((read_yaml(path).get("resources") or {}).get(kind) or {})
    return out


def walk_strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from walk_strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from walk_strings(v)


def refs_in(value: Any) -> set[str]:
    return {m for s in walk_strings(value) for m in REF.findall(s)}


def substitute(text: str) -> str:
    """Resolve ${var.x} with the defaults from databricks.yml (for comparisons)."""
    variables = bundle_variables()

    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name.startswith("var."):
            return str(variables[name[4:]]["default"])
        return m.group(0)

    return REF.sub(repl, text)


def edges(tasks: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """(upstream, downstream) pairs from a job's tasks."""
    return {
        (d["task_key"], t["task_key"])
        for t in tasks
        for d in t.get("depends_on", [])
    }
