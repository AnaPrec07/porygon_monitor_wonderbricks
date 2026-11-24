"""Pure-Python rules shared by the pipelines and the tests (no Spark needed).

Each rule here has a Spark SQL twin in `sql_rules.py`; tests run both on the same
fixtures so they cannot drift apart.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any, Iterable, Mapping

_V4_PORT = re.compile(r"^(\d{1,3}(?:\.\d{1,3}){3}):\d+$")
_V6_BRACKET = re.compile(r"^\[([0-9a-fA-F:.]+)\](?::\d+)?$")

INTERNAL_IPS = {"0.0.0.0", "127.0.0.1", "::1"}


def strip_port(ip: str | None) -> str | None:
    """'1.2.3.4:5678' -> '1.2.3.4', '[::1]:80' -> '::1', '' -> None."""
    if ip is None:
        return None
    ip = ip.strip()
    if not ip:
        return None
    m = _V4_PORT.match(ip) or _V6_BRACKET.match(ip)
    return m.group(1) if m else ip


def ipv4_to_int(ip: str) -> int | None:
    try:
        return int(ipaddress.IPv4Address(ip))
    except ValueError:
        return None


def cidr_bounds(cidr: str) -> tuple[int, int]:
    """First and last address of an IPv4 CIDR as integers (raises on invalid or IPv6 input)."""
    net = ipaddress.IPv4Network(cidr.strip(), strict=False)
    return int(net.network_address), int(net.broadcast_address)


def ip_status(ip: str | None, ranges: Iterable[tuple[int, int]]) -> str:
    """trusted | untrusted | internal | unparsed (IPv6 or garbage) | unknown (no IP)."""
    clean = strip_port(ip)
    if clean is None:
        return "unknown"
    if clean in INTERNAL_IPS:
        return "internal"
    value = ipv4_to_int(clean)
    if value is None:
        return "unparsed"
    return "trusted" if any(lo <= value <= hi for lo, hi in ranges) else "untrusted"


def is_service_principal(email: str | None) -> bool:
    """Service principals appear with an application id instead of an email address."""
    return bool(email) and "@" not in email and email != "System-User"


def _matches(cond: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    value = row.get(cond["field"])
    if cond.get("not_null"):
        return value is not None
    return value is not None and str(cond["contains"]).lower() in str(value).lower()


def classify_channel(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> str:
    for rule in cfg["rules"]:
        if any(_matches(c, row) for c in rule["any"]):
            return rule["channel"]
    return cfg["default_channel"]


def download_action_lookup(cfg: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Enabled download actions by name."""
    return {a["action"]: a for a in cfg["actions"] if a.get("enabled", True)}


def is_download_event(action: str, request_params: Mapping[str, str] | None,
                      cfg: Mapping[str, Any]) -> bool:
    spec = download_action_lookup(cfg).get(action)
    if spec is None:
        return False
    skip = spec.get("ignore_when")
    return not (skip and (request_params or {}).get(skip["param"]) == skip["equals"])
