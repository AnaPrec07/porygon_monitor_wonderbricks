import pytest

from dm_common.classify import (
    cidr_bounds, classify_channel, ip_status, is_download_event,
    is_service_principal, strip_port,
)
from dm_common.config_data import CONFIG

RANGES = [cidr_bounds("10.0.0.0/8"), cidr_bounds("203.0.113.0/24")]


@pytest.mark.parametrize("raw,expected", [
    ("1.2.3.4:5678", "1.2.3.4"), ("1.2.3.4", "1.2.3.4"), ("[::1]:80", "::1"),
    ("2001:db8::1", "2001:db8::1"), ("", None), ("  ", None), (None, None),
])
def test_strip_port(raw, expected):
    assert strip_port(raw) == expected


@pytest.mark.parametrize("ip,expected", [
    ("10.1.2.3", "trusted"), ("10.1.2.3:443", "trusted"), ("203.0.113.9", "trusted"),
    ("203.0.114.1", "untrusted"), ("8.8.8.8", "untrusted"),
    ("127.0.0.1", "internal"), ("0.0.0.0", "internal"),
    ("2001:db8::1", "unparsed"), ("not-an-ip", "unparsed"),
    ("", "unknown"), (None, "unknown"),
])
def test_ip_status(ip, expected):
    assert ip_status(ip, RANGES) == expected


def test_overlapping_ranges_are_still_trusted():
    assert ip_status("10.1.1.1", [cidr_bounds("10.0.0.0/8"), cidr_bounds("10.1.0.0/16")]) == "trusted"


def test_cidr_bounds():
    assert cidr_bounds("192.168.1.0/24") == (3232235776, 3232236031)
    with pytest.raises(ValueError):
        cidr_bounds("2001:db8::/32")


def test_service_principal():
    assert is_service_principal("fac0c18d-0db4-47f4-a020-99739bf422c7")
    assert not is_service_principal("ana@example.com")
    assert not is_service_principal("System-User")
    assert not is_service_principal(None)


@pytest.mark.parametrize("row,channel", [
    ({"dashboard_id": "d1", "client_application": "Excel"}, "dashboard"),
    ({"genie_space_id": "g"}, "genie"),
    ({"notebook_id": "n"}, "notebook"),
    ({"client_application": "Microsoft Excel"}, "excel"),
    ({"client_application": "Databricks SQL Editor"}, "sql_editor"),
    ({"client_driver": "Databricks SQL Connector for Python"}, "api_connector"),
    ({"client_driver": "Simba ODBC"}, "odbc_jdbc_other"),
    ({"client_application": "Tableau Desktop"}, "odbc_jdbc_other"),
    ({"client_application": "something else"}, "other"),
    ({}, "other"),
])
def test_channel(row, channel):
    assert classify_channel(row, CONFIG["channels"]) == channel


def test_download_events():
    cfg = CONFIG["actions"]
    assert is_download_event("downloadPreviewResults", {}, cfg)
    assert is_download_event("downloadQueryResult", {"fileType": "csv"}, cfg)
    assert not is_download_event("downloadQueryResult", {"fileType": "arrows"}, cfg)
    assert not is_download_event("workspaceExport", {"workspaceExportFormat": "SOURCE"}, cfg)
    assert is_download_event("workspaceExport", {"workspaceExportFormat": "DBC"}, cfg)
    assert not is_download_event("getModelVersionDownloadUri", None, cfg)  # disabled
    assert not is_download_event("login", None, cfg)
