"""Generates download_monitor.lvdash.json (AI/BI dashboard) from the definitions below.

Keeping the definition in Python makes the layout reviewable and lets the tests run every
dataset query on local Spark. Regenerate with:

    uv run python src/download_monitor/dashboards/build_dashboard.py

You can also open the deployed dashboard in the UI, adjust it, and export it back with
`databricks bundle generate dashboard --existing-id <id>`; if you do, drop this generator.
Dataset queries use unqualified table names: the bundle sets dataset_catalog/dataset_schema.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

OUT = Path(__file__).resolve().parent / "download_monitor.lvdash.json"

DOWNLOAD_COLS = ("download_id, event_time, event_date, workspace_id, user_email, channel, download_kind, "
                 "action_name, rows_returned, read_bytes, source_ip, ip_status, ip_attribution, "
                 "pii_evidence, pii_columns, pii_tag_values")

DATASETS = {
    "downloads": ("All downloads", f"SELECT {DOWNLOAD_COLS} FROM fct_data_downloads"),
    "suspicious": ("Suspicious downloads",
                   f"SELECT {DOWNLOAD_COLS}, reasons, risk_score FROM suspicious_downloads"),
    "daily": ("Daily summary",
              "SELECT event_date, workspace_id, channel, download_kind, downloads, users, rows_downloaded, "
              "pii_downloads, untrusted_ip_downloads, large_downloads, suspicious_downloads "
              "FROM daily_download_summary"),
    "pii_tags": ("PII downloads by tag",
                 "SELECT event_date, channel, user_email, tag AS pii_tag FROM fct_data_downloads "
                 "LATERAL VIEW explode(pii_tag_values) t AS tag WHERE pii_evidence IN ('column', 'table')"),
    "ips": ("IP addresses",
            "SELECT event_date, channel, source_ip, ip_status, user_email, download_id FROM fct_data_downloads "
            "WHERE source_ip IS NOT NULL"),
    "freshness": ("Freshness",
                  "SELECT max(event_time) AS last_download_time, count(*) AS total_downloads FROM fct_data_downloads"),
}


def _fields(*specs):
    return [{"name": n, "expression": e} for n, e in specs]


def _col(name):
    return (name, f"`{name}`")


def _widget(name, dataset, fields, spec, disaggregated=False):
    return {
        "name": name,
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset, "fields": _fields(*fields), "disaggregated": disaggregated}}],
        "spec": spec,
    }


def _frame(title):
    return {"showTitle": True, "title": title}


def counter(name, title, dataset, expr, label):
    return _widget(name, dataset, [("value", expr)], {
        "version": 2, "widgetType": "counter", "frame": _frame(title),
        "encodings": {"value": {"fieldName": "value", "displayName": label}}})


def bar(name, title, dataset, x, y_expr, y_label, color=None, x_label=None):
    fields = [_col(x), ("value", y_expr)] + ([_col(color)] if color else [])
    enc = {"x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": x_label or x},
           "y": {"fieldName": "value", "scale": {"type": "quantitative"}, "displayName": y_label}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return _widget(name, dataset, fields, {"version": 3, "widgetType": "bar", "frame": _frame(title), "encodings": enc})


def line(name, title, dataset, x, y_expr, y_label, color=None):
    fields = [_col(x), ("value", y_expr)] + ([_col(color)] if color else [])
    enc = {"x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": x},
           "y": {"fieldName": "value", "scale": {"type": "quantitative"}, "displayName": y_label}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return _widget(name, dataset, fields, {"version": 3, "widgetType": "line", "frame": _frame(title), "encodings": enc})


def table(name, title, dataset, columns):
    return _widget(name, dataset, [_col(c) for c in columns], {
        "version": 2, "widgetType": "table", "frame": _frame(title),
        "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in columns]}}, disaggregated=True)


def date_filter(name, title, datasets, field):
    return _filter(name, title, datasets, field, "filter-date-range-picker")


def multi_filter(name, title, datasets, field):
    return _filter(name, title, datasets, field, "filter-multi-select")


def _filter(name, title, datasets, field, kind):
    queries = [{"name": f"q_{d}", "query": {"datasetName": d, "fields": _fields(_col(field)), "disaggregated": False}}
               for d in datasets]
    return {"name": name, "queries": queries, "spec": {
        "version": 2, "widgetType": kind, "frame": _frame(title),
        "encodings": {"fields": [{"fieldName": field, "queryName": f"q_{d}", "displayName": title} for d in datasets]}}}


def place(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def _filters(datasets):
    return [place(date_filter("f_date", "Date", datasets, "event_date"), 0, 0, 2, 2),
            place(multi_filter("f_channel", "Channel", datasets, "channel"), 2, 0, 2, 2),
            place(multi_filter("f_user", "User", [d for d in datasets if d != "daily"], "user_email"), 4, 0, 2, 2)]


def pages():
    both = ["downloads", "suspicious", "daily"]
    return [
        ("overview", "Overview", [
            *_filters(both),
            place(counter("c_downloads", "Downloads", "daily", "SUM(`downloads`)", "Downloads"), 0, 2, 1, 3),
            place(counter("c_susp", "Suspicious downloads", "daily", "SUM(`suspicious_downloads`)", "Suspicious"), 1, 2, 1, 3),
            place(counter("c_pii", "PII downloads", "daily", "SUM(`pii_downloads`)", "PII"), 2, 2, 1, 3),
            place(counter("c_ip", "Untrusted IP downloads", "daily", "SUM(`untrusted_ip_downloads`)", "Untrusted IP"), 3, 2, 1, 3),
            place(counter("c_rows", "Rows downloaded", "daily", "SUM(`rows_downloaded`)", "Rows"), 4, 2, 2, 3),
            place(line("l_trend", "Downloads per day by channel", "daily", "event_date", "SUM(`downloads`)",
                       "Downloads", "channel"), 0, 5, 6, 6),
            place(bar("b_risk", "Suspicious downloads by risk score", "suspicious", "risk_score",
                      "COUNT(`*`)", "Downloads"), 0, 11, 6, 5),
        ]),
        ("triage", "Triage", [
            *_filters(["suspicious"]),
            place(table("t_triage", "Suspicious downloads (highest risk first)", "suspicious",
                        ["risk_score", "reasons", "event_time", "user_email", "channel", "source_ip", "ip_status",
                         "rows_returned", "pii_evidence", "pii_columns", "workspace_id", "download_id"]), 0, 2, 6, 14),
        ]),
        ("sensitive", "Sensitive data", [
            *_filters(["pii_tags"]),
            place(bar("b_tag", "PII downloads by tag value", "pii_tags", "pii_tag", "COUNT(`*`)", "Downloads"), 0, 2, 3, 6),
            place(bar("b_evidence", "Downloads by PII evidence level", "downloads", "pii_evidence", "COUNT(`*`)",
                      "Downloads"), 3, 2, 3, 6),
            place(bar("b_pii_user", "PII downloads by user", "pii_tags", "user_email", "COUNT(`*`)", "Downloads"), 0, 8, 6, 6),
        ]),
        ("large", "Large downloads", [
            *_filters(["suspicious"]),
            place(bar("b_rows_user", "Rows downloaded by user (suspicious downloads)", "suspicious", "user_email",
                      "SUM(`rows_returned`)", "Rows"), 0, 2, 6, 6),
            place(table("t_large", "Largest downloads", "suspicious",
                        ["rows_returned", "read_bytes", "event_time", "user_email", "channel", "reasons"]), 0, 8, 6, 8),
        ]),
        ("ips", "IP addresses", [
            *_filters(["ips"]),
            place(bar("b_ip_status", "Downloads by IP status", "ips", "event_date", "COUNT(`*`)", "Downloads",
                      color="ip_status"), 0, 2, 6, 6),
            place(bar("b_ip_top", "Top source IPs", "ips", "source_ip", "COUNT(`*`)", "Downloads", color="ip_status"), 0, 8, 6, 6),
        ]),
        ("users", "User drill-down", [
            *_filters(["downloads"]),
            place(table("t_user", "All downloads", "downloads",
                        ["event_time", "user_email", "channel", "action_name", "rows_returned", "source_ip", "ip_status",
                         "pii_evidence", "download_kind", "download_id"]), 0, 2, 6, 14),
        ]),
        ("quality", "Data quality", [
            place(counter("q_last", "Last download seen", "freshness", "MAX(`last_download_time`)", "Last download time"), 0, 0, 3, 3),
            place(counter("q_total", "Downloads in the fact table", "freshness", "SUM(`total_downloads`)", "Downloads"), 3, 0, 3, 3),
            place(bar("q_attr", "IP attribution quality", "downloads", "ip_attribution", "COUNT(`*`)", "Downloads"), 0, 3, 3, 6),
            place(bar("q_pii", "PII evidence levels", "downloads", "pii_evidence", "COUNT(`*`)", "Downloads"), 3, 3, 3, 6),
            place(bar("q_status", "IP status", "downloads", "ip_status", "COUNT(`*`)", "Downloads"), 0, 9, 6, 5),
        ]),
    ]


def build() -> dict:
    return {
        "datasets": [{"name": n, "displayName": d, "queryLines": [q]}
                     for n, (d, q) in DATASETS.items()],
        "pages": [{"name": n, "displayName": d, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}
                  for n, d, layout in pages()],
        "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGN_AUTO"}},
    }


def render() -> str:
    return json.dumps(build(), indent=2) + "\n"


def referenced_columns(expression: str) -> set[str]:
    return set(re.findall(r"`([^`]+)`", expression)) - {"*"}


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
