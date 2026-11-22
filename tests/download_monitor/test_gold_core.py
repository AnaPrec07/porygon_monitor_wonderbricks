"""Gold core logic on small fixtures, run with a local Spark session."""

from datetime import datetime

import pytest

from dm_common import gold_logic as G
from dm_common.classify import classify_channel, ip_status, strip_port, cidr_bounds
from dm_common.config_data import CONFIG

T = datetime(2026, 11, 1, 10, 0, 0)
AUDIT_SCHEMA = ("event_id string, event_time timestamp, workspace_id string, service_name string, action_name string,"
                " source_ip_address string, request_params map<string,string>, user_identity struct<email:string>")


def audit(spark, rows):
    return spark.createDataFrame(rows, AUDIT_SCHEMA)


def row(eid, action, email, ip, params=None, service="notebook", t=T):
    return (eid, t, "w1", service, action, ip, params or {}, (email,))


QH_SCHEMA = ("statement_id string, workspace_id string, executed_by string, client_application string,"
             " client_driver string, execution_status string, statement_text string, produced_rows bigint,"
             " read_bytes bigint, start_time timestamp,"
             " query_source struct<dashboard_id:string,legacy_dashboard_id:string,genie_space_id:string,notebook_id:string>")


def qh(spark, rows):
    return spark.createDataFrame(rows, QH_SCHEMA)


def q(sid, user, app=None, driver=None, rows=10, text="select 1", dash=None, status="FINISHED", t=T):
    return (sid, "w1", user, app, driver, status, text, rows, 1000, t, (dash, None, None, None))


RANGES_ROWS = [{"cidr": "10.0.0.0/8", "description": "d", "owner": "o",
                "start_int": cidr_bounds("10.0.0.0/8")[0], "end_int": cidr_bounds("10.0.0.0/8")[1]}]


@pytest.fixture
def ranges(spark):
    return G.ref_ip_ranges(spark, RANGES_ROWS)


def test_clean_ip_matches_python_twin(spark):
    cases = ["1.2.3.4:5678", "1.2.3.4", "[::1]:80", "2001:db8::1", "", "  ", None, "999.1.1.1"]
    df = spark.createDataFrame([(c,) for c in cases], "ip string")
    got = [r[0] for r in df.select(G.clean_ip("ip")).collect()]
    assert got == [strip_port(c) for c in cases]


def test_ip_status_matches_python_twin(spark, ranges):
    cases = ["10.1.2.3", "10.1.2.3:443", "8.8.8.8", "127.0.0.1", "0.0.0.0", "2001:db8::1", "999.1.1.1", "", None]
    df = spark.createDataFrame([(c,) for c in cases], "raw string").withColumn("ip", G.clean_ip("raw"))
    got = {r["raw"]: r["ip_status"] for r in G.with_ip_status(df, "ip", ranges).collect()}
    rb = [cidr_bounds("10.0.0.0/8")]
    assert got == {c: ip_status(c, rb) for c in cases}


def test_channel_case_matches_python_twin(spark):
    cases = [
        {"dashboard_id": "d1", "client_application": "Excel"}, {"genie_space_id": "g"}, {"notebook_id": "n"},
        {"client_application": "Microsoft Excel"}, {"client_application": "Databricks SQL Editor"},
        {"client_driver": "Databricks SQL Connector for Python"}, {"client_driver": "Simba ODBC"},
        {"client_application": "Tableau Desktop"}, {"client_application": "other thing"}, {},
    ]
    rows = [(i, c.get("client_application"), c.get("client_driver"),
             (c.get("dashboard_id"), None, c.get("genie_space_id"), c.get("notebook_id")))
            for i, c in enumerate(cases)]
    df = spark.createDataFrame(rows, "i int, client_application string, client_driver string, "
                                     "query_source struct<dashboard_id:string,legacy_dashboard_id:string,"
                                     "genie_space_id:string,notebook_id:string>")
    got = {r["i"]: r["ch"] for r in df.select("i", G.channel_case(CONFIG["channels"]).alias("ch")).collect()}
    assert got == {i: classify_channel(c, CONFIG["channels"]) for i, c in enumerate(cases)}


def test_pii_reference_tables(spark):
    ct = spark.createDataFrame([
        ("main", "hr", "emp", "ssn", "PII", "ssn"), ("main", "hr", "emp", "name", "pii", "name"),
        ("main", "hr", "emp", "dept", "other", "x"),
    ], "catalog_name string, schema_name string, table_name string, column_name string, tag_name string, tag_value string")
    cols = {tuple(r) for r in G.pii_columns(ct, "PII").select("table_full_name", "column_name").collect()}
    assert cols == {("main.hr.emp", "ssn"), ("main.hr.emp", "name")}

    tt = spark.createDataFrame([("main", "fin", "pay", "PII", "card")],
                               "catalog_name string, schema_name string, table_name string, tag_name string, tag_value string")
    st = spark.createDataFrame([("main", "crm", "PII", "contact")],
                               "catalog_name string, schema_name string, tag_name string, tag_value string")
    ca = spark.createDataFrame([], "catalog_name string, tag_name string, tag_value string")
    scopes = {(r["scope"], r["table_full_name"]) for r in G.pii_tables(ct, tt, st, ca, "PII").collect()}
    assert scopes == {("column", "main.hr.emp"), ("table", "main.fin.pay"), ("schema", None)}


def test_explicit_downloads_filter_and_channel(spark):
    a = audit(spark, [
        row("e1", "downloadPreviewResults", "ana@x.com", "10.1.1.1:443"),
        row("e2", "downloadQueryResult", "ana@x.com", "8.8.8.8", {"fileType": "csv", "statementId": "s1"}),
        row("e3", "downloadQueryResult", "ana@x.com", "8.8.8.8", {"fileType": "arrows"}),   # ignored
        row("e4", "workspaceExport", "ana@x.com", "8.8.8.8", {"workspaceExportFormat": "SOURCE"}),  # ignored
        row("e5", "login", "ana@x.com", "8.8.8.8"),
        row("e6", "getModelVersionDownloadUri", "ana@x.com", "8.8.8.8"),  # disabled
    ])
    got = {r["download_id"]: r for r in G.explicit_downloads(a, CONFIG["actions"]).collect()}
    assert set(got) == {"e1", "e2"}
    assert got["e1"]["channel"] == "notebook" and got["e1"]["source_ip"] == "10.1.1.1"
    assert got["e2"]["statement_id"] == "s1" and got["e2"]["channel"] == "sql_editor"


def test_latest_query_row_wins(spark):
    h = qh(spark, [q("s1", "a@x.com", rows=1, t=datetime(2026, 11, 1, 9)), q("s1", "a@x.com", rows=99, t=datetime(2026, 11, 1, 11))])
    r = G.latest_queries(h, CONFIG["channels"]).collect()
    assert len(r) == 1 and r[0]["rows_returned"] == 99 and r[0]["start_time"] == datetime(2026, 11, 1, 11)


@pytest.fixture
def world(spark):
    """Four statements: excel on PII column, dashboard on PII table, plain sql, excel clean."""
    pii_ct = spark.createDataFrame([("main", "hr", "emp", "ssn", "PII", "ssn")],
                                   "catalog_name string, schema_name string, table_name string, column_name string, tag_name string, tag_value string")
    empty_t = spark.createDataFrame([], "catalog_name string, schema_name string, table_name string, tag_name string, tag_value string")
    empty_s = spark.createDataFrame([], "catalog_name string, schema_name string, tag_name string, tag_value string")
    empty_c = spark.createDataFrame([], "catalog_name string, tag_name string, tag_value string")
    cols = G.pii_columns(pii_ct, "PII")
    tbls = G.pii_tables(pii_ct, empty_t, empty_s, empty_c, "PII")
    history = qh(spark, [
        q("s_excel_pii", "ana@x.com", app="Microsoft Excel", rows=500, text="select ssn from main.hr.emp"),
        q("s_dash_tbl", "bob@x.com", dash="d1", rows=20, text="select * from v"),
        q("s_excel_clean", "ana@x.com", app="Microsoft Excel", rows=5, text="select 1 from main.sales.orders"),
        q("s_excel_failed", "ana@x.com", app="Microsoft Excel", rows=5, status="FAILED"),
        q("s_text_only", "cy@x.com", driver="Simba ODBC", rows=7, text="SELECT * FROM Main.HR.Emp"),
    ])
    cl = spark.createDataFrame([("s_excel_pii", "main.hr.emp", "ssn"), ("s_excel_clean", "main.sales.orders", "id")],
                               "statement_id string, source_table_full_name string, source_column_name string")
    tl = spark.createDataFrame([("s_excel_pii", "main.hr.emp"), ("s_dash_tbl", "main.hr.emp"), ("s_excel_clean", "main.sales.orders")],
                               "statement_id string, source_table_full_name string")
    a = audit(spark, [
        row("e_dl", "downloadQueryResult", "bob@x.com", "8.8.8.8:1", {"fileType": "csv", "statementId": "s_dash_tbl"}),
        row("v1", "commandSubmit", "ana@x.com", "10.9.9.9", {"statementId": "s_excel_pii"}, service="databrickssql"),
        row("login1", "login", "ana@x.com", "10.9.9.9", service="accounts"),
        row("login2", "login", "ana@x.com", "8.8.4.4", service="accounts"),
    ])
    return a, history, cl, tl, cols, tbls


def test_fct_data_downloads(spark, ranges, world):
    a, history, cl, tl, cols, tbls = world
    fct = G.build_fct_data_downloads(a, history, cl, tl, cols, tbls, ranges, CONFIG)
    got = {r["download_id"]: r for r in fct.collect()}

    # external-client results: finished excel/odbc statements with rows; the dashboard one is not external
    assert set(got) == {"e_dl", "s_excel_pii", "s_excel_clean", "s_text_only"}

    pii = got["s_excel_pii"]
    assert (pii["channel"], pii["download_kind"], pii["rows_returned"]) == ("excel", "external_client", 500)
    assert pii["pii_evidence"] == "column" and pii["pii_columns"] == ["main.hr.emp.ssn"]
    assert pii["pii_tag_values"] == ["ssn"]
    assert (pii["source_ip"], pii["ip_attribution"], pii["ip_status"]) == ("10.9.9.9", "verbose_audit", "trusted")

    clean = got["s_excel_clean"]  # no verbose audit: the user's worst IP of the day is used (8.8.4.4)
    assert clean["pii_evidence"] == "none" and clean["pii_columns"] == []
    assert (clean["ip_attribution"], clean["ip_status"], clean["source_ip"]) == ("inferred", "untrusted", "8.8.4.4")

    dash = got["e_dl"]  # explicit download joined to a dashboard statement with table-level lineage only
    assert (dash["channel"], dash["download_kind"], dash["rows_returned"]) == ("dashboard", "explicit", 20)
    assert dash["pii_evidence"] == "table" and dash["ip_status"] == "untrusted" and dash["ip_attribution"] == "direct"

    txt = got["s_text_only"]  # no lineage: only the table name in the statement text
    assert txt["pii_evidence"] == "text" and txt["channel"] == "odbc_jdbc_other"
    assert "statement_text" not in fct.columns and "statement_text_preview" in fct.columns


def test_service_principal_flag(spark, ranges, world):
    a, history, cl, tl, cols, tbls = world
    a = a.unionByName(audit(spark, [row("e_sp", "downloadPreviewResults", "fac0c18d-0db4", "10.1.1.1")]))
    got = {r["download_id"]: r["is_service_principal"] for r in
           G.build_fct_data_downloads(a, history, cl, tl, cols, tbls, ranges, CONFIG).collect()}
    assert got["e_sp"] is True and got["e_dl"] is False


def test_missing_optional_columns_do_not_fail(spark, ranges):
    a = spark.createDataFrame([("e1", T, "w", "notebook", "downloadPreviewResults", "1.1.1.1", ("a@x.com",))],
                              "event_id string, event_time timestamp, workspace_id string, service_name string,"
                              " action_name string, source_ip_address string, user_identity struct<email:string>")
    h = spark.createDataFrame([], "statement_id string, start_time timestamp")
    empty = spark.createDataFrame([], "statement_id string, source_table_full_name string, source_column_name string")
    cols = spark.createDataFrame([], "table_full_name string, column_name string, tag_value string")
    tbls = spark.createDataFrame([], "catalog_name string, schema_name string, table_name string, table_full_name string, tag_value string")
    out = G.build_fct_data_downloads(a, h, empty, empty.drop("source_column_name"), cols, tbls, ranges, CONFIG).collect()
    assert len(out) == 1 and out[0]["pii_evidence"] == "none"
