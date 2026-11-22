import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("dm-tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def gold_views(spark):
    """Temp views named like the gold tables, built from the fixture world."""
    from dm_common import gold_logic as G, suspicious_logic as S
    from dm_common.config_data import CONFIG
    from tests.download_monitor.test_gold_core import RANGES_ROWS, make_world

    a, history, cl, tl, cols, tbls = make_world(spark)
    fct = G.build_fct_data_downloads(a, history, cl, tl, cols, tbls, G.ref_ip_ranges(spark, RANGES_ROWS), CONFIG)
    flagged = S.with_flags(fct, CONFIG["thresholds"])
    fct.createOrReplaceTempView("fct_data_downloads")
    a.createOrReplaceTempView("bronze_audit")
    for name, df in {
        "suspicious_downloads": S.suspicious_downloads(flagged),
        "suspicious_pii_downloads": flagged.where("flag_pii"),
        "suspicious_large_downloads": flagged.where("flag_large"),
        "suspicious_untrusted_ip_downloads": flagged.where("flag_untrusted_ip"),
        "daily_download_summary": S.daily_summary(flagged),
    }.items():
        df.createOrReplaceTempView(name)
    return spark
