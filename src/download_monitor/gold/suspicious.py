"""Suspicious-activity materialized views over `fct_data_downloads`.

Rules (thresholds in config/download_monitor/thresholds.yml, see dm_common/suspicious_logic.py):
  suspicious_pii_downloads            PII evidence (column or table level by default)
  suspicious_large_downloads          large single download, download burst, or large daily volume
  suspicious_untrusted_ip_downloads   source IP outside config/download_monitor/supported_ip_ranges.csv
  suspicious_downloads                union with `reasons` and `risk_score` (feeds dashboard, alerts, export)
  daily_download_summary              day x workspace x channel trend for the dashboard
"""

from pyspark import pipelines as dp

from dm_common import suspicious_logic as S
from dm_common.config_data import CONFIG

T = CONFIG["thresholds"]


def _flagged():
    return S.with_flags(spark.read.table("fct_data_downloads"), T)


@dp.materialized_view(name="suspicious_pii_downloads",
                      comment="Downloads whose source data carries the PII governed tag.")
def suspicious_pii_downloads():
    return _flagged().where("flag_pii")


@dp.materialized_view(name="suspicious_large_downloads",
                      comment="Large single downloads, download bursts per user-hour and large daily volumes.")
def suspicious_large_downloads():
    return _flagged().where("flag_large")


@dp.materialized_view(name="suspicious_untrusted_ip_downloads",
                      comment="Downloads from IP addresses outside the supported IP list.")
def suspicious_untrusted_ip_downloads():
    return _flagged().where("flag_untrusted_ip")


@dp.materialized_view(name="suspicious_downloads",
                      comment="All suspicious downloads with reasons and an additive risk score.",
                      cluster_by=["event_date", "risk_score"])
def suspicious_downloads():
    return S.suspicious_downloads(_flagged())


@dp.materialized_view(name="daily_download_summary",
                      comment="Daily download counts by workspace and channel, with suspicious breakdown.")
def daily_download_summary():
    return S.daily_summary(_flagged())
