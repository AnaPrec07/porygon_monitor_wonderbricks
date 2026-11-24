"""Suspicious-download rules over `fct_data_downloads` (tested on local Spark)."""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

UNTRUSTED_STATUSES = ("untrusted", "unparsed")

REASON_COLUMNS = [
    ("flag_pii", "pii_data"), ("flag_untrusted_ip", "untrusted_ip"), ("flag_large_single", "large_download"),
    ("flag_burst", "download_burst"), ("flag_daily_volume", "daily_volume"),
]


def with_flags(fct: DataFrame, t: Mapping[str, Any]) -> DataFrame:
    """Adds one boolean flag per rule, `flag_large`, `flag_any`, `reasons` and `risk_score`.

    Principals in `excluded_principals` are dropped. Thresholds come from thresholds.yml.
    """
    d = fct.where(~F.coalesce(F.col("user_email").isin(*t["excluded_principals"]), F.lit(False)))

    hour = F.date_trunc("hour", "event_time")
    per_hour = Window.partitionBy("user_email", hour)
    per_day = Window.partitionBy("user_email", "event_date")
    rows = F.coalesce("rows_returned", F.lit(0))

    d = (
        d.withColumn("flag_pii", F.col("pii_evidence").isin(*t["pii_evidence_levels"]))
        .withColumn("flag_untrusted_ip", F.col("ip_status").isin(*UNTRUSTED_STATUSES))
        .withColumn("flag_large_single",
                    (rows >= t["large_download_rows"])
                    | (F.coalesce("read_bytes", F.lit(0)) >= t["large_download_bytes"]))
        .withColumn("flag_burst",
                    F.col("user_email").isNotNull() & (F.count("*").over(per_hour) > t["burst_downloads_per_hour"]))
        .withColumn("flag_daily_volume",
                    F.col("user_email").isNotNull() & (F.sum(rows).over(per_day) >= t["daily_rows_per_user"]))
        .withColumn("flag_large", F.col("flag_large_single") | F.col("flag_burst") | F.col("flag_daily_volume"))
    )
    d = d.withColumn("flag_any", F.col("flag_pii") | F.col("flag_untrusted_ip") | F.col("flag_large"))
    reasons = F.filter(F.array(*[F.when(F.col(c), F.lit(r)) for c, r in REASON_COLUMNS]), lambda x: x.isNotNull())
    w = t["risk_weights"]
    score = (F.when(F.col("flag_pii"), w["pii"]).otherwise(0)
             + F.when(F.col("flag_untrusted_ip"), w["untrusted_ip"]).otherwise(0)
             + F.when(F.col("flag_large"), w["large"]).otherwise(0))
    return d.withColumn("reasons", reasons).withColumn("risk_score", F.least(score, F.lit(100)))


def suspicious_downloads(flagged: DataFrame) -> DataFrame:
    return flagged.where("flag_any")


def daily_summary(flagged: DataFrame) -> DataFrame:
    return flagged.groupBy("event_date", "workspace_id", "channel", "download_kind").agg(
        F.count("*").alias("downloads"),
        F.countDistinct("user_email").alias("users"),
        F.sum(F.coalesce("rows_returned", F.lit(0))).alias("rows_downloaded"),
        F.sum(F.col("flag_pii").cast("int")).alias("pii_downloads"),
        F.sum(F.col("flag_untrusted_ip").cast("int")).alias("untrusted_ip_downloads"),
        F.sum(F.col("flag_large").cast("int")).alias("large_downloads"),
        F.sum(F.col("flag_any").cast("int")).alias("suspicious_downloads"),
        F.max("risk_score").alias("max_risk_score"),
    )
