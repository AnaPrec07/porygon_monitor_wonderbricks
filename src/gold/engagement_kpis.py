"""Gold `engagement_kpis` dataset (Lakeflow Spark Declarative Pipeline).

Single-row headline KPIs for dashboard counters, combining silver
`user_categories` (funnel / revenue) and `average_rating` (quality).

Pipeline configuration keys:
  silver_catalog, silver_schema  where the silver tables live
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="engagement_kpis",
    comment="Headline engagement, conversion, revenue and rating KPIs (one row).",
)
def engagement_kpis():
    catalog = spark.conf.get("silver_catalog")
    schema = spark.conf.get("silver_schema")

    users = spark.read.table(f"`{catalog}`.`{schema}`.`user_categories`")
    ratings = spark.read.table(f"`{catalog}`.`{schema}`.`average_rating`")

    booked = F.col("booking_count") > 0

    user_kpis = users.agg(
        F.count("*").alias("total_users"),
        F.sum(F.when(booked, 1).otherwise(0)).alias("booking_users"),
        F.sum("page_view_count").alias("total_page_views"),
        F.sum("booking_count").alias("total_bookings"),
        F.round(F.sum("total_spent"), 2).alias("total_revenue"),
    ).select(
        "*",
        F.round(100 * F.col("booking_users") / F.col("total_users"), 2).alias(
            "conversion_rate_pct"
        ),
        F.round(F.col("total_revenue") / F.col("total_bookings"), 2).alias(
            "avg_booking_value"
        ),
    )

    rating_kpis = ratings.agg(
        F.round(F.avg("avg_rating"), 2).alias("top_properties_mean_rating"),
        F.max("avg_rating").alias("best_property_rating"),
    )

    return user_kpis.crossJoin(rating_kpis)
