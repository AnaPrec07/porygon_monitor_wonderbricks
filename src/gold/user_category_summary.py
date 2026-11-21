"""Gold `user_category_summary` dataset (Lakeflow Spark Declarative Pipeline).

One row per behavioural user category (from silver `user_categories`) with user
counts, share of all users, engagement and revenue. Feeds the dashboard's
user-segmentation charts.

Pipeline configuration keys:
  silver_catalog, silver_schema  where the silver tables live
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="user_category_summary",
    comment="Users, engagement and revenue per behavioural user category.",
)
@dp.expect_or_drop("category_not_null", "user_category IS NOT NULL")
def user_category_summary():
    catalog = spark.conf.get("silver_catalog")
    schema = spark.conf.get("silver_schema")

    users = spark.read.table(f"`{catalog}`.`{schema}`.`user_categories`")

    return (
        users.groupBy("user_category")
        .agg(
            F.count("*").alias("user_count"),
            F.round(F.avg("page_view_count"), 2).alias("avg_page_views"),
            F.sum("booking_count").alias("total_bookings"),
            F.round(F.sum("total_spent"), 2).alias("total_revenue"),
            F.round(F.avg("total_spent"), 2).alias("avg_spent_per_user"),
        )
        .withColumn(
            "user_share_pct",
            F.round(
                100 * F.col("user_count") / F.sum("user_count").over(Window.partitionBy()),
                2,
            ),
        )
        .orderBy(F.col("user_count").desc())
    )
