"""Silver `user_categories` dataset (Lakeflow Spark Declarative Pipeline).

Combines each user's clickstream (`page_views`) with their `bookings` and assigns
a behavioural category:

  booker          2+ confirmed bookings
  new_booker      exactly 1 confirmed booking
  high_intent     no confirmed booking, but >= `high_intent_views` page views
  browser         some page views, no confirmed booking
  inactive        neither page views nor confirmed bookings

Pipeline configuration keys:
  bronze_catalog, bronze_schema  where the bronze tables live
  high_intent_views (default 10)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="user_categories",
    comment="Per-user behavioural category derived from clickstream and bookings.",
)
@dp.expect_or_drop("user_id_not_null", "user_id IS NOT NULL")
def user_categories():
    catalog = spark.conf.get("bronze_catalog")
    schema = spark.conf.get("bronze_schema")
    high_intent_views = int(spark.conf.get("high_intent_views", "10"))

    def bronze(table):
        return spark.read.table(f"`{catalog}`.`{schema}`.`{table}`")

    users = bronze("users").select("user_id")

    clicks = (
        bronze("page_views")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("page_view_count"),
            F.countDistinct("property_id").alias("distinct_properties_viewed"),
        )
    )

    bookings = (
        bronze("bookings")
        .where(F.col("status") == "confirmed")
        .groupBy("user_id")
        .agg(
            F.count("*").alias("booking_count"),
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
        )
    )

    joined = (
        users.join(clicks, "user_id", "left")
        .join(bookings, "user_id", "left")
        .fillna(
            0,
            subset=[
                "page_view_count",
                "distinct_properties_viewed",
                "booking_count",
                "total_spent",
            ],
        )
    )

    category = (
        F.when(F.col("booking_count") >= 2, "booker")
        .when(F.col("booking_count") == 1, "new_booker")
        .when(F.col("page_view_count") >= high_intent_views, "high_intent")
        .when(F.col("page_view_count") > 0, "browser")
        .otherwise("inactive")
    )

    return joined.withColumn("user_category", category).withColumn(
        "views_per_booking",
        F.when(
            F.col("booking_count") > 0,
            F.round(F.col("page_view_count") / F.col("booking_count"), 2),
        ),
    )
