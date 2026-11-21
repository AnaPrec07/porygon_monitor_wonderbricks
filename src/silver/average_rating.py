"""Silver `average_rating` dataset (Lakeflow Spark Declarative Pipeline).

Joins bronze properties with their non-deleted reviews, keeps properties with at
least `min_reviews` reviews, and exposes the top `top_n` by average rating as a
materialized view.

Pipeline configuration keys:
  bronze_catalog, bronze_schema  where the bronze tables live
  min_reviews (default 5), top_n (default 10)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="average_rating",
    comment="Top properties by average guest rating (non-deleted reviews only).",
)
@dp.expect_or_drop("rating_not_null", "avg_rating IS NOT NULL")
def average_rating():
    catalog = spark.conf.get("bronze_catalog")
    schema = spark.conf.get("bronze_schema")
    min_reviews = int(spark.conf.get("min_reviews", "5"))
    top_n = int(spark.conf.get("top_n", "10"))

    properties = spark.read.table(f"`{catalog}`.`{schema}`.`properties`").alias("p")
    reviews = spark.read.table(f"`{catalog}`.`{schema}`.`reviews`").alias("r")

    return (
        properties.join(reviews, F.col("p.property_id") == F.col("r.property_id"))
        .where(~F.col("r.is_deleted"))
        .groupBy(F.col("p.title").alias("property_title"), F.col("p.property_type"))
        .agg(
            F.round(F.avg("r.rating"), 2).alias("avg_rating"),
            F.count("r.rating").alias("review_count"),
        )
        .where(F.col("review_count") >= min_reviews)
        .orderBy(F.col("avg_rating").desc())
        .limit(top_n)
    )
