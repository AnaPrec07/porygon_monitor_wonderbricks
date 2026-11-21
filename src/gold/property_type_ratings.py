"""Gold `property_type_ratings` dataset (Lakeflow Spark Declarative Pipeline).

Aggregates silver `average_rating` (top-rated properties) by property type:
how many top properties each type has, their mean rating and review volume.
Feeds the dashboard's property-quality charts.

Pipeline configuration keys:
  silver_catalog, silver_schema  where the silver tables live
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="property_type_ratings",
    comment="Rating and review volume of top-rated properties, by property type.",
)
@dp.expect_or_drop("property_type_not_null", "property_type IS NOT NULL")
def property_type_ratings():
    catalog = spark.conf.get("silver_catalog")
    schema = spark.conf.get("silver_schema")

    ratings = spark.read.table(f"`{catalog}`.`{schema}`.`average_rating`")

    return (
        ratings.groupBy("property_type")
        .agg(
            F.count("*").alias("top_property_count"),
            F.round(F.avg("avg_rating"), 2).alias("mean_rating"),
            F.max("avg_rating").alias("best_rating"),
            F.sum("review_count").alias("total_reviews"),
        )
        .orderBy(F.col("mean_rating").desc())
    )
