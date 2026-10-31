
import argparse
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from transformations.bronze_to_silver import create_spark_session


BUCKET = os.getenv("RUSTFS_BUCKET", "chicago-taxi-lake")

SILVER_PATH = f"s3a://{BUCKET}/silver/taxi_trips"
GOLD_BASE_PATH = f"s3a://{BUCKET}/gold"


def build_daily_revenue(silver_df):
    eligible = silver_df.filter(
        F.col("trip_start_timestamp").isNotNull()
    )

    return (
        eligible
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .groupBy("year", "month", "trip_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.count("trip_total").alias("revenue_eligible_trips"),
            F.sum("trip_total").alias("total_revenue"),
            F.avg("trip_total").alias("average_trip_revenue"),
            F.sum("fare").alias("total_fares"),
            F.sum("tips").alias("total_tips"),
        )
        .orderBy("trip_date")
    )


def build_daily_trip_metrics(silver_df):
    eligible = silver_df.filter(
        F.col("trip_start_timestamp").isNotNull()
    )

    duration_valid = eligible.filter(
        (F.col("has_invalid_duration") == False)
        & (F.col("has_dst_ambiguous_duration") == False)
        & (F.col("has_missing_duration_data") == False)
        & (F.col("has_negative_duration") == False)
        & F.col("trip_seconds").isNotNull()
        & (F.col("trip_seconds") > 0)
    )

    distance_valid = eligible.filter(
        F.col("trip_miles").isNotNull()
        & (F.col("trip_miles") >= 0)
    )

    duration_metrics = (
        duration_valid
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .groupBy("year", "month", "trip_date")
        .agg(
            F.count("*").alias("valid_duration_trips"),
            F.round(F.avg("trip_seconds") / 60, 2)
            .alias("average_trip_duration_minutes"),
        )
    )

    distance_metrics = (
        distance_valid
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .groupBy("year", "month", "trip_date")
        .agg(
            F.count("*").alias("valid_distance_trips"),
            F.round(F.avg("trip_miles"), 2)
            .alias("average_trip_distance_miles"),
            F.round(F.sum("trip_miles"), 2)
            .alias("total_distance_miles"),
        )
    )

    return (
        duration_metrics
        .join(
            distance_metrics,
            on=["year", "month", "trip_date"],
            how="full",
        )
        .orderBy("trip_date")
    )


def build_pickup_zone_performance(silver_df):
    eligible = silver_df.filter(
        F.col("pickup_community_area").isNotNull()
        & F.col("trip_start_timestamp").isNotNull()
    )

    monthly_zones = (
        eligible
        .groupBy("year", "month", "pickup_community_area")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("trip_total").alias("total_revenue"),
            F.round(
                F.avg(
                    F.when(
                        F.col("trip_miles") >= 0,
                        F.col("trip_miles"),
                    )
                ),
                2,
            ).alias("average_trip_distance_miles"),
        )
    )

    ranking_window = (
        Window
        .partitionBy("year", "month")
        .orderBy(F.col("total_trips").desc())
    )

    return (
        monthly_zones
        .withColumn(
            "monthly_rank",
            F.dense_rank().over(ranking_window),
        )
        .orderBy("year", "month", "monthly_rank")
    )


def transform_silver_to_gold(
    silver_path=SILVER_PATH,
    gold_base_path=GOLD_BASE_PATH,
):
    spark = create_spark_session()

    try:
        print("Starting Silver-to-Gold transformation.")
        print(f"Silver input: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        outputs = {
            "daily_revenue": build_daily_revenue(silver_df),
            "daily_trip_metrics": build_daily_trip_metrics(silver_df),
            "pickup_zone_performance": build_pickup_zone_performance(silver_df),
        }

        for table_name, dataframe in outputs.items():
            output_path = f"{gold_base_path}/{table_name}"

            print(f"Writing {table_name} to {output_path}")

            dataframe.write.mode("overwrite").parquet(output_path)

            saved_df = spark.read.parquet(output_path)

            print(
                f"{table_name}: "
                f"{saved_df.count()} records persisted"
            )

            saved_df.show(5, truncate=False)

        print("Silver-to-Gold transformation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform Silver taxi data into Gold analytics tables."
    )

    parser.add_argument("--silver-path", default=SILVER_PATH)
    parser.add_argument("--gold-base-path", default=GOLD_BASE_PATH)

    args = parser.parse_args()

    transform_silver_to_gold(
        silver_path=args.silver_path,
        gold_base_path=args.gold_base_path,
    )
