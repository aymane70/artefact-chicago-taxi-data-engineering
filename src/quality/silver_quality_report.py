
import argparse
import os

from pyspark.sql import functions as F

from transformations.bronze_to_silver import create_spark_session


SILVER_PATH = (
    f"s3a://{os.getenv('RUSTFS_BUCKET', 'chicago-taxi-lake')}"
    "/silver/taxi_trips"
)

REPORT_PATH = (
    f"s3a://{os.getenv('RUSTFS_BUCKET', 'chicago-taxi-lake')}"
    "/quality/silver/monthly_report"
)

QUALITY_FLAGS = [
    "has_invalid_numeric",
    "has_negative_values",
    "has_dst_ambiguous_duration",
    "has_negative_duration",
    "has_missing_duration_data",
    "has_invalid_duration",
    "has_incomplete_financial_data",
    "has_total_mismatch",
]


def generate_quality_report(
    silver_path=SILVER_PATH,
    report_path=REPORT_PATH,
):
    spark = create_spark_session()

    try:
        print("Starting Silver data-quality reporting.")
        print(f"Silver input: {silver_path}")

        silver_df = spark.read.parquet(silver_path)

        # Validate required columns
        required_columns = {
            "year",
            "month",
            "trip_id",
            *QUALITY_FLAGS,
        }

        missing_columns = required_columns - set(silver_df.columns)

        if missing_columns:
            raise ValueError(
                f"Missing required Silver columns: {sorted(missing_columns)}"
            )

        # Aggregate quality metrics by month
        aggregations = [
            F.count("*").alias("total_records"),
            F.countDistinct("trip_id").alias("unique_trip_ids"),
        ]

        for flag in QUALITY_FLAGS:
            aggregations.append(
                F.sum(
                    F.when(F.col(flag) == True, 1).otherwise(0)
                ).alias(f"{flag}_count")
            )

        report_df = silver_df.groupBy(
            "year", "month"
        ).agg(*aggregations)

        # Calculate percentages for every quality flag
        for flag in QUALITY_FLAGS:
            count_column = f"{flag}_count"

            report_df = report_df.withColumn(
                f"{flag}_percentage",
                F.round(
                    F.col(count_column) / F.col("total_records") * 100,
                    2,
                ),
            )

        report_df = report_df.withColumn(
            "duplicate_trip_ids",
            F.col("total_records") - F.col("unique_trip_ids"),
        )

        report_df = report_df.orderBy("year", "month")

        # Persist the report in RustFS
        print(f"Writing quality report to: {report_path}")

        report_df.write.mode("overwrite").parquet(report_path)

        # Read back the small persisted report for display
        saved_report = spark.read.parquet(report_path)

        print("\nMonthly Silver data-quality report:")
        saved_report.orderBy("year", "month").show(
            truncate=False
        )

        print("Quality report successfully generated.")

        return report_path

    finally:
        spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate monthly Silver data-quality reports."
    )

    parser.add_argument(
        "--silver-path",
        type=str,
        default=SILVER_PATH,
    )

    parser.add_argument(
        "--report-path",
        type=str,
        default=REPORT_PATH,
    )

    args = parser.parse_args()

    generate_quality_report(
        silver_path=args.silver_path,
        report_path=args.report_path,
    )
