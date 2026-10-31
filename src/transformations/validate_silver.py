
from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "/opt/airflow/data/processed/taxi_trips/silver"

spark = (
    SparkSession.builder
    .appName("validate-silver")
    .getOrCreate()
)

try:
    df = spark.read.parquet(SILVER_PATH)

    print("\n=== OVERALL VALIDATION ===")

    metrics = df.agg(
        F.count("*").alias("total_records"),
        F.countDistinct("trip_id").alias("unique_trip_ids"),
        F.sum(F.col("trip_id").isNull().cast("int")).alias("missing_trip_ids"),
        F.sum(F.col("has_invalid_numeric").cast("int")).alias("invalid_numeric"),
        F.sum(F.col("has_negative_values").cast("int")).alias("negative_values"),
        F.sum(F.col("has_invalid_duration").cast("int")).alias("invalid_durations"),
        F.sum(F.col("has_incomplete_financial_data").cast("int")).alias("incomplete_financial"),
        F.sum(F.col("has_total_mismatch").cast("int")).alias("financial_mismatches"),
    ).first()

    metrics_dict = metrics.asDict()

    for name, value in metrics_dict.items():
        print(f"{name}: {value:,}" if value is not None else f"{name}: 0")

    duplicate_count = (
        metrics_dict["total_records"] - metrics_dict["unique_trip_ids"]
    )
    print(f"duplicate_trip_ids: {duplicate_count:,}")

    print("\n=== MONTHLY RECORD COUNTS ===")

    (
        df.groupBy("year", "month")
        .count()
        .orderBy("year", "month")
        .show()
    )

    print("\n=== SAMPLE FINANCIAL MISMATCHES ===")

    (
        df.filter(F.col("has_total_mismatch"))
        .select(
            "trip_id",
            "payment_type",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total",
            "financial_difference",
        )
        .show(10, truncate=False)
    )

finally:
    spark.stop()
