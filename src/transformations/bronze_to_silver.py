
import argparse
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from transformations.schemas import RAW_TAXI_TRIPS_SCHEMA


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RUSTFS_BUCKET = os.getenv(
    "RUSTFS_BUCKET",
    "chicago-taxi-lake"
)

BRONZE_PATH = (
    f"s3a://{RUSTFS_BUCKET}/bronze/year=2023"
)

SILVER_PATH = (
    f"s3a://{RUSTFS_BUCKET}/silver/taxi_trips"
)

QUARANTINE_PATH = (
    f"s3a://{RUSTFS_BUCKET}/quarantine/taxi_trips"
)


# --------------------------------------------------
# Spark session with RustFS / S3A configuration
# --------------------------------------------------

def create_spark_session():

    required_env_vars = [
        "RUSTFS_ENDPOINT_URL",
        "RUSTFS_ACCESS_KEY",
        "RUSTFS_SECRET_KEY",
    ]

    missing_vars = [
        var for var in required_env_vars
        if not os.getenv(var)
    ]

    if missing_vars:
        raise EnvironmentError(
            f"Missing environment variables: {missing_vars}"
        )

    return (
        SparkSession.builder
        .appName("ChicagoTaxi-BronzeToSilver")
        .master("local[2]")

        # Memory and execution configuration
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.files.maxPartitionBytes",
            str(32 * 1024 * 1024)
        )

        # Hadoop AWS connector
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0"
        )

        # RustFS connection
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.environ["RUSTFS_ENDPOINT_URL"]
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.environ["RUSTFS_ACCESS_KEY"]
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.environ["RUSTFS_SECRET_KEY"]
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )

        .getOrCreate()
    )


# --------------------------------------------------
# 1. Read Bronze data from RustFS
# --------------------------------------------------

def read_bronze(spark, bronze_path):

    return (
        spark.read
        .schema(RAW_TAXI_TRIPS_SCHEMA)
        .json(f"{bronze_path}/month=*/batch_*.jsonl")
        .withColumn(
            "month",
            F.regexp_extract(
                F.input_file_name(),
                r"month=(\d{2})",
                1
            )
        )
        .withColumn("year", F.lit(2023))
    )


# --------------------------------------------------
# 2. Clean string columns
# --------------------------------------------------

def clean_string_columns(df):

    string_columns = [
        "trip_id",
        "taxi_id",
        "trip_start_timestamp",
        "trip_end_timestamp",
        "trip_seconds",
        "trip_miles",
        "pickup_census_tract",
        "dropoff_census_tract",
        "pickup_community_area",
        "dropoff_community_area",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
        "payment_type",
        "company",
        "pickup_centroid_latitude",
        "pickup_centroid_longitude",
        "dropoff_centroid_latitude",
        "dropoff_centroid_longitude",
    ]

    for column in string_columns:

        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column))
            )
        )

    return df


# --------------------------------------------------
# 3. Cast columns to appropriate data types
# --------------------------------------------------

def cast_columns(df):

    cast_definitions = {
        "trip_start_timestamp": "TIMESTAMP",
        "trip_end_timestamp": "TIMESTAMP",
        "trip_seconds": "INT",
        "trip_miles": "DOUBLE",
        "pickup_community_area": "INT",
        "dropoff_community_area": "INT",
        "fare": "DECIMAL(12,2)",
        "tips": "DECIMAL(12,2)",
        "tolls": "DECIMAL(12,2)",
        "extras": "DECIMAL(12,2)",
        "trip_total": "DECIMAL(12,2)",
        "pickup_centroid_latitude": "DOUBLE",
        "pickup_centroid_longitude": "DOUBLE",
        "dropoff_centroid_latitude": "DOUBLE",
        "dropoff_centroid_longitude": "DOUBLE",
    }

    for column, target_type in cast_definitions.items():

        df = df.withColumn(
            column,
            F.expr(
                f"try_cast(`{column}` AS {target_type})"
            )
        )

    return df


# --------------------------------------------------
# 4. Data quality rules
# --------------------------------------------------

def apply_data_quality_rules(df):

    # ----------------------------------------------
    # 4.1 Critical validation
    # ----------------------------------------------

    invalid_records = (
        F.col("trip_id").isNull()
        | F.col("trip_start_timestamp").isNull()
    )

    df = df.withColumn(
        "dq_reason",
        F.concat_ws(
            " | ",
            F.when(
                F.col("trip_id").isNull(),
                F.lit("MISSING_TRIP_ID")
            ),
            F.when(
                F.col("trip_start_timestamp").isNull(),
                F.lit("INVALID_START_TIMESTAMP")
            )
        )
    )

    # ----------------------------------------------
    # 4.2 Numeric conversion failures
    # ----------------------------------------------

    numeric_columns = [
        "trip_seconds",
        "trip_miles",
        "pickup_community_area",
        "dropoff_community_area",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
        "pickup_centroid_latitude",
        "pickup_centroid_longitude",
        "dropoff_centroid_latitude",
        "dropoff_centroid_longitude",
    ]

    invalid_numeric_conditions = [
        F.col(column).isNull()
        & F.col(f"raw_{column}").isNotNull()
        & (F.trim(F.col(f"raw_{column}")) != "")
        for column in numeric_columns
    ]

    df = df.withColumn(
        "has_invalid_numeric",
        F.coalesce(
            F.array_contains(
                F.array(*[
                    condition.cast("boolean")
                    for condition in invalid_numeric_conditions
                ]),
                True
            ),
            F.lit(False)
        )
    )

    # ----------------------------------------------
    # 4.3 Negative values
    # ----------------------------------------------

    negative_condition = (
        (F.col("trip_seconds") < 0)
        | (F.col("trip_miles") < 0)
        | (F.col("fare") < 0)
        | (F.col("tips") < 0)
        | (F.col("tolls") < 0)
        | (F.col("extras") < 0)
        | (F.col("trip_total") < 0)
    )

    df = df.withColumn(
        "has_negative_values",
        F.coalesce(
            negative_condition,
            F.lit(False)
        )
    )

    # ----------------------------------------------
    # 4.4 Duration validation
    # ----------------------------------------------

    # November 5, 2023: Chicago daylight-saving transition.
    # The local 1 AM hour occurred twice.

    dst_ambiguous_condition = (
        (F.to_date("trip_start_timestamp") == F.lit("2023-11-05"))
        & (F.hour("trip_start_timestamp") == 1)
        & (F.hour("trip_end_timestamp") == 1)
        & (
            F.col("trip_end_timestamp")
            < F.col("trip_start_timestamp")
        )
    )

    invalid_duration_condition = (
        F.col("trip_end_timestamp").isNotNull()
        & F.col("trip_start_timestamp").isNotNull()
        & (
            F.col("trip_end_timestamp")
            < F.col("trip_start_timestamp")
        )
    )

    df = (
        df
        .withColumn(
            "has_dst_ambiguous_duration",
            F.coalesce(
                dst_ambiguous_condition,
                F.lit(False)
            )
        )
        .withColumn(
            "has_negative_duration",
            F.coalesce(
                invalid_duration_condition
                & ~dst_ambiguous_condition,
                F.lit(False)
            )
        )
        .withColumn(
            "has_missing_duration_data",
            F.col("trip_end_timestamp").isNull()
            | F.col("trip_seconds").isNull()
        )
        .withColumn(
            "has_invalid_duration",
            F.col("has_negative_duration")
        )
    )

    # ----------------------------------------------
    # 4.5 Financial completeness and reconciliation
    # ----------------------------------------------

    financial_columns = [
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
    ]

    complete_financial_condition = F.lit(True)

    for column in financial_columns:

        complete_financial_condition = (
            complete_financial_condition
            & F.col(column).isNotNull()
        )

    df = df.withColumn(
        "has_incomplete_financial_data",
        ~complete_financial_condition
    )

    expected_total = (
        F.col("fare")
        + F.col("tips")
        + F.col("tolls")
        + F.col("extras")
    )

    # Positive difference means reported total is higher.

    df = df.withColumn(
        "financial_difference",
        F.when(
            complete_financial_condition,
            F.round(
                F.col("trip_total") - expected_total,
                2
            )
        )
    )

    df = df.withColumn(
        "has_total_mismatch",
        F.coalesce(
            complete_financial_condition
            & (
                F.abs(F.col("financial_difference")) > 0.02
            ),
            F.lit(False)
        )
    )

    return df, invalid_records


# --------------------------------------------------
# 5. Bronze -> Silver transformation
# --------------------------------------------------

def transform_bronze_to_silver(
    bronze_path=BRONZE_PATH,
    silver_path=SILVER_PATH,
    quarantine_path=QUARANTINE_PATH,
):

    spark = create_spark_session()

    transformed_df = None

    try:

        print("Starting Bronze-to-Silver transformation.")
        print(f"Bronze input: {bronze_path}")

        print(
            "Spark master:",
            spark.sparkContext.master
        )

        print(
            "Spark driver memory:",
            spark.sparkContext.getConf().get(
                "spark.driver.memory",
                "Not explicitly configured"
            )
        )

        # ------------------------------------------
        # Read Bronze
        # ------------------------------------------

        raw_df = read_bronze(
            spark,
            bronze_path
        )

        # ------------------------------------------
        # Preserve raw values for conversion checks
        # ------------------------------------------

        numeric_columns = [
            "trip_seconds",
            "trip_miles",
            "pickup_community_area",
            "dropoff_community_area",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total",
            "pickup_centroid_latitude",
            "pickup_centroid_longitude",
            "dropoff_centroid_latitude",
            "dropoff_centroid_longitude",
        ]

        for column in numeric_columns:

            raw_df = raw_df.withColumnRenamed(
                column,
                f"raw_{column}"
            )

        # Recreate original column names for casting
        for column in numeric_columns:

            raw_df = raw_df.withColumn(
                column,
                F.col(f"raw_{column}")
            )

        # ------------------------------------------
        # Clean and cast
        # ------------------------------------------

        raw_df = clean_string_columns(raw_df)

        transformed_df = cast_columns(raw_df)

        # ------------------------------------------
        # Apply data quality rules
        # ------------------------------------------

        transformed_df, invalid_condition = (
            apply_data_quality_rules(transformed_df)
        )

        # Persist transformed data on disk.
        # This avoids parsing the Bronze JSON source again
        # when writing Silver and Quarantine separately.

        transformed_df = transformed_df.persist(
            StorageLevel.DISK_ONLY
        )

        # ------------------------------------------
        # Separate Silver and Quarantine
        # ------------------------------------------

        quarantine_df = transformed_df.filter(
            invalid_condition
        )

        silver_df = transformed_df.filter(
            ~invalid_condition
        )

        # ------------------------------------------
        # Remove temporary columns from Silver
        # ------------------------------------------

        raw_columns = [
            f"raw_{column}"
            for column in numeric_columns
        ]

        silver_df = silver_df.drop(
            "dq_reason",
            *raw_columns
        )

        # ------------------------------------------
        # Write Silver to RustFS
        # ------------------------------------------

        print(f"Writing Silver data to: {silver_path}")

        (
            silver_df
            .coalesce(8)
            .write
            .mode("overwrite")
            .option("maxRecordsPerFile", 100000)
            .partitionBy("year", "month")
            .parquet(silver_path)
        )

        print("Silver write completed.")

        # ------------------------------------------
        # Write Quarantine to RustFS
        # ------------------------------------------

        print(f"Writing Quarantine data to: {quarantine_path}")

        (
            quarantine_df
            .coalesce(8)
            .write
            .mode("overwrite")
            .option("maxRecordsPerFile", 100000)
            .partitionBy("year", "month")
            .parquet(quarantine_path)
        )

        print("Quarantine write completed.")

        # ------------------------------------------
        # Completion
        # ------------------------------------------

        print("\nBronze-to-Silver transformation completed.")
        print(f"Silver output: {silver_path}")
        print(f"Quarantine output: {quarantine_path}")

        print(
            "Cached transformed data was released after processing."
        )

    finally:

        # ------------------------------------------
        # Release cached data
        # ------------------------------------------

        if transformed_df is not None:

            try:
                transformed_df.unpersist(blocking=True)

            except Exception as cache_error:
                print(
                    f"Warning: could not release Spark cache: "
                    f"{cache_error}"
                )

        # ------------------------------------------
        # Stop Spark
        # ------------------------------------------

        try:
            spark.stop()

        except Exception as stop_error:
            print(
                f"Warning: Spark shutdown encountered an error: "
                f"{stop_error}"
            )


# --------------------------------------------------
# 6. Command-line interface
# --------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Transform Chicago Taxi Bronze data into Silver "
            "using PySpark and RustFS."
        )
    )

    parser.add_argument(
        "--bronze-path",
        type=str,
        default=BRONZE_PATH
    )

    parser.add_argument(
        "--silver-path",
        type=str,
        default=SILVER_PATH
    )

    parser.add_argument(
        "--quarantine-path",
        type=str,
        default=QUARANTINE_PATH
    )

    args = parser.parse_args()

    transform_bronze_to_silver(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        quarantine_path=args.quarantine_path,
    )
