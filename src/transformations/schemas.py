


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
    TimestampType,
    ArrayType,
)


# GeoJSON Point structure
GEO_POINT_SCHEMA = StructType([
    StructField("type", StringType(), nullable=True),
    StructField(
        "coordinates",
        ArrayType(DoubleType()),
        nullable=True
    ),
])


# Raw Bronze schema: preserve source values before casting
RAW_TAXI_TRIPS_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("taxi_id", StringType(), True),

    StructField("trip_start_timestamp", StringType(), True),
    StructField("trip_end_timestamp", StringType(), True),

    StructField("trip_seconds", StringType(), True),
    StructField("trip_miles", StringType(), True),

    StructField("pickup_census_tract", StringType(), True),
    StructField("dropoff_census_tract", StringType(), True),
    StructField("pickup_community_area", StringType(), True),
    StructField("dropoff_community_area", StringType(), True),

    StructField("fare", StringType(), True),
    StructField("tips", StringType(), True),
    StructField("tolls", StringType(), True),
    StructField("extras", StringType(), True),
    StructField("trip_total", StringType(), True),

    StructField("payment_type", StringType(), True),
    StructField("company", StringType(), True),

    StructField("pickup_centroid_latitude", StringType(), True),
    StructField("pickup_centroid_longitude", StringType(), True),
    StructField("pickup_centroid_location", GEO_POINT_SCHEMA, True),

    StructField("dropoff_centroid_latitude", StringType(), True),
    StructField("dropoff_centroid_longitude", StringType(), True),
    StructField("dropoff_centroid_location", GEO_POINT_SCHEMA, True),
])
