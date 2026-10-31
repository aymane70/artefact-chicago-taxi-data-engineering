# Data Dictionary — Chicago Taxi Data Pipeline

This document describes the datasets produced by the Chicago Taxi Data Engineering Pipeline, including the Bronze, Silver, Quality and Gold layers.

## 1. Bronze Layer

**Storage format:** JSONL  
**Location:** `s3://chicago-taxi-lake/bronze/year=2023/`

The Bronze layer stores raw records extracted from the City of Chicago Taxi Trips API.

Data is organized by month, with each ingestion batch stored as a separate JSONL object.

```text
bronze/
└── year=2023/
    ├── month=09/
    │   ├── batch_00000.jsonl
    │   ├── batch_00001.jsonl
    │   └── _manifest.json
    ├── month=10/
    ├── month=11/
    └── month=12/
```

Each JSONL record represents one taxi trip and retains the source fields. The source includes trip identifiers, timestamps, payment information, trip duration, distance, financial amounts and pickup/drop-off locations.

The ingestion manifests record batch-level metadata used to support ingestion tracking and recovery.

## 2. Silver Layer

**Storage format:** Parquet  
**Location:** `s3://chicago-taxi-lake/silver/taxi_trips/`

The Silver layer contains transformed taxi trip records with enforced data types, cleaned string values and data-quality indicators.

### Schema

| Field | Data type | Description |
|---|---|---|
| `trip_id` | String | Unique taxi trip identifier. |
| `taxi_id` | String | Taxi identifier. |
| `trip_start_timestamp` | Timestamp | Trip start timestamp. |
| `trip_end_timestamp` | Timestamp | Trip end timestamp. |
| `trip_seconds` | Integer | Trip duration in seconds. |
| `trip_miles` | Double | Trip distance in miles. |
| `pickup_census_tract` | String | Pickup census tract. |
| `dropoff_census_tract` | String | Drop-off census tract. |
| `pickup_community_area` | Integer | Pickup community area identifier. |
| `dropoff_community_area` | Integer | Drop-off community area identifier. |
| `fare` | Decimal(12,2) | Fare amount. |
| `tips` | Decimal(12,2) | Tip amount. |
| `tolls` | Decimal(12,2) | Toll amount. |
| `extras` | Decimal(12,2) | Additional charges. |
| `trip_total` | Decimal(12,2) | Total trip amount reported by the source. |
| `payment_type` | String | Payment method. |
| `company` | String | Taxi operating company. |
| `pickup_centroid_latitude` | Double | Pickup latitude. |
| `pickup_centroid_longitude` | Double | Pickup longitude. |
| `pickup_centroid_location` | Struct | Source GeoJSON pickup location containing `type` and `coordinates`. |
| `dropoff_centroid_latitude` | Double | Drop-off latitude. |
| `dropoff_centroid_longitude` | Double | Drop-off longitude. |
| `dropoff_centroid_location` | Struct | Source GeoJSON drop-off location containing `type` and `coordinates`. |
| `year` | Integer | Partition year. |
| `month` | String | Partition month, extracted from the Bronze object path. |

### Data-quality indicators

These Boolean fields identify records affected by the implemented validation rules.

| Field | Description |
|---|---|
| `has_invalid_numeric` | At least one numeric field contains a non-empty source value that could not be converted to its expected numeric type. |
| `has_negative_values` | At least one checked duration, distance or financial amount is negative. |
| `has_dst_ambiguous_duration` | A negative timestamp interval matches the November 5, 2023 Chicago daylight-saving transition heuristic: both timestamps fall within the repeated 1 AM hour. |
| `has_negative_duration` | The trip end timestamp precedes the start timestamp, excluding intervals flagged as DST-ambiguous. |
| `has_missing_duration_data` | The end timestamp or duration in seconds is missing. |
| `has_invalid_duration` | The trip has a negative duration. |
| `has_incomplete_financial_data` | At least one of fare, tips, tolls, extras or trip total is missing. |
| `financial_difference` | Reported trip total minus the sum of fare, tips, tolls and extras. |
| `has_total_mismatch` | The absolute financial difference exceeds $0.02 for a financially complete record. |

### Validation and transformation rules

- Empty strings are converted to null values in cleaned string columns.
- Numeric and timestamp fields are converted to their expected data types.
- `try_cast` is used for numeric and timestamp conversions to avoid failing the entire transformation on individual conversion errors.
- Original source financial amounts are preserved; the pipeline does not silently adjust reported totals.
- Financial reconciliation uses a tolerance of $0.02.
- The DST rule specifically handles potentially ambiguous negative intervals during Chicago's November 5, 2023 clock transition.

### Quarantine policy

Records are quarantined when either of these critical conditions is met:

- `trip_id` is null.
- `trip_start_timestamp` is null after timestamp conversion.

**Quarantine location:** `s3://chicago-taxi-lake/quarantine/taxi_trips/`

Quarantined records retain diagnostic information, including:
- `dq_reason`, identifying the critical validation failure.
- Raw numeric source columns prefixed with `raw_`.
- Converted fields and other quality indicators.

Other data-quality issues do not automatically remove records from Silver. They remain available for downstream analysis with their corresponding flags.

The current implementation produced zero quarantined records across the four selected months.
## 3. Quality Layer

**Storage format:** Parquet  
**Location:** `s3://chicago-taxi-lake/quality/silver/monthly_report/`

The Quality layer contains monthly data-quality metrics calculated from the Silver dataset. Records are grouped by `year` and `month`.

### Report schema

| Field | Description |
|---|---|
| `year` | Reporting year. |
| `month` | Reporting month. |
| `total_records` | Total number of Silver records in the month. |
| `unique_trip_ids` | Number of distinct trip identifiers. |
| `duplicate_trip_ids` | Difference between total records and unique trip identifiers. |

### Quality metrics

Each quality flag produces two metrics: a count of flagged records and the percentage of monthly records affected.

| Quality flag | Count field | Percentage field |
|---|---|---|
| Invalid numeric values | `has_invalid_numeric_count` | `has_invalid_numeric_percentage` |
| Negative values | `has_negative_values_count` | `has_negative_values_percentage` |
| DST-ambiguous duration | `has_dst_ambiguous_duration_count` | `has_dst_ambiguous_duration_percentage` |
| Negative duration | `has_negative_duration_count` | `has_negative_duration_percentage` |
| Missing duration data | `has_missing_duration_data_count` | `has_missing_duration_data_percentage` |
| Invalid duration | `has_invalid_duration_count` | `has_invalid_duration_percentage` |
| Incomplete financial data | `has_incomplete_financial_data_count` | `has_incomplete_financial_data_percentage` |
| Total mismatch | `has_total_mismatch_count` | `has_total_mismatch_percentage` |

Percentages are calculated as:

`flagged records / total records × 100`

Results are rounded to two decimal places.

The report is overwritten whenever the quality-report task runs, ensuring that it reflects the latest Silver dataset.
## 4. Gold Layer

**Storage format:** Parquet  
**Base location:** `s3://chicago-taxi-lake/gold/`

The Gold layer contains three analytical datasets derived from Silver. They are designed for reporting and dashboard consumption.

### 4.1 Daily Revenue

**Location:** `s3://chicago-taxi-lake/gold/daily_revenue/`

**Granularity:** One record per trip date.

| Field | Description |
|---|---|
| `year` | Reporting year. |
| `month` | Reporting month. |
| `trip_date` | Date extracted from the trip start timestamp. |
| `total_trips` | Number of trips with a valid start timestamp. |
| `revenue_eligible_trips` | Number of trips with a non-null `trip_total`. |
| `total_revenue` | Sum of reported trip totals. |
| `average_trip_revenue` | Average reported trip total across revenue-eligible trips. |
| `total_fares` | Sum of fare amounts. |
| `total_tips` | Sum of tip amounts. |

**Business rules:**
- Trips without a start timestamp are excluded.
- Revenue eligibility requires a non-null `trip_total`.
- Financial amounts are aggregated as reported in Silver.

### 4.2 Daily Trip Metrics

**Location:** `s3://chicago-taxi-lake/gold/daily_trip_metrics/`

**Granularity:** One record per trip date.

| Field | Description |
|---|---|
| `year` | Reporting year. |
| `month` | Reporting month. |
| `trip_date` | Date extracted from the trip start timestamp. |
| `valid_duration_trips` | Number of trips included in duration calculations. |
| `average_trip_duration_minutes` | Average valid trip duration in minutes, rounded to two decimals. |
| `valid_distance_trips` | Number of trips included in distance calculations. |
| `average_trip_distance_miles` | Average valid trip distance in miles, rounded to two decimals. |
| `total_distance_miles` | Sum of valid trip distances in miles, rounded to two decimals. |

**Duration eligibility:**
- Start timestamp must be present.
- Duration must be present and greater than zero.
- Records flagged for invalid duration, negative duration, missing duration or DST ambiguity are excluded.

**Distance eligibility:**
- Start timestamp must be present.
- `trip_miles` must be non-null and greater than or equal to zero.
- Distance eligibility is evaluated independently of duration eligibility.

Duration and distance metrics are aggregated separately and combined using a full outer join. Therefore, their respective counts can differ.

### 4.3 Pickup Zone Performance

**Location:** `s3://chicago-taxi-lake/gold/pickup_zone_performance/`

**Granularity:** One record per pickup community area and month.

| Field | Description |
|---|---|
| `year` | Reporting year. |
| `month` | Reporting month. |
| `pickup_community_area` | Pickup community area identifier. |
| `total_trips` | Number of eligible trips originating from the area. |
| `total_revenue` | Sum of reported trip totals for the area. |
| `average_trip_distance_miles` | Average non-negative trip distance in miles, rounded to two decimals. |
| `monthly_rank` | Dense ranking of pickup areas by trip volume within each month. |

**Business rules:**
- Pickup community area and trip start timestamp must be present.
- Ranking is calculated separately for each year and month.
- Areas are ranked by descending trip volume.
- Equal trip volumes receive the same rank.

## 5. Dataset Summary

| Layer | Format | Purpose |
|---|---|---|
| Bronze | JSONL | Raw API data and ingestion manifests. |
| Silver | Parquet | Validated and transformed trip records. |
| Quarantine | Parquet | Records failing critical structural validation. |
| Quality | Parquet | Monthly data-quality metrics. |
| Gold | Parquet | Aggregated analytical datasets. |

**Note:** Field descriptions reflect the current implementation. The Chicago Data Portal remains the authority for original source-field definitions.
