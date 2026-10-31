# Technical Decisions — Chicago Taxi Data Pipeline

This document explains the main engineering decisions behind the Chicago Taxi Data Pipeline, including ingestion, storage, transformation, data quality and orchestration.

## 1. Data Source and Extraction Strategy

**Decision:** Use the City of Chicago Data Portal API as the source of taxi trip records.

The pipeline retrieves data through the Socrata API endpoint:

`https://data.cityofchicago.org/resource/wrvz-psew.json`

The selected reporting period covers the four complete months from September to December 2023.

### Pagination

The API is queried using offset-based pagination with:
- A page size of 10,000 records.
- `$limit` to control the number of records per request.
- `$offset` to retrieve successive batches.
- Ordering by `trip_start_timestamp` and `trip_id`.

**Rationale:**
- Avoids attempting to retrieve the entire dataset in a single request.
- Keeps memory consumption manageable.
- Supports processing large monthly datasets in smaller batches.
- Explicit ordering helps make pagination more deterministic.

The extraction is organized by month to simplify tracking, recovery and reconciliation.

## 2. Bronze Storage and Batch Management

**Decision:** Store raw API responses as JSONL objects in RustFS.

Each monthly extraction is divided into separate batch objects.

**Rationale:**
- JSONL supports record-by-record storage and processing.
- Raw records remain available for reprocessing.
- Monthly organization makes the ingestion process easier to monitor.
- Separate batches make it possible to identify and recover individual ingestion units.

### Batch Validation and Idempotency

Each batch is associated with metadata stored in a monthly ingestion manifest.

The ingestion process validates existing batch objects using their recorded metadata, including record counts and checksums.

If an existing batch passes validation, it can be skipped instead of downloaded and written again. Invalid or incomplete batches can be reprocessed.

**Rationale:**
- Prevents unnecessary duplicate ingestion.
- Supports recovery after interrupted extraction.
- Provides a way to verify that stored batches match their expected content.
- Makes ingestion more reliable across repeated executions.

## 3. Retry and Failure Handling

**Decision:** Handle transient API failures through retries rather than immediately failing the entire extraction.

API requests can experience temporary network issues or read timeouts, particularly when retrieving large datasets.

Retry handling allows the extraction process to recover from temporary failures.

**Rationale:**
- Reduces failures caused by temporary connectivity problems.
- Avoids restarting an entire monthly extraction because of one failed request.
- Improves the reliability of long-running ingestion operations.

Retries are intended for transient failures. Persistent errors should remain visible rather than being silently ignored.

## 4. Storage Technology

**Decision:** Use RustFS as the persistent object storage layer.

RustFS provides an S3-compatible interface, allowing the pipeline to interact with object storage through standard S3 clients and Hadoop's S3A connector.

**Rationale:**
- Provides a consistent storage interface across ingestion, Spark and dashboard components.
- Supports Parquet and JSONL objects.
- Allows local development without depending on a cloud storage account.
- Integrates with Python S3 clients and Spark.

The persistent datasets are stored in RustFS, while temporary Spark processing files remain within the execution environment.

## 5. Medallion Architecture

**Decision:** Separate data into Bronze, Silver and Gold layers.

| Layer | Responsibility | Format |
|---|---|---|
| Bronze | Preserve raw API records. | JSONL |
| Silver | Clean, cast and validate trip records. | Parquet |
| Gold | Provide aggregated datasets for analytics. | Parquet |

**Rationale:**
- Separates ingestion from transformation and consumption.
- Preserves raw data for reproducibility.
- Makes data-quality checks explicit.
- Provides optimized analytical datasets for downstream consumers.

A separate Quarantine location stores records that fail critical structural validation. The Quality layer stores aggregated validation metrics.

## 6. Transformation and Processing

**Decision:** Use PySpark for Bronze-to-Silver and Silver-to-Gold transformations.

**Rationale:**
- Provides distributed data processing capabilities.
- Supports structured transformations using DataFrames.
- Offers native support for Parquet, partitioning and aggregations.
- Separates transformation logic from API extraction.

### Type Enforcement

Source values are initially read using an explicit schema. Numeric and timestamp fields are converted to their expected types.

Invalid conversions are handled through tolerant casting so that individual malformed values do not automatically fail the entire transformation.

Original numeric source values are retained in diagnostic columns for quarantined records.

### Partitioning

Silver datasets are partitioned by year and month.

**Rationale:**
- Organizes data around the reporting period.
- Supports month-level filtering and processing.
- Makes the storage layout easier to inspect and manage.

## 7. Data Quality and Quarantine Policy

**Decision:** Distinguish between critical structural failures and non-critical data-quality issues.

### Critical failures

A record is quarantined when:
- `trip_id` is missing.
- `trip_start_timestamp` is missing or cannot be converted into a valid timestamp.

These fields are required to identify a trip and associate it with a reporting date.

### Non-critical issues

Other issues, such as negative values, missing duration information, incomplete financial data or total mismatches, are represented through quality flags.

These records remain in Silver.

**Rationale:**
- Avoids discarding potentially useful records unnecessarily.
- Keeps data-quality issues visible to downstream consumers.
- Separates structural validity from analytical eligibility.
- Allows different Gold datasets to apply their own business rules.

### Financial Reconciliation

The pipeline compares the reported trip total with the sum of fare, tips, tolls and extras.

A tolerance of $0.02 is applied to account for small rounding differences.

The original financial values are preserved rather than silently corrected.

### Duration and Daylight-Saving Time

The pipeline identifies potentially ambiguous negative durations around Chicago's November 5, 2023 daylight-saving transition.

The implemented rule is a targeted heuristic based on the repeated 1 AM hour and the ordering of timestamps. It is not a general timezone reconstruction mechanism.

## 8. Gold Dataset Design

**Decision:** Produce three separate analytical datasets rather than one large aggregated table.

| Dataset | Analytical purpose |
|---|---|
| `daily_revenue` | Daily trip volume and financial performance. |
| `daily_trip_metrics` | Daily duration and distance metrics. |
| `pickup_zone_performance` | Monthly performance by pickup community area. |

**Rationale:**
- Gives each dataset a clear analytical purpose.
- Keeps business rules specific to each metric.
- Avoids mixing different eligibility criteria.
- Simplifies dashboard queries and visualizations.

Duration and distance eligibility are evaluated independently. Consequently, their valid trip counts can differ.

Revenue metrics use reported financial amounts and do not automatically exclude every record with a quality flag.

## 9. Orchestration

**Decision:** Use Apache Airflow to orchestrate the pipeline.

The workflow is divided into four sequential tasks:

1. `ingest_data`
2. `bronze_to_silver`
3. `silver_quality_report`
4. `silver_to_gold`

**Rationale:**
- Makes dependencies explicit.
- Provides task-level execution status and logs.
- Allows individual stages to be monitored.
- Separates pipeline execution from dashboard consumption.

The DAG is manually triggered with `schedule=None`, making execution controlled and suitable for this project scope.

## 10. Dashboard and Consumption

**Decision:** Use Streamlit to present the Gold and Quality datasets.

The dashboard reads persisted Parquet data directly from RustFS through its S3-compatible interface.

**Rationale:**
- Provides a lightweight interface for exploring analytical results.
- Avoids coupling dashboard requests to Spark transformations.
- Reuses the same persisted datasets across dashboard sessions.
- Makes data-quality metrics accessible alongside business metrics.

## 11. Scope and Limitations

The current implementation is designed as a reproducible local data engineering project.

The following considerations define its current scope:

- The pipeline processes four selected months from 2023 rather than the full historical dataset.
- Airflow execution is manually triggered.
- RustFS runs locally and is not configured as a production-grade distributed storage deployment.
- The DST handling is specific to the selected reporting period.
- The dashboard consumes precomputed datasets rather than performing transformations.
- Authentication and service configuration are intended for local demonstration, not production deployment.

These choices keep the solution focused on reliable ingestion, transformation, validation, orchestration and analytical consumption.