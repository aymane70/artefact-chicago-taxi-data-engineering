# Architecture — Chicago Taxi Data Pipeline

## 1. Overview

The Chicago Taxi Data Pipeline is an end-to-end data engineering solution that extracts taxi trip records from the City of Chicago Data Portal, stores raw data in an object storage layer, transforms it into analytical datasets and exposes key metrics through an interactive dashboard.

The pipeline follows a Medallion Architecture, separating data into Bronze, Silver and Gold layers. Apache Airflow orchestrates the workflow, while RustFS provides S3-compatible object storage.

## 2. Architecture Diagram

```text
                 City of Chicago Data Portal
                    Taxi Trips API
                           |
                           v
                 +---------------------+
                 |  Python Ingestion   |
                 |  API extraction     |
                 |  Pagination         |
                 |  Batch validation   |
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |     Bronze Layer    |
                 |   RustFS / JSONL    |
                 |   Raw API records   |
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |  PySpark Processing |
                 |  Cleaning & Casting |
                 |  Data Quality Rules |
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |     Silver Layer    |
                 |  RustFS / Parquet   |
                 |  Cleaned Trip Data  |
                 +---------------------+
                           |
                +----------+----------+
                |                     |
                v                     v
      +-------------------+  +-------------------+
      |  Quality Report   |  |  Gold Aggregation |
      |  Monthly Metrics  |  |  PySpark          |
      +-------------------+  +-------------------+
                |                     |
                v                     v
      +-------------------+  +-------------------+
      | Quality Layer     |  | Gold Layer        |
      | RustFS / Parquet  |  | RustFS / Parquet  |
      +-------------------+  +-------------------+
                                      |
                                      v
                             +-------------------+
                             | Streamlit         |
                             | Dashboard         |
                             +-------------------+

              Apache Airflow orchestrates
              the pipeline processing tasks.
```

## 3. Technology Stack

| Component | Technology | Responsibility |
|---|---|---|
| Data source | City of Chicago Data Portal API | Provides taxi trip records. |
| Ingestion | Python, Requests | Extracts and stores paginated API data. |
| Object storage | RustFS | Stores Bronze, Silver, Quality and Gold datasets. |
| Storage protocol | S3-compatible API | Enables access to RustFS through S3 clients. |
| Processing | Apache Spark, PySpark | Cleans, validates and aggregates trip records. |
| Orchestration | Apache Airflow | Schedules and coordinates pipeline tasks. |
| Storage formats | JSONL, Parquet | Stores raw and analytical datasets. |
| Dashboard | Streamlit, Pandas, PyArrow | Reads Gold and Quality datasets and presents metrics. |
| Environment | Docker, Docker Compose | Runs the pipeline services in containers. |

## 4. Data Flow

### 4.1 Data Extraction — Bronze

A Python ingestion module extracts taxi trip records from the Chicago Data Portal API.

The extraction process:
- Retrieves records for the selected reporting period.
- Uses pagination to retrieve large volumes of data in batches.
- Stores raw records in JSONL format.
- Organizes objects by year and month.
- Maintains ingestion manifests containing batch-level metadata.

Bronze preserves the source records before analytical transformations.

### 4.2 Data Transformation — Silver

PySpark reads the Bronze JSONL files and applies the transformation rules.

The processing includes:
- Cleaning string fields and handling empty values.
- Converting timestamps and numeric fields to their expected data types.
- Preserving raw numeric values for diagnostic purposes.
- Applying data-quality checks.
- Separating records that fail critical structural validation into Quarantine.

The resulting Silver dataset is stored in Parquet format, partitioned by year and month.

### 4.3 Data Quality Reporting

A dedicated quality-reporting task evaluates the Silver dataset.

It calculates monthly metrics, including:
- Total records and unique trip identifiers.
- Duplicate trip identifiers.
- Counts and percentages of records affected by each implemented quality flag.

The resulting report is stored in the Quality layer in Parquet format.

### 4.4 Analytical Transformation — Gold

PySpark generates three analytical datasets from Silver:

- **Daily Revenue:** Daily trip volume, reported revenue, average trip revenue, fares and tips.
- **Daily Trip Metrics:** Daily average trip duration and distance, with independent eligibility rules.
- **Pickup Zone Performance:** Monthly trip volume, revenue, average distance and ranking by pickup community area.

These datasets are stored in Parquet format and serve as the dashboard's analytical source.

### 4.5 Dashboard

The Streamlit dashboard reads the persisted Gold and Quality datasets directly from RustFS through its S3-compatible interface.

It provides:
- Overall trip and revenue KPIs.
- Daily revenue and trip-volume trends.
- Duration and distance indicators.
- Pickup community area performance.
- Monthly data-quality metrics.
- A month filter for exploring the reporting period.

The dashboard does not execute the transformations. It consumes the datasets produced by the pipeline.

## 5. Orchestration

Apache Airflow coordinates the workflow through the `chicago_taxi_pipeline` DAG.

The DAG contains four sequential tasks:

| Order | Task | Responsibility |
|---|---|---|
| 1 | `ingest_data` | Extracts API records and writes Bronze batches. |
| 2 | `bronze_to_silver` | Cleans, casts and validates raw records. |
| 3 | `silver_quality_report` | Generates monthly data-quality metrics. |
| 4 | `silver_to_gold` | Produces the analytical Gold datasets. |

Each task depends on the successful completion of the preceding task.

The DAG is configured for manual execution (`schedule=None`).

## 6. Storage Organization

All persistent pipeline datasets are stored in the RustFS bucket `chicago-taxi-lake`.

```text
chicago-taxi-lake/
├── bronze/
│   └── year=2023/
│       ├── month=09/
│       ├── month=10/
│       ├── month=11/
│       └── month=12/
├── silver/
│   └── taxi_trips/
├── quarantine/
│   └── taxi_trips/
├── quality/
│   └── silver/
│       └── monthly_report/
└── gold/
    ├── daily_revenue/
    ├── daily_trip_metrics/
    └── pickup_zone_performance/
```

Bronze data is stored as JSONL, while Silver, Quarantine, Quality and Gold datasets use Parquet.

## 7. Deployment and Runtime

Docker Compose runs the main application services:

- **Airflow:** Executes and monitors the pipeline DAG.
- **RustFS:** Provides persistent S3-compatible object storage.
- **Streamlit:** Serves the analytical dashboard.

PySpark runs within the Airflow execution environment for the transformation tasks. Its temporary processing files are separate from the persistent datasets stored in RustFS.

The dashboard accesses RustFS through the internal Docker network. The Airflow UI, RustFS console and dashboard are exposed on their respective local ports.

## 8. Design Principles

The architecture follows several principles:

- **Layer separation:** Raw, transformed and analytical datasets are stored independently.
- **Reproducibility:** Transformations operate on persisted Bronze data.
- **Data traceability:** Quality indicators and quarantine diagnostics make data issues visible.
- **Idempotent ingestion:** Existing validated batches can be reused rather than unnecessarily re-ingested.
- **Decoupled consumption:** The dashboard reads Gold and Quality outputs without being coupled to Spark execution.
- **Persistent storage:** Docker volumes preserve RustFS data across container restarts.

---

For field definitions, quality flags and analytical dataset schemas, see [Data Dictionary](data_dictionary.md).