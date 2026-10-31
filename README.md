# Chicago Taxi Data Engineering Pipeline

An end-to-end batch data engineering project built with Python, Apache Airflow, PySpark, RustFS and Streamlit.

The project extracts Chicago Taxi Trips data from the Chicago Data Portal, stores raw records in an S3-compatible data lake, transforms them through a Medallion architecture, applies data-quality rules and produces analytical datasets for a dashboard.

## 1. Project Overview

The pipeline processes four months of Chicago Taxi Trips data, from September to December 2023.

**Key results:**

| Metric | Result |
|---|---:|
| Extraction period | September–December 2023 |
| Total ingested records | 2,158,006 |
| Ingestion batches | 217 |
| Silver records | 2,158,006 |
| Quarantined records | 0 |
| Daily revenue rows | 122 |
| Daily trip metrics rows | 122 |
| Pickup-zone performance rows | 308 |

### Architecture

```text
                 Chicago Data Portal API
                           |
                           v
                  Python Data Ingestion
                           |
                           v
                    RustFS - Bronze
                     Raw JSONL data
                           |
                           v
                       Apache
                        Airflow
                           |
                           v
                     PySpark
                           |
                 +---------+---------+
                 |                   |
                 v                   v
           RustFS - Silver     Quarantine
             Parquet             Parquet
                 |
                 v
             Data Quality
               Reports
                 |
                 v
              PySpark
                 |
                 v
             RustFS - Gold
               Parquet
                 |
                 v
          Streamlit Dashboard
```

### Technologies

- **Python:** API extraction and pipeline logic.
- **Apache Airflow:** Pipeline orchestration and task monitoring.
- **PySpark:** Data cleaning, validation and analytical transformations.
- **RustFS:** S3-compatible object storage for the data lake.
- **Parquet / JSONL:** Data storage formats.
- **Streamlit:** Interactive dashboard.
- **PyArrow:** Direct access to Parquet datasets from RustFS.

---

## 2. Getting Started

### Prerequisites

Before starting, install:

- Docker Desktop with the WSL2 backend enabled on Windows.
- Git.
- At least 8 GB of available RAM is recommended.
- A stable internet connection for the initial image builds, API extraction and Spark dependency downloads.

No separate Spark installation or cloud account is required.

### Step 1 — Clone the repository

Open PowerShell or a terminal and run:

```powershell
git clone https://github.com/aymane70/artefact-chicago-taxi-data-engineering.git
```

Navigate to the project directory:

```powershell
cd artefact-chicago-taxi-data-engineering
```

### Step 2 — Start the services

Make sure Docker Desktop is running.

Build the Docker images and start all services:

```powershell
docker compose up --build -d
```

Check that the containers are running:

```powershell
docker compose ps
```

The project starts three main services:

| Service | Purpose |
|---|---|
| Airflow | Orchestrates the data pipeline |
| RustFS | Stores Bronze, Silver, Gold and Quality data |
| Streamlit | Displays analytical results |

The first startup may take a few minutes while Docker builds the images and initializes the services.

### Step 3 — Access the web interfaces

Once the containers are running, open the following URLs.

| Interface | URL | Purpose |
|---|---|---|
| Airflow | http://localhost:8080 | Run and monitor the pipeline |
| RustFS Console | http://localhost:9001 | Browse the data lake |
| Streamlit Dashboard | http://localhost:8501 | Explore analytical results |

**Airflow**

Open:

http://localhost:8080

The Airflow interface is used to trigger the pipeline and monitor each task.

If authentication is requested, retrieve the generated credentials from the Airflow container logs:

```powershell
docker compose logs airflow
```

**RustFS**

Open:

http://localhost:9001

Use the demonstration credentials configured in `docker-compose.yml`:

```text
Username: CHICAGO_TAXI_ADMIN
Password: ChicagoTaxi_DE_Project_2026_Key
```

The main bucket is:

```text
chicago-taxi-lake
```

After ingestion and transformation, its contents include:

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

**Streamlit Dashboard**

Open:

http://localhost:8501

The dashboard reads the persisted Gold and Quality datasets directly from RustFS. It does not rerun the Spark transformations.

The dashboard provides:

- Revenue KPIs and daily revenue trends.
- Trip volume and duration analysis.
- Distance metrics.
- Pickup-zone performance rankings.
- Monthly data-quality reports.
- Month-based filtering.

The dashboard becomes useful after the pipeline has completed successfully and the Gold datasets are available.

---

## 3. Run the Data Pipeline

### Step 1 — Open Airflow

Navigate to:

http://localhost:8080

Find the DAG:

```text
chicago_taxi_pipeline
```

The DAG contains four sequential tasks:

| Task | Description |
|---|---|
| `ingest_data` | Extracts Chicago Taxi Trips records and stores them in Bronze |
| `bronze_to_silver` | Cleans, casts, validates and separates records into Silver and Quarantine |
| `silver_quality_report` | Calculates and persists monthly data-quality metrics |
| `silver_to_gold` | Builds the analytical Gold datasets |

### Step 2 — Trigger the DAG

1. Find `chicago_taxi_pipeline` in the DAG list.
2. Enable the DAG if it is paused.
3. Open the DAG.
4. Click **Trigger DAG**.
5. Monitor the task execution in the Grid or Graph view.

The tasks run sequentially. Each task starts only after its upstream task succeeds.

### Step 3 — Monitor execution

Open a task to inspect its logs.

The logs provide information about:

- API extraction progress.
- Number of records and batches processed.
- Bronze, Silver and Quarantine storage paths.
- Spark transformation execution.
- Data-quality results.
- Gold dataset generation.

If a task fails, inspect its logs to identify the cause before retrying it.

### Step 4 — Inspect the outputs

After successful execution:

1. Open RustFS and browse the `chicago-taxi-lake` bucket.
2. Inspect the Bronze, Silver, Quality and Gold folders.
3. Open the Streamlit dashboard to explore the analytical results.

---

## 4. Execution Time and Expected Results

The first execution may take longer than subsequent runs.

| Pipeline stage | What to expect |
|---|---|
| `ingest_data` | Approximately 20 minutes. The extractor retrieves more than 2 million records through paginated API requests. Execution time depends on API response times and network connectivity. |
| `bronze_to_silver` | Can take longer on the first execution because Spark downloads and initializes its S3A/Hadoop dependencies. It then reads and transforms the Bronze data. |
| `silver_quality_report` | Calculates monthly quality metrics and persists the report. |
| `silver_to_gold` | Aggregates Silver data into the three Gold datasets. |

### Expected pipeline results

| Metric | Expected result |
|---|---:|
| Bronze records | 2,158,006 |
| Silver records | 2,158,006 |
| Quarantined records | 0 |
| Daily revenue rows | 122 |
| Daily trip metrics rows | 122 |
| Pickup-zone performance rows | 308 |

The quality analysis also identifies records with duration anomalies, incomplete financial data and financial mismatches. These observations are retained and reported according to the defined quality rules.

### Important execution notes

- Keep Docker services running throughout pipeline execution.
- The first Spark execution may download Hadoop AWS and AWS SDK dependencies.
- Subsequent Spark executions can reuse cached dependencies when the cache is preserved.
- The ingestion process uses batch manifests to identify completed batches and resume interrupted extraction.
- Monitor Airflow task logs if execution takes longer than expected.
- Avoid deleting Docker volumes while the pipeline data needs to be preserved.

---

## 5. Stop the Services

To stop the containers:

```powershell
docker compose down
```

This stops and removes the containers but preserves the named Docker volumes, including the RustFS data.

To start the project again:

```powershell
docker compose up -d
```

**Warning:** The following command deletes the named volumes and their stored data:

```powershell
docker compose down -v
```

Use it only when you intentionally want a clean environment and are prepared to lose the existing Bronze, Silver and Gold datasets.

---

## 6. Data Lake Design

The project follows the Medallion architecture, with three main data layers.

| Layer | Responsibility |
|---|---|
| Bronze | Preserves raw source records |
| Silver | Normalizes, validates and enriches records |
| Gold | Produces analytical datasets |

A separate Quality area stores monthly data-quality reports, while Quarantine stores structurally invalid records.

### Storage formats

| Data area | Format | Reason |
|---|---|---|
| Bronze | JSONL | Preserves source records in a readable raw format |
| Silver | Parquet | Efficient columnar storage for analytical processing |
| Gold | Parquet | Efficient consumption by reporting tools |
| Quality | Parquet | Persists structured quality metrics |

The raw representation remains separate from the normalized analytical representation.

---

## 7. Data Quality Rules

### Quarantine versus flagging

Two approaches were considered:

1. Reject every record that violates a quality rule.
2. Quarantine structurally invalid records while retaining other records with explicit quality flags.

The second approach was selected.

Records without a usable trip identifier or valid trip start timestamp are quarantined.

Other issues, such as negative durations, incomplete financial information and financial mismatches, are flagged in Silver.

A suspicious value does not necessarily make the entire record unusable for every analytical purpose. Retaining the record and exposing its quality status allows downstream transformations to apply metric-specific eligibility rules.

### Financial reconciliation

The calculated financial difference is:

```text
trip_total - (fare + tips + tolls + extras)
```

A mismatch is flagged when the absolute difference exceeds `0.02`.

This tolerance avoids treating very small rounding differences as significant mismatches. Mismatches are retained and reported rather than automatically discarded.

### Metric-specific eligibility

The Gold layer does not apply one universal rule to every metric.

For example:

- Revenue requires an available `trip_total`.
- Duration metrics exclude invalid, negative, missing and DST-ambiguous duration records.
- Distance metrics use their own eligibility conditions.

This avoids unnecessarily excluding records from metrics that can still be calculated reliably.

---

## 8. Project Structure

```text
artefact-chicago-taxi-data-engineering/
│
├── dags/
│   └── chicago_taxi_pipeline.py
│
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── technical_decisions.md
│
├── src/
│   ├── ingestion/
│   │   └── extract_taxi_data.py
│   ├── quality/
│   │   └── silver_quality_report.py
│   └── transformations/
│       ├── bronze_to_silver.py
│       ├── schemas.py
│       └── silver_to_gold.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## 9. Documentation

Additional documentation is available in the `docs/` directory:

- **[Architecture](docs/architecture.md):** Explains the system architecture, components and data flow.
- **[Data Dictionary](docs/data_dictionary.md):** Describes the datasets, fields, data types and analytical outputs.
- **[Technical Decisions](docs/technical_decisions.md):** Documents the alternatives considered, design evolution, implementation choices and future considerations.

The technical decisions document also explains why certain technologies were selected and why others were not implemented.

---

## 10. Data Source

The project uses the official Chicago Data Portal — Chicago Taxi Trips dataset.

- Dataset ID: `wrvz-psew`
- API: https://data.cityofchicago.org/resource/wrvz-psew.json
- Extraction period: September–December 2023
- Batch size: 10,000 records

The API is queried month by month using pagination, with a stable ordering based on `trip_start_timestamp` and `trip_id`.

---

## 11. Future Improvements

Potential future extensions include:

- Incremental Silver and Gold processing.
- Partition-aware updates instead of full refreshes.
- Automated data-quality thresholds and failure conditions.
- More advanced observability and alerting.
- Deployment to cloud object storage and managed orchestration.
- Automated tests for transformations and data-quality rules.

These are potential extensions, not features of the current implementation.
