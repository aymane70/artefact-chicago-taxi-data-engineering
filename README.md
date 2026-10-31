# Technical Decisions and Design Evolution

## 1. Purpose

This document records the main technical discussions, alternatives considered, implementation changes and final engineering choices made during the development of the Chicago Taxi Data Engineering project.

The objective is to distinguish between:

- Initial ideas and proposed approaches.
- Alternatives evaluated during development.
- Decisions changed during implementation.
- The final architecture actually delivered.

The final implementation reflects the requirements and scope of a reproducible local batch data engineering project.

---

## 2. Architecture Evolution

### 2.1 Initial pipeline design

The initial concept was an end-to-end batch pipeline:

```text
Chicago API
    |
    v
Python Ingestion
    |
    v
Bronze
    |
    v
PySpark Transformations
    |
    v
Silver / Gold
```

The first design discussions focused on API pagination, raw-data preservation, transformation stages and orchestration.

As the project evolved, we introduced dedicated quality reporting, object storage and a dashboard.

### 2.2 Storage: local filesystem, MinIO and RustFS

**Initially considered:**
- Local filesystem directories mounted through Docker volumes.
- MinIO as an S3-compatible object-storage service.

**Design evolution:**

A lightweight local setup using mounted directories and local-mode Spark was considered to keep the deployment simple. MinIO was also explored as an object-storage option.

The storage design was subsequently revised to use RustFS.

**Final implementation: RustFS**

RustFS provides an S3-compatible object-storage layer for the project.

The pipeline uses the S3A filesystem connector to read and write Parquet and JSONL datasets.

**Reason for the final choice:**

It provides an object-storage-based architecture while remaining deployable locally through Docker Compose.

The project therefore keeps the separation between processing and storage without requiring a cloud account.

### 2.3 Orchestration: Apache Airflow

**Considered approach:**

Running extraction and transformation scripts manually or chaining them through a lightweight execution mechanism.

**Final implementation: Apache Airflow**

Airflow coordinates the pipeline through a DAG containing four sequential tasks.

This provides:
- Explicit task dependencies.
- Independent task execution and monitoring.
- Execution history.
- Manual triggering.
- Configurable extraction parameters.

The DAG is intentionally manually triggered rather than scheduled because the project processes a defined historical dataset.

### 2.4 Processing: PySpark

**Final implementation: PySpark**

PySpark is used for Bronze-to-Silver and Silver-to-Gold transformations.

It provides distributed-data processing capabilities and a consistent DataFrame-based API for cleaning, validation and aggregation.

The local deployment uses Spark in the project container rather than requiring a separate Spark cluster.

### 2.5 Dashboard

**Final implementation: Streamlit with PyArrow**

The dashboard was added to make the Gold and Quality datasets accessible through a simple interface.

It reads persisted Parquet data directly from RustFS using PyArrow.

An important design choice was to avoid rerunning Spark transformations from the dashboard. Processing and presentation remain separate.

---

## 3. Data Ingestion Decisions

### 3.1 Source selection

**Final source:** Chicago Data Portal — Chicago Taxi Trips.

- Dataset ID: `wrvz-psew`
- API: `https://data.cityofchicago.org/resource/wrvz-psew.json`

The selected extraction window is September to December 2023.

This provides a substantial historical dataset while keeping the project manageable for local execution.

### 3.2 Batch extraction

**Final choice: 10,000 records per batch.**

The API is queried month by month using pagination.

The ordering uses:
- `trip_start_timestamp`
- `trip_id`

This provides a stable ordering for pagination.

The extractor also implements retries and maintains batch-level metadata.

### 3.3 Idempotency and resumability

A batch manifest stores information such as record counts and checksums.

The extractor uses this information to identify completed batches and avoid unnecessary repeated extraction.

This was preferred over treating every execution as a completely new ingestion.

---

## 4. Data Lake Design Decisions

### 4.1 Medallion architecture

**Final choice: Bronze, Silver and Gold.**

The layers have distinct responsibilities:

| Layer | Responsibility |
|---|---|
| Bronze | Preserve raw source records |
| Silver | Normalize, validate and enrich records |
| Gold | Produce analytical datasets |

A separate Quality area stores monthly data-quality reports, while Quarantine stores structurally invalid records.

### 4.2 Storage formats

**Final choices:**

| Data area | Format | Reason |
|---|---|---|
| Bronze | JSONL | Preserve source records in a readable raw format |
| Silver | Parquet | Efficient columnar storage for analytical processing |
| Gold | Parquet | Efficient consumption by reporting tools |
| Quality | Parquet | Persist structured quality metrics |

The raw representation is kept separate from the normalized analytical representation.

---

## 5. Data Quality Decisions

### 5.1 Quarantine versus flagging

Two possible approaches were considered for invalid or suspicious records:

1. Reject every record that violates a quality rule.
2. Quarantine structurally invalid records while retaining other records with explicit quality flags.

**Final choice: the second approach.**

Records without a usable trip identifier or valid trip start timestamp are quarantined.

Other issues, such as negative durations, incomplete financial information and financial mismatches, are flagged in Silver.

**Reason:**

A suspicious value does not necessarily make the entire record unusable for every analytical purpose.

Keeping the record and exposing its quality status allows downstream transformations to apply metric-specific eligibility rules.

### 5.2 Financial mismatch tolerance

The calculated financial difference is:

```text
trip_total - (fare + tips + tolls + extras)
```

A mismatch is flagged when the absolute difference exceeds `0.02`.

The tolerance avoids treating very small rounding differences as significant mismatches.

Mismatches are retained and reported rather than automatically discarded.

### 5.3 Metric-specific eligibility

The Gold layer does not apply one universal rule to every metric.

For example:
- Revenue requires an available `trip_total`.
- Duration metrics exclude invalid, negative, missing and DST-ambiguous duration records.
- Distance metrics use their own eligibility conditions.

This avoids unnecessarily excluding records from metrics that can still be calculated reliably.

---

### 6. Execution Time and First Run

The first execution may take longer than subsequent runs.

| Pipeline stage | What to expect |
|---|---|
| `ingest_data` | Approximately 20 minutes. The extractor retrieves more than 2 million records through paginated API requests. Execution time depends on API response times and network connectivity. |
| `bronze_to_silver` | May take longer on the first execution because Spark needs to download and initialize its S3A/Hadoop dependencies. It then reads and transforms the Bronze data. |
| `silver_quality_report` | Calculates monthly quality metrics and persists the report. |
| `silver_to_gold` | Aggregates Silver data into the three Gold datasets. |

**Important notes:**
- Keep Docker services running throughout execution.
- The first Spark execution may involve dependency downloads and initialization overhead.
- Subsequent executions may be faster because dependencies are already available.
- Monitor task logs in Airflow if a task appears to be taking longer than expected.
- Do not interrupt ingestion unnecessarily; the extractor supports resuming completed batches.


---

## 7. Technologies Considered but Not Implemented

The project deliberately avoids adding technologies that are not necessary for its current batch-processing requirements.

| Technology | Decision | Reason |
|---|---|---|
| Kafka | Not implemented | The source is a historical REST API and the pipeline is batch-oriented |
| Kubernetes | Not implemented | Docker Compose is sufficient for the local deployment |
| MLflow | Not implemented | The project focuses on data engineering, not model training or experiment tracking |
| Separate Spark cluster | Not implemented | Local Spark execution is sufficient for the assignment |
| Cloud-managed storage | Not implemented | RustFS provides local S3-compatible storage |

These are scope decisions, not claims that the technologies are unsuitable for production systems.

---

## 8. Final Implementation Results

The final pipeline processed four months of Chicago Taxi Trips data.

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

The quality analysis identified records with duration anomalies, incomplete financial data and financial mismatches. These observations were retained and reported according to the defined quality rules.

The full Airflow pipeline completed successfully.

---

## 9. Future Considerations

The current architecture is intentionally scoped to local batch processing.

Potential future improvements include:

- Incremental Silver and Gold processing.
- Partition-aware updates instead of full refreshes.
- Automated data-quality thresholds and failure conditions.
- More advanced observability and alerting.
- Deployment to cloud object storage and managed orchestration.
- Automated tests for transformations and data-quality rules.

These are potential extensions, not features of the current implementation.
