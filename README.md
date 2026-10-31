# Chicago Taxi Data Engineering Pipeline

An end-to-end data engineering project built on the City of Chicago Taxi Trips dataset. The pipeline extracts raw data from the Chicago Data Portal, stores it in an S3-compatible data lake, transforms it through Bronze, Silver and Gold layers using PySpark, and orchestrates the workflow with Apache Airflow.

## Documentation

For more details about the pipeline:

- [Architecture](docs/architecture.md) — System components, data flow, orchestration and deployment.
- [Data Dictionary](docs/data_dictionary.md) — Bronze, Silver, Quality and Gold schemas, fields and validation rules.
- [Technical Decisions](docs/technical_decisions.md) — Engineering choices, ingestion strategy, idempotency, retries and data-quality policies.


## Architecture

```text
                 City of Chicago API
                         |
                         v
                  Python Ingestion
                         |
                         v
                  RustFS - Bronze
                   Raw JSONL data
                         |
                         v
                  PySpark Processing
                         |
                  +------+------+
                  |             |
                  v             v
             RustFS - Silver  Quarantine
                  |
          +-------+--------+
          |                |
          v                v
     Quality Report    PySpark Aggregations
          |                |
          v                v
     RustFS - Quality  RustFS - Gold
                            |
                            v
                    Streamlit Dashboard

               Apache Airflow
             orchestrates all tasks
```

For more details, see:
- [Architecture](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)

### Technology stack

| Technology | Purpose |
|---|---|
| Python | API ingestion and processing utilities |
| Apache Airflow 3.3.2 | Workflow orchestration |
| Apache Spark / PySpark 4.2.0 | Data transformations |
| RustFS | S3-compatible object storage |
| Parquet / JSONL | Data storage formats |
| Streamlit | Analytics dashboard |
| Docker Compose | Local deployment |

## Dataset and scope

**Source:** [City of Chicago — Taxi Trips 2013–2023](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew)

The pipeline processes all available records for the four months September–December 2023.

| Month | Records |
|---|---:|
| September 2023 | 558,682 |
| October 2023 | 606,474 |
| November 2023 | 527,797 |
| December 2023 | 465,053 |
| **Total** | **2,158,006** |

## Project structure

```text
artefact-chicago-taxi-data-engineering/
├── dags/
│   └── chicago_taxi_pipeline.py
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
├── src/
│   ├── ingestion/
│   │   └── extract_taxi_data.py
│   ├── quality/
│   │   └── silver_quality_report.py
│   └── transformations/
│       ├── bronze_to_silver.py
│       ├── schemas.py
│       ├── silver_to_gold.py
│       └── validate_silver.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

## Prerequisites

- Docker Desktop with Docker Compose.
- An internet connection for the initial Docker image builds and data ingestion.

Make sure Docker Desktop is running before starting the project.

## Getting started

Clone the repository:

```powershell
git clone https://github.com/aymane70/artefact-chicago-taxi-data-engineering.git
cd artefact-chicago-taxi-data-engineering
```

Build and start all services:

```powershell
docker compose up -d --build
```

Check the service status:

```powershell
docker compose ps
```

The project starts three services:

| Service | Purpose | Local URL |
|---|---|---|
| Airflow | Pipeline orchestration | http://localhost:8080 |
| RustFS Console | Data lake exploration | http://localhost:9001 |
| Streamlit | Analytics dashboard | http://localhost:8501 |

**Note:** Docker Compose starts the services but does not automatically execute the Airflow DAG.

## Running the pipeline

On a fresh installation, the data lake is initially empty. Run the pipeline before opening the dashboard.

1. Open the Airflow UI at http://localhost:8080.
2. Locate `chicago_taxi_pipeline`.
3. Enable the DAG if necessary.
4. Trigger a DAG run.
5. Monitor the execution until all four tasks have succeeded.

The workflow executes the following tasks sequentially:

| Task | Description |
|---|---|
| `ingest_data` | Extracts the Chicago API data and stores raw JSONL batches in Bronze. |
| `bronze_to_silver` | Applies schema enforcement, transformations and data-quality checks. Writes Silver and Quarantine outputs. |
| `silver_quality_report` | Generates monthly data-quality metrics. |
| `silver_to_gold` | Builds the analytical Gold datasets from Silver. |

The DAG is configured for manual execution (`schedule=None`).

## Exploring the data lake

Open the RustFS Console:

http://localhost:9001

Use the local demonstration credentials configured in `docker-compose.yml`:

- **Access key:** `CHICAGO_TAXI_ADMIN`
- **Secret key:** `ChicagoTaxi_DE_Project_2026_Key`

Select the `chicago-taxi-lake` bucket.

The main object prefixes are:

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

Bronze contains raw JSONL batches and ingestion manifests. Silver, Quality and Gold datasets are stored in Parquet format.

## Analytics dashboard

Open the Streamlit dashboard:

http://localhost:8501

The dashboard reads the persisted Gold and Quality datasets directly from RustFS. It does not rerun Spark transformations.

It provides:

- Total trips and revenue KPIs.
- Daily revenue trends.
- Daily trip volume, duration and distance metrics.
- Monthly pickup-zone performance rankings.
- Monthly Silver data-quality metrics.
- A filter for September–December 2023.

**Important:** On a fresh installation, trigger and complete the pipeline before opening the dashboard. On subsequent starts, previously persisted results remain available in the RustFS volume.

## Stopping the project

Stop the containers:

```powershell
docker compose down
```

This removes the containers and network while preserving named volumes, including the RustFS data lake.

To remove all containers and persistent volumes:

```powershell
docker compose down -v
```

**Warning:** The `-v` option deletes the persisted data lake and other named volume data.

## Security note

This project is configured for local development and technical evaluation:

- Airflow uses simplified authentication.
- RustFS uses predefined demonstration credentials.
- The Streamlit dashboard does not require authentication.
- Service ports are bound to `127.0.0.1`.

These settings are intended to simplify local evaluation, not production deployment. A production environment requires proper authentication, secret management, access controls and network security.

## Results

The complete pipeline has been successfully executed for the four selected months:

- **2,158,006** records ingested.
- **217** raw ingestion batches stored in Bronze.
- **2,158,006** records processed into Silver.
- **0** records quarantined under the current critical-record validation rules.
- Monthly data-quality reports generated.
- Three Gold datasets generated and persisted in RustFS.
- Streamlit dashboard successfully reads and displays the persisted outputs.
