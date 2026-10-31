
import sys
from datetime import datetime, timezone

from airflow.sdk import dag, task, get_current_context

# Make the project's src directory importable
sys.path.insert(0, "/opt/airflow/src")

from ingestion.extract_taxi_data import extract_all_months, MONTHS


@dag(
    dag_id="chicago_taxi_pipeline",
    description="Chicago Taxi Trips end-to-end data engineering pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["chicago", "taxi", "data-engineering"],
)
def chicago_taxi_pipeline():

    @task
    def ingest_data():
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        months = conf.get("months", MONTHS)
        max_batches = conf.get("max_batches")

        extract_all_months(
            months=months,
            max_batches=max_batches,
        )

    @task
    def bronze_to_silver():
        from transformations.bronze_to_silver import (
            transform_bronze_to_silver,
        )

        transform_bronze_to_silver()

    @task
    def silver_quality_report():
        from quality.silver_quality_report import (
            generate_quality_report,
        )

        generate_quality_report()

    @task
    def silver_to_gold():
        from transformations.silver_to_gold import (
            transform_silver_to_gold,
        )

        transform_silver_to_gold()


    # Define execution order
    ingestion = ingest_data()
    transformation = bronze_to_silver()
    quality_report = silver_quality_report()
    gold = silver_to_gold()

    ingestion >> transformation >> quality_report >> gold


chicago_taxi_pipeline()
