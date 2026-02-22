from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Reuse ETL functions from Task 2
from pipeline.data_pipeline import (
    load_env,
    bq_client,
    ensure_dataset,
    extract_all_pages,
    transform_daily_visits,
    transform_ga_sessions,
    run_quality_checks_daily,
    run_quality_checks_ga,
    load_df_to_bq,
    schema_daily_visits,
    schema_ga_sessions,
)

from google.cloud import bigquery


# ---------------------------------
# Config (can be overridden by env vars)
# ---------------------------------
DAILY_START_DATE = os.getenv("DAILY_START_DATE", "2016-08-01")
DAILY_END_DATE = os.getenv("DAILY_END_DATE", "2017-08-01")

# Optional override for testing GA task manually (format: YYYYMMDD)
GA_DATE_OVERRIDE = os.getenv("GA_DATE_OVERRIDE")


def get_connections():
    """
    Reads environment variables and creates:
    - API key
    - BigQuery client
    - BigQuery dataset (creates it if missing)
    """
    api_key, gcp_project, bq_dataset, bq_location = load_env()

    client = bq_client(gcp_project, bq_location)
    dataset_id = f"{gcp_project}.{bq_dataset}"

    ensure_dataset(client, dataset_id, bq_location)
    return api_key, client, dataset_id


def run_daily_visits_task(**context):
    """
    Full refresh load for daily_visits.
    This table is small and deterministic, so WRITE_TRUNCATE is used.
    """
    api_key, client, dataset_id = get_connections()

    records, last_payload = extract_all_pages(
        api_key=api_key,
        endpoint="daily-visits",
        base_params={"start_date": DAILY_START_DATE, "end_date": DAILY_END_DATE},
        limit=500,
        raw_subdir="daily_visits",
        raw_prefix=f"daily_visits_{DAILY_START_DATE}_to_{DAILY_END_DATE}",
    )

    df = transform_daily_visits(records, last_payload, DAILY_START_DATE, DAILY_END_DATE)
    run_quality_checks_daily(df)

    load_df_to_bq(
        client=client,
        dataset_id=dataset_id,
        table_name="daily_visits",
        df=df,
        schema=schema_daily_visits(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"[INFO] daily_visits task finished. Rows loaded: {len(df)}")


def run_ga_sessions_task(**context):
    """
    Incremental load for ga_sessions (one date per DAG run).

    Uses Airflow logical date (ds_nodash) unless GA_DATE_OVERRIDE is set.
    Example ds_nodash: 20160803
    """
    api_key, client, dataset_id = get_connections()

    # Airflow passes ds_nodash in the task context
    ga_date = GA_DATE_OVERRIDE or context["ds_nodash"]

    records, last_payload = extract_all_pages(
        api_key=api_key,
        endpoint="ga-sessions-data",
        base_params={"date": ga_date},
        limit=500,
        raw_subdir=f"ga_sessions/date={ga_date}",
        raw_prefix=f"ga_sessions_{ga_date}",
    )

    df = transform_ga_sessions(records, last_payload, ga_date)
    run_quality_checks_ga(df)

    # Incremental append. For production rerun safety, use staging + MERGE.
    load_df_to_bq(
        client=client,
        dataset_id=dataset_id,
        table_name="ga_sessions",
        df=df,
        schema=schema_ga_sessions(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"[INFO] ga_sessions task finished for date={ga_date}. Rows loaded: {len(df)}")


def run_summary_task(**context):
    """
    Simple logging task to print DAG run metadata.
    """
    print("[INFO] DAG run completed successfully")
    print(f"[INFO] run_id: {context.get('run_id')}")
    print(f"[INFO] ds: {context.get('ds')}")
    print(f"[INFO] ds_nodash: {context.get('ds_nodash')}")


# ---------------------------------
# Airflow defaults (case study requirements)
# ---------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=3),  # per task
}


with DAG(
    dag_id="etl_google_analytics_dag",
    description="Dish assessment ETL: API -> Transform -> BigQuery",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule="0 6,18 * * 3",  # Wednesdays at 06:00 and 18:00
    default_args=default_args,
    tags=["ga_dag"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    daily_visits_etl = PythonOperator(
        task_id="daily_visits_etl",
        python_callable=run_daily_visits_task,
    )

    ga_sessions_etl = PythonOperator(
        task_id="ga_sessions_etl",
        python_callable=run_ga_sessions_task,
    )

    summary = PythonOperator(
        task_id="summary",
        python_callable=run_summary_task,
    )

    end = EmptyOperator(task_id="end")

    # Task order
    start >> daily_visits_etl >> ga_sessions_etl >> summary >> end