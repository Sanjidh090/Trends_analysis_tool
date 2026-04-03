# dags/daily_collection_dag.py
"""
Airflow DAG: Daily Trends Collection
Runs every day at 06:00 UTC, mirroring the APScheduler job_daily_collection().
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)

DEFAULT_ARGS = {
    "owner":            "trends_intel",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


def _collect():
    import sys
    sys.path.insert(0, _PROJECT_ROOT)
    from jobs import job_daily_collection
    job_daily_collection()


with DAG(
    dag_id="daily_trends_collection",
    description="Collect Google Trends data for all configured geos and keywords",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trends", "collection"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_all_geos",
        python_callable=_collect,
    )
