# dags/breakout_check_dag.py
"""
Airflow DAG: Breakout Detection & Slack Alerts
Runs every 30 minutes, mirroring the APScheduler job_breakout_check().
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)

DEFAULT_ARGS = {
    "owner":            "trends_intel",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def _breakout_check():
    import sys
    sys.path.insert(0, _PROJECT_ROOT)
    from jobs import job_breakout_check
    job_breakout_check()


with DAG(
    dag_id="breakout_detection",
    description="Detect trend breakouts and fire Slack alerts",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/30 * * * *",   # every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trends", "alerts"],
) as dag:

    breakout_task = PythonOperator(
        task_id="check_for_breakouts",
        python_callable=_breakout_check,
    )
