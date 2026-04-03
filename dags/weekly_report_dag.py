# dags/weekly_report_dag.py
"""
Airflow DAG: Weekly Trends Report
Runs every Monday at 07:00 UTC, mirroring the APScheduler job_weekly_report().
Generates an Excel brief and emails/Slacks it to configured recipients.
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
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": False,
}


def _weekly_report():
    import sys
    sys.path.insert(0, _PROJECT_ROOT)
    from jobs import job_weekly_report
    job_weekly_report()


with DAG(
    dag_id="weekly_trends_report",
    description="Generate and distribute weekly Google Trends Ads Intelligence Brief",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 7 * * 1",   # Monday 07:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trends", "reports"],
) as dag:

    report_task = PythonOperator(
        task_id="generate_and_send_report",
        python_callable=_weekly_report,
    )
