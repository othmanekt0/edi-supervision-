import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def long_task():
    time.sleep(60)


with DAG(
    dag_id="test_long_running",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supervision", "test"],
) as dag:

    wait = PythonOperator(
        task_id="wait_60_seconds",
        python_callable=long_task,
    )