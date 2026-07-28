from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def fail():
    raise Exception("Failure generated for supervision test")


with DAG(
    dag_id="test_failure",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supervision", "test"],
) as dag:

    task = PythonOperator(
        task_id="failing_task",
        python_callable=fail,
    )