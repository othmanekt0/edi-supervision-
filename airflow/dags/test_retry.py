from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context


def retry_task(**context: Context):
    try_number = context['ti'].try_number
    raise Exception(f"Retry test — échec définitif, tentative {try_number}")


with DAG(
        dag_id="test_retry",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["supervision", "test"],
) as dag:

    retry = PythonOperator(
        task_id="retry_task",
        python_callable=retry_task,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )