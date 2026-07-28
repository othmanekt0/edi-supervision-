from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_parallel",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supervision", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_c = EmptyOperator(task_id="task_c")

    end = EmptyOperator(task_id="end")

    start >> [task_a, task_b, task_c] >> end