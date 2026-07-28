from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def slow_task():
    """Tâche qui dure 30 secondes — dépasse volontairement le SLA de 10s."""
    time.sleep(30)

with DAG(
    dag_id="test_sla_breach",
    start_date=datetime(2026, 7, 17, 8, 0, 0),
    schedule_interval="*/2 * * * *",  # toutes les 2 minutes
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
) as dag:

    task = PythonOperator(
        task_id="slow_task",
        python_callable=slow_task,
        sla=timedelta(seconds=10),  # SLA natif Airflow : 10 secondes
    )