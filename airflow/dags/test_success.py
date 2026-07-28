from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def push_metrics(**context):
    # En prod, ces valeurs viendraient des tâches upstream via XCom
    # Ici on simule des valeurs réalistes pour le test
    context['ti'].xcom_push(key='rows_read', value=1500)
    context['ti'].xcom_push(key='rows_written', value=1450)
    context['ti'].xcom_push(key='rows_rejected', value=50)

with DAG(
    dag_id="test_success",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supervision", "test"],
) as dag:

    start = EmptyOperator(task_id="start")
    extract = EmptyOperator(task_id="extract")
    transform = EmptyOperator(task_id="transform")
    load = EmptyOperator(task_id="load")
    metrics = PythonOperator(task_id="push_metrics", python_callable=push_metrics)
    end = EmptyOperator(task_id="end")

    start >> extract >> transform >> load >> metrics >> end