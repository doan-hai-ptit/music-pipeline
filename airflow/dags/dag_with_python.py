from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator

default_agrs = {
    "owner": "Hai",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

def greet():
    print("Hello World!")

with DAG(
    dag_id='dag_with_python_v1',
    default_args = default_agrs,
    description = "Dag with python",
    start_date=datetime(2026, 1, 5),
    schedule="@daily"
) as dag:
    task1 = PythonOperator(
        task_id="first_task",
        python_callable = greet
    )
    task1