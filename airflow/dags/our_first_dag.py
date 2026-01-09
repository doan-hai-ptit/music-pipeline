from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    'owner': "Hai",
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="our_first_dag",
    default_args=default_args,
    description="this is our first dag",
    start_date=datetime(2026, 1, 8),
    schedule="@daily"
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello World!'
    )
