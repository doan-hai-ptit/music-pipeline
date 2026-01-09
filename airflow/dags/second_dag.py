from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator

default_args ={
    "owner": "Hai",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="the_second_dag",
    default_args = default_args,
    description="this is a the second test dag",
    start_date=datetime(2026, 1, 5, 8),
    schedule="@daily"
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello World!'
    )
    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo Hello PTIT!"
    )
    task1>>task2