from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator

default_agrs = {
    "owner": "Hai",
    "retries": 5,
    "retry_delay": timedelta(minutes= 2)
}

def get_dbt_version():
    import subprocess
    output = subprocess.check_output(["dbt", "--version"], text=True)
    print(output.splitlines()[0])

with DAG(
    dag_id="dag_with_python_dependencies_v01",
    default_args = default_agrs,
    start_date =datetime(2026, 1, 8),
    schedule = "@daily"
) as dag:
    get_dbt_version = PythonOperator(
        task_id = "get_dbt_version",
        python_callable = get_dbt_version
    )
    get_dbt_version