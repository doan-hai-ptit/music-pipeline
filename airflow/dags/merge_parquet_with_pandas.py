import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': "Hai",
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

PROJECT_ID = "{{ var.value.project_id }}"
REGION = "{{ var.value.region_name }}"
BUCKET_NAME = "{{ var.value.bucket_name}}"

def merge_parquet_with_pandas(ds):
    input_path = f"gs://{BUCKET_NAME}/data/status=raw/event_date={ds}/"
    output_path = f"gs://{BUCKET_NAME}/data/status=merged/event_date={ds}/merged.parquet"
    
    try:
        df = pd.read_parquet(input_path)
        df.to_parquet(output_path, index=False)
        print(f"Thành công! Đã gộp dữ liệu ngày {ds}")
    except Exception as e:
        print(f"Lỗi: {e}")

with DAG(
    dag_id="merge_by_pandas_v4", 
    default_args=default_args,
    start_date=datetime(2026, 1, 11), 
    schedule="@daily", 
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="merge_task",
        python_callable=merge_parquet_with_pandas
    )
    task1