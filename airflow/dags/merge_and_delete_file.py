from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from datetime import date, timedelta, datetime
import uuid

PROJECT_ID = "{{ var.value.project_id }}"
REGION = "{{ var.value.region_name }}"
BUCKET_NAME = "{{ var.value.bucket_name }}"

yesterday = (date.today() - timedelta(days=1)).isoformat()

default_args = {
    'owner': "Hai",
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/dataproc_merge_file.py",
        "args": [
            f"gs://{BUCKET_NAME}/data/status=raw/event_date={yesterday}/*.parquet",
            f"gs://{BUCKET_NAME}/data/status=merged/event_date={yesterday}"
        ],
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default",
        }
    },
}

with DAG(
    dag_id="merge_and_delete_file_in_GCS",
    default_args = default_args,
    start_date=datetime(2026, 1, 11),
    catchup=False,
    schedule="@daily"
) as dag:
    run_merge_job = DataprocCreateBatchOperator(
        task_id="run_pyspark_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="merge-{{ ds_nodash }}-" + str(uuid.uuid4().hex[:5]),
        batch=BATCH_CONFIG,
    )

    # delete_old_raw = GCSDeleteObjectsOperator(
    #     task_id="cleanup_raw_data",
    #     bucket_name=BUCKET_NAME,
    #     prefix=f"gs://{BUCKET_NAME}/data/status=raw/event_date={yesterday}/",
    # )
    run_merge_job 

    # run_merge_job >> delete_old_raw