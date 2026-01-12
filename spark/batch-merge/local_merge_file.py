import os
from pyspark.sql import SparkSession
from datetime import date, timedelta

yesterday = (date.today() - timedelta(days=1)).isoformat()
# yesterday = "2026-01-09"
GCS_BUCKET = os.getenv("GCS_BUCKET", "music-pipeline")
KEY_PATH= os.getenv("GCP_KEY_PATH", "keys/key.json")
RAW_PATH = f"gs://{GCS_BUCKET}/data/status=raw/event_date={yesterday}"
MERGED_PATH = f"gs://{GCS_BUCKET}/data/status=merged/event_date={yesterday}"

spark = SparkSession.builder \
    .appName("Merge Parquet") \
    .config("spark.jars","spark/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH)\
    .getOrCreate()

df = spark.read.parquet(RAW_PATH)
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(MERGED_PATH)
spark.stop()
