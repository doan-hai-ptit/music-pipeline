import os
from pyspark.sql import SparkSession
from datetime import date, timedelta

yesterday = (date.today() - timedelta(days=1)).isoformat()
# yesterday = "2026-01-09"
GCS_BUCKET = os.getenv("GCS_BUCKET", "music-pipeline")

RAW_PATH = f"gs://{GCS_BUCKET}/raw/music_events/event_date={yesterday}"
MERGED_PATH = f"gs://{GCS_BUCKET}/curated/music_events/event_date={yesterday}"

spark = SparkSession.builder \
    .appName("Merge Parquet") \
    .config("spark.jars","C:\\Code\\Python\\music-pipeline\\spark\\jars\\gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "C:\\Code\\Python\\music-pipeline\\keys\\music-pipline-165b5ccbdee5.json")\
    .getOrCreate()

df = spark.read.parquet(RAW_PATH)
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(MERGED_PATH)
spark.stop()
