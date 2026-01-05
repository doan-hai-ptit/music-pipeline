from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("Merge Parquet") \
    .getOrCreate()

input_path = "datalake/raw/music_events/*.parquet"
output_path = "datalake/merged/music_events/"

df = spark.read.parquet(input_path)

if "event_id" in df.columns:
    df = df.dropDuplicates(["event_id"])
df.coalesce(1) \
    .write \
    .mode("append") \
    .parquet(output_path)

spark.stop()
