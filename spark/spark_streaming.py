import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from schema import MESSAGE_SCHEMA
from sink.to_console import write_to_console
from sink.to_parquet import write_to_parquet


GCS_BUCKET = os.getenv("GCS_BUCKET", "music-pipeline")
RAW_PATH = f"gs://{GCS_BUCKET}/raw/music_events"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoint/raw/music_events"

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "C:\\Code\\Python\\music-pipeline\\keys\\music-pipline-165b5ccbdee5.json")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "earliest") \
    .load()

print("Kafka Connected OK:", df.isStreaming)
df.printSchema()

events = df.select(
    from_json(col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
).select("data.*")

events = events.withColumn(
    "created_at",
    to_timestamp(col("created_at"))
)

events = events.withColumn(
    "event_date",
    to_date(col("created_at"))
)

raw_query = write_to_parquet(
    df=events,
    output_path=RAW_PATH,
    checkpoint=CHECKPOINT_PATH,
    trigger="5 minutes",
    partition_by=["event_date"]
)

# raw_query = write_to_console(
#     df = events
# )

raw_query.awaitTermination()