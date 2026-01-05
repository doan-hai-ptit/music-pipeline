import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count

from schema import MESSAGE_SCHEMA
from sink.to_console import write_to_console
from sink.to_parquet import write_to_parquet

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.sql.shuffle.partitions", 4) \
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


raw_query = write_to_parquet(
    df=events,
    output_path="datalake/raw/music_events",
    checkpoint="checkpoint/raw_music_events",
    trigger="1 minutes"
)

raw_query.awaitTermination()