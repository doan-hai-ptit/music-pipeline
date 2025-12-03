import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType

from schema import MESSAGE_SCHEMA
from sink.to_console import write_to_console
from sink.to_parquet import write_to_parquet

jars_dir = os.path.join(os.getcwd(), "libs")
jars = [os.path.join(jars_dir, jar) for jar in os.listdir(jars_dir) if jar.endswith(".jar")]
jars_list = ",".join(jars)


spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .master("local[*]") \
    .config("spark.jars", jars_list) \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("user_id", StringType()) \
    .add("song_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "earliest") \
    .load()

print("Kafka Connected OK:", df.isStreaming)
df.printSchema()
# df.show()
events = df.select(
    from_json(col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
).select("data.*")

checkpoint_dir = "C:/spark-checkpoint/music_events"

events = events.filter(col("event_type") == "play")
agg_df = events.groupBy("song_id").agg(count("*").alias("play_count"))
# query = events.writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

query = write_to_console(agg_df, "complete")

# query = write_to_parquet(
#     agg_df, 
#     output_mode="update",
#     output_path="datalake/music_events_play", 
#     checkpoint="checkpoint/music_events_parquet", 
#     trigger="1 minutes"
# )

query.awaitTermination()
