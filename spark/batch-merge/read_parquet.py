from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Parquet") \
    .master("local[*]") \
    .getOrCreate()

RAW_PATH = "datalake/raw/music_events"
MERGE_PATH = "datalake/merged/music_events"
df = spark.read.parquet(MERGE_PATH)

df.show(200, truncate=False)
df.printSchema()
