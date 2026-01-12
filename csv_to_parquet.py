from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CSVtoParquet") \
    .getOrCreate()

artists_df = spark.read \
    .option("header", "true") \
    .csv("data/artists.csv")

artists_df = artists_df.withColumn("artist_id", col("artist_id").cast("int"))

artists_df.write \
    .mode("overwrite") \
    .parquet("datalake/dim_artists")

songs_df = spark.read \
    .option("header", "true") \
    .csv("data/songs.csv")

songs_df = songs_df \
    .withColumn("song_id", col("song_id").cast("int")) \
    .withColumn("artist_id", col("artist_id").cast("int"))

songs_df.write \
    .mode("overwrite") \
    .parquet("datalake/dim_songs")

country_df = spark.read \
    .option("header", "true") \
    .csv("data/countries.csv")

country_df = country_df.withColumn("country_id", col("country_id").cast("int"))

country_df.write \
    .mode("overwrite") \
    .parquet("datalake/dim_countries")
