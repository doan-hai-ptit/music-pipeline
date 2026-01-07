from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSVtoParquet") \
    .getOrCreate()

# # Artists
# artists_df = spark.read \
#     .option("header", "true") \
#     .csv("data/artists.csv")

# artists_df.write \
#     .mode("overwrite") \
#     .parquet("datalake/dim_artists")

# # Songs
# songs_df = spark.read \
#     .option("header", "true") \
#     .csv("data/songs.csv")

# songs_df.write \
#     .mode("overwrite") \
#     .parquet("datalake/dim_songs")

# Country
Country_df = spark.read \
    .option("header", "true") \
    .csv("data/country.csv")

Country_df.write \
    .mode("overwrite") \
    .parquet("datalake/dim_country")
