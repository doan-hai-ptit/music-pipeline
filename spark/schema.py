from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

MESSAGE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("song_id", IntegerType(), True),
    StructField("country_id", IntegerType(), True),
    StructField("event_type", StringType(), True),     # play, pause, like...
    StructField("platform", StringType(), True),
    StructField("created_at", StringType(), True)       # sẽ convert sang TimestampType
])
