from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

MESSAGE_SCHEMA = StructType([
    StructField("song_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("timestamp", StringType(), True),
])