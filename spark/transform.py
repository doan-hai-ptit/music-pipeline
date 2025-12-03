from pyspark.sql.functions import col, from_json, to_timestamp
from .schema import MESSAGE_SCHEMA

def parse_messages(df):
    """Parse kafka 'value' (bytes->string->json) and cast timestamp."""
    parsed = df.selectExpr('CAST(value AS STRING) as json_str') \
        .select(from_json(col('json_str'), MESSAGE_SCHEMA).alias('data')) \
        .select('data.*')

    # cast timestamp string to proper timestamp type when possible
    parsed = parsed.withColumn('ts', to_timestamp(col('timestamp')))
    return parsed


def agg_plays_by_song(parsed_df):
    # count plays per song (only action == 'play')
    plays = parsed_df.filter(col('action') == 'play')
    agg = plays.groupBy('song_id').count().alias('play_count')
    return agg
