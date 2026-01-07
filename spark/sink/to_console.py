# spark/sink/to_console.py
# Helper ghi streaming DataFrame ra console

def write_to_console(df, output_mode="append", checkpoint="checkpoint/console"):
    """
    df: Spark streaming DataFrame
    output_mode: 'append', 'complete' hoặc 'update'
    checkpoint: folder checkpoint để Spark nhớ trạng thái
    """
    query = df.writeStream \
        .format("console") \
        .outputMode(output_mode)\
        .option("truncate", False) \
        .option("numRows", 50) \
        .option("checkpointLocation", checkpoint) \
        .start()
    return query
