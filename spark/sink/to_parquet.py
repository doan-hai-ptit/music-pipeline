# sink/to_parquet.py

def write_to_parquet(df, output_mode, output_path, checkpoint="checkpoint/parquet", trigger="2 minutes"):
    """
    Ghi streaming DataFrame vào Parquet theo trigger định kỳ.
    
    Args:
        df: pyspark.sql.DataFrame - streaming DataFrame đã transform/filter
        output_path: str - folder lưu Parquet (Data Lake)
        checkpoint: str - folder checkpoint để Spark lưu trạng thái
        trigger: str - khoảng thời gian batch (vd: "2 minutes")
    
    Returns:
        StreamingQuery
    """
    query = df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime=trigger) \
        .outputMode(output_mode) \
        .start()
    
    return query
