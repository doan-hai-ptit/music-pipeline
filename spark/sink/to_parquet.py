def write_to_parquet(
        df,
        output_path,
        checkpoint="checkpoint/parquet",
        trigger="2 minutes",
        partition_by=None
    ):
    """
    Write streaming DataFrame into Parquet with optional partitioning.
    """

    writer = df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint) \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger)

    # Partition theo date
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer.start()