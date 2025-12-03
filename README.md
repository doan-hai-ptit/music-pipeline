# music-pipeline

Realtime pipeline that reads music event messages from Kafka and processes them with PySpark Structured Streaming.

## Quickstart

1. Start Kafka (or use provided docker-compose in `/docker`).
2. Run `kafka/producer.py` to publish sample messages (or use your own producer).
3. Run `spark/spark_streaming.py` using `spark-submit`.

Example:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 spark\spark_streaming.py