FROM apache/spark:3.5.7

USER root
WORKDIR /app

COPY . .

# Tạo thư mục data + checkpoint
RUN mkdir -p datalake/raw/music_events datalake/merged/music_events checkpoint/raw_music_events

# Chạy Spark Streaming + Kafka
CMD spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
    spark/spark_streaming.py
