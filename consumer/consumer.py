from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "music_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer connected. Listening for events...\n")

for message in consumer:
    print("[RECEIVED]", message.value)
