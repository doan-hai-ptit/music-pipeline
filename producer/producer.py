import time
import json
from kafka import KafkaProducer
from event_generator.generate_event import generate_event

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producer connected to Kafka. Streaming events...")

while True:
    event = generate_event()
    producer.send("music_events", event)
    print("[SENT]", event)
    time.sleep(1)
