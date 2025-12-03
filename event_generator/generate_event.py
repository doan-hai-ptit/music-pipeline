from faker import Faker
import random
import uuid
from datetime import datetime, timezone

fake = Faker()

event_types = ["play", "click", "login", "skip"]
platforms = ["web", "ios", "android"]

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(event_types, weights=[0.6, 0.2, 0.1, 0.1])[0],
        "user_id": fake.uuid4(),
        "song_id": f"song_{random.randint(1, 500)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "platform": random.choice(platforms),
        "country": fake.country_code(),
        "city": fake.city(),
        "session_id": str(uuid.uuid4()),
        "duration_ms": random.choice([180000, 210000, 240000, 300000])
    }
