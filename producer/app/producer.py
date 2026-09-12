import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
RATE = max(float(os.getenv("EVENTS_PER_SECOND", "2")), 0.1)
CATEGORIES = ["Electronics", "Fashion", "Home", "Grocery"]
LOCATIONS = ["Bangalore", "Mumbai", "Delhi", "Hyderabad", "Chennai"]


def connect() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(bootstrap_servers=BROKERS, value_serializer=lambda value: json.dumps(value).encode())
        except NoBrokersAvailable:
            time.sleep(2)


def event() -> dict:
    amount = round(random.choice([249, 799, 1499, 3299, 7499, 12500]) * random.uniform(0.8, 1.3), 2)
    return {
        "event_id": f"EVT-{uuid.uuid4().hex[:10].upper()}",
        "event_type": "purchase",
        "user_id": f"U-{random.randint(1000, 1099)}",
        "product_id": f"P-{random.randint(100, 999)}",
        "amount": amount,
        "quantity": random.randint(1, 4),
        "category": random.choice(CATEGORIES),
        "location": random.choice(LOCATIONS),
        "device": random.choice(["mobile", "web", "tablet"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = connect()
    delay = 1 / RATE
    while True:
        payload = event()
        producer.send(TOPIC, key=payload["user_id"].encode(), value=payload)
        producer.flush()
        time.sleep(delay)


if __name__ == "__main__":
    main()
