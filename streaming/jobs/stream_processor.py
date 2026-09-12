import json
import os
import time

import requests
import numpy as np
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
API_URL = os.getenv("API_URL", "http://api:8000")


def connect() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BROKERS,
                group_id="streampulse-processor",
                auto_offset_reset="earliest",
                value_deserializer=lambda value: json.loads(value.decode()),
            )
        except NoBrokersAvailable:
            time.sleep(2)


def enrich(payload: dict) -> dict:
    algorithm = payload.get("algorithm", "heuristic")
    amount = payload["amount"]
    baseline = np.array([[250, 1], [800, 1], [1500, 2], [3500, 2], [7500, 3], [12000, 4]])
    point = np.array([[amount, payload["quantity"]]])
    if algorithm == "isolation_forest":
        model = IsolationForest(contamination=0.2, random_state=42).fit(baseline)
        score = float(np.clip(0.5 - model.decision_function(point)[0], 0, 1))
    elif algorithm == "kmeans":
        model = KMeans(n_clusters=3, n_init=10, random_state=42).fit(baseline)
        cluster = model.predict(point)[0]
        score = float(np.clip(np.linalg.norm(point[0] - model.cluster_centers_[cluster]) / 6000, 0, 1))
    elif algorithm == "zscore":
        score = float(np.clip(abs(amount - baseline[:, 0].mean()) / baseline[:, 0].std() / 3, 0, 1))
    else:
        score = min(1.0, amount / 15000 + (0.12 if payload["device"] == "web" else 0))
    payload["risk_score"] = round(score, 3)
    payload["algorithm"] = algorithm
    payload["risk_level"] = "HIGH" if score >= 0.85 else "MEDIUM" if score >= 0.6 else "LOW"
    return payload


def main() -> None:
    consumer = connect()
    for message in consumer:
        payload = enrich(message.value)
        for attempt in range(8):
            try:
                response = requests.post(f"{API_URL}/api/events", json=payload, timeout=5)
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 7:
                    raise
                time.sleep(2)


if __name__ == "__main__":
    main()
