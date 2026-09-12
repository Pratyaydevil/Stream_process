import os
import time

import numpy as np
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
API_URL = os.getenv("API_URL", "http://api:8000")

schema = StructType([
    StructField("event_id", StringType()), StructField("event_type", StringType()),
    StructField("user_id", StringType()), StructField("product_id", StringType()),
    StructField("amount", DoubleType()), StructField("quantity", IntegerType()),
    StructField("category", StringType()), StructField("location", StringType()),
    StructField("device", StringType()), StructField("timestamp", TimestampType()),
    StructField("algorithm", StringType()),
])


def score(payload: dict) -> dict:
    algorithm = payload.get("algorithm") or "heuristic"
    amount = float(payload["amount"])
    quantity = int(payload["quantity"])
    baseline = np.array([[250, 1], [800, 1], [1500, 2], [3500, 2], [7500, 3], [12000, 4]])
    point = np.array([[amount, quantity]])
    if algorithm == "isolation_forest":
        model = IsolationForest(contamination=0.2, random_state=42).fit(baseline)
        value = float(np.clip(0.5 - model.decision_function(point)[0], 0, 1))
    elif algorithm == "kmeans":
        model = KMeans(n_clusters=3, n_init=10, random_state=42).fit(baseline)
        cluster = model.predict(point)[0]
        value = float(np.clip(np.linalg.norm(point[0] - model.cluster_centers_[cluster]) / 6000, 0, 1))
    elif algorithm == "zscore":
        value = float(np.clip(abs(amount - baseline[:, 0].mean()) / baseline[:, 0].std() / 3, 0, 1))
    else:
        value = min(1.0, amount / 15000 + (0.12 if payload.get("device") == "web" else 0))
    payload["algorithm"] = algorithm
    payload["risk_score"] = round(value, 3)
    payload["risk_level"] = "HIGH" if value >= 0.85 else "MEDIUM" if value >= 0.6 else "LOW"
    return payload


def write_batch(batch, _batch_id: int) -> None:
    for row in batch.toLocalIterator():
        payload = row.asDict(recursive=True)
        payload["timestamp"] = payload["timestamp"].isoformat()
        enriched = score(payload)
        for attempt in range(8):
            try:
                response = requests.post(f"{API_URL}/api/events", json=enriched, timeout=5)
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 7:
                    raise
                time.sleep(2)


spark = SparkSession.builder.appName("StreamPulseStructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)
events = raw.select(from_json(col("value").cast("string"), schema).alias("event")).select("event.*")
query = events.writeStream.foreachBatch(write_batch).option("checkpointLocation", "/tmp/streampulse-checkpoint").start()
query.awaitTermination()
