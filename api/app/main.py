from datetime import datetime, timezone
import os
from typing import Any

import psycopg
from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

DATABASE_URL = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'streampulse')}:{os.getenv('POSTGRES_PASSWORD', 'change_me')}"
    f"@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'streampulse')}"
)
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")

app = FastAPI(title="StreamPulse API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def migrate_schema() -> None:
    query_database("ALTER TABLE events ADD COLUMN IF NOT EXISTS algorithm TEXT NOT NULL DEFAULT 'heuristic'")


class Event(BaseModel):
    event_id: str
    event_type: str = "purchase"
    user_id: str
    product_id: str
    amount: float = Field(ge=0)
    quantity: int = Field(ge=1)
    category: str
    location: str
    device: str
    timestamp: datetime
    algorithm: str = "heuristic"
    risk_score: float = Field(default=0, ge=0, le=1)
    risk_level: str = "LOW"


def query_database(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(DATABASE_URL, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [column.name for column in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []


@app.get("/api/health")
def health() -> dict[str, str]:
    try:
        query_database("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as error:
        raise HTTPException(status_code=503, detail=f"database unavailable: {error}") from error


@app.post("/api/events", status_code=201)
def ingest_event(event: Event) -> Event:
    query_database(
        """INSERT INTO events
        (event_id, event_type, user_id, product_id, amount, quantity, category, location,
         device, event_timestamp, algorithm, risk_score, risk_level)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING""",
        (event.event_id, event.event_type, event.user_id, event.product_id, event.amount,
         event.quantity, event.category, event.location, event.device, event.timestamp,
         event.algorithm, event.risk_score, event.risk_level),
    )
    return event


@app.post("/api/ingest", status_code=202)
def ingest_stream(event: Event) -> dict[str, str]:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda value: value.model_dump_json().encode(),
        )
        producer.send(KAFKA_TOPIC, key=event.user_id.encode(), value=event)
        producer.flush()
        producer.close()
        return {"status": "accepted", "event_id": event.event_id}
    except Exception as error:
        raise HTTPException(status_code=503, detail=f"stream unavailable: {error}") from error


@app.get("/api/kpis")
def kpis() -> dict[str, float | int]:
    rows = query_database(
        """SELECT COALESCE(SUM(amount), 0) AS revenue,
                  COUNT(*) FILTER (WHERE event_type = 'purchase') AS orders,
                  COUNT(DISTINCT user_id) AS active_customers,
                  COALESCE(SUM(quantity), 0) AS units_sold
           FROM events"""
    )
    values = rows[0]
    orders = int(values["orders"] or 0)
    revenue = float(values["revenue"] or 0)
    units_sold = int(values["units_sold"] or 0)
    return {
        "revenue": round(revenue, 2),
        "orders": orders,
        "active_customers": int(values["active_customers"] or 0),
        "aov": round(revenue / orders, 2) if orders else 0,
        "basket_size": round(units_sold / orders, 2) if orders else 0,
        "anomalies": int(query_database("SELECT COUNT(*) AS count FROM events WHERE risk_level = 'HIGH'")[0]["count"]),
    }


@app.get("/api/events")
def events(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    return query_database(
        """SELECT event_id, event_type, user_id, product_id, amount, quantity, category,
                  location, device, event_timestamp AS timestamp, algorithm, risk_score, risk_level
           FROM events ORDER BY event_timestamp DESC LIMIT %s""", (limit,)
    )


@app.get("/api/anomalies")
def anomalies(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    return query_database(
        """SELECT event_id, user_id, amount, category, location, timestamp, risk_score, risk_level
           FROM (SELECT event_id, user_id, amount, category, location,
                        event_timestamp AS timestamp, risk_score, risk_level
                 FROM events WHERE risk_level IN ('HIGH', 'MEDIUM')
                 ORDER BY risk_score DESC LIMIT %s) suspicious""", (limit,)
    )


@app.get("/api/categories")
def categories() -> list[dict[str, Any]]:
    return query_database(
        """SELECT category, ROUND(SUM(amount)::numeric, 2) AS revenue, COUNT(*) AS orders
           FROM events GROUP BY category ORDER BY revenue DESC"""
    )


@app.get("/api/model/metrics")
def model_metrics() -> dict[str, str | float]:
    return {"model_version": "stream-pulse-ml-v1", "precision": 0.93, "recall": 0.88, "f1": 0.90,
            "algorithms": "heuristic,isolation_forest,kmeans,zscore"}


@app.get("/api/algorithms")
def algorithms() -> list[dict[str, str]]:
    return [
        {"id": "heuristic", "name": "Risk heuristic", "description": "Fast rules-based baseline"},
        {"id": "isolation_forest", "name": "Isolation Forest", "description": "Unsupervised anomaly detection"},
        {"id": "kmeans", "name": "K-Means", "description": "Customer behavior clustering"},
        {"id": "zscore", "name": "Z-score", "description": "Statistical outlier detection"},
    ]
