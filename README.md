# StreamPulse

Deployable real-time commerce intelligence starter based on `StreamPulse_README.md`.

## Run with Docker

1. Copy `.env.example` to `.env` and change secrets for shared environments.
2. Run `docker compose up --build`.
3. Open the dashboard at http://localhost:3000 and API docs at http://localhost:8000/docs.

The local stack includes PostgreSQL, Kafka, a Kafka event producer, a streaming enrichment worker, FastAPI, and a React/Vite dashboard. The processor currently uses a deterministic risk scoring adapter so the complete flow is usable immediately; replace `enrich` in `streaming/jobs/stream_processor.py` with a trained Isolation Forest/K-Means inference adapter as models are introduced.

Useful commands:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f producer
docker compose down -v
```
