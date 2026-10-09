# txn-pipeline
![CI](https://github.com/DakshSohanlal/txn-pipeline/actions/workflows/ci.yml/badge.svg)

Event-driven transaction pipeline for a mock payments app.

**Producer → Redpanda (Kafka) → idempotent consumer (+ DLQ) → PostgreSQL → Airflow aggregates & anomaly flags → FastAPI**

> Status: ingestion layer (producer, consumer, schema, validation tests). Airflow DAG and API are next.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
cp .env.example .env
docker compose up -d                                 # Redpanda, console (localhost:8080), Postgres
pytest tests/unit                                    # validation tests

# terminal 1
python -m consumer.main
# terminal 2
python -m producer.simulate --count 500 --rate 50
```

Check results: `docker compose exec postgres psql -U txn -c "select count(*) from transactions;"`
and view the `transactions.dlq` topic in Redpanda Console.

## Design decisions
- **Money is integer minor units** (pence/cents), never floats.
- **Idempotency:** `event_id` primary key + `ON CONFLICT DO NOTHING`.
- **At-least-once delivery:** offsets committed only after the DB commit.
- **Bad messages** go to `transactions.dlq` with an `error_reason` header instead of blocking the stream.
- **Validation is a pure function** (`consumer/validate.py`) so it is fast to unit test.
