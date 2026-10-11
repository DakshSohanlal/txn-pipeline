"""Small helpers for integration tests. They go through the real application code paths."""

import uuid
from datetime import UTC, date, datetime, time, timedelta

from consumer.db import insert_transaction
from consumer.validate import validate_event
from pipeline.job import run_for_day
from pipeline.store import PostgresStore


def at(day: date, hour: int = 12, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=UTC)


def add_txn(conn, when, amount, user="u1", currency="GBP", category="food") -> bool:
    """Insert one transaction the same way the consumer does. Returns False for a duplicate."""
    event = validate_event(
        {
            "event_id": str(uuid.uuid4()),
            "user_id": user,
            "type": "card",
            "amount_minor": amount,
            "currency": currency,
            "merchant_name": "Test merchant",
            "merchant_category": category,
            "occurred_at": when.isoformat(),
        }
    )
    return insert_transaction(conn, event)


def seed_spike(conn, target: date, user="u1", currency="GBP", spike=4000) -> None:
    """Five normal days of 1000 before `target`, then a day of `spike` (4x the baseline)."""
    for i in range(1, 6):
        add_txn(conn, at(target - timedelta(days=i)), 1000, user=user, currency=currency)
    add_txn(conn, at(target), spike, user=user, currency=currency)


def run_job(day: date):
    """Run the batch job through PostgresStore.connect(), exactly as Airflow does."""
    with PostgresStore.connect() as store:
        return run_for_day(store, day)
