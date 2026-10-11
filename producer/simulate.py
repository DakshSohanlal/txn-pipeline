"""Simulates card/transfer transactions and publishes them to Kafka (Redpanda).

Deliberately injects duplicates and malformed events so the consumer's
idempotency and dead-letter handling can be demonstrated.

Usage:  python -m producer.simulate --count 500 --rate 20
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

CURRENCIES = ["GBP", "EUR", "USD", "PLN"]
MERCHANTS = [
    ("Tesco", "groceries"),
    ("Pret A Manger", "food"),
    ("Uber", "transport"),
    ("Netflix", "entertainment"),
    ("Amazon", "shopping"),
    ("Shell", "fuel"),
]


def make_event(user_count: int = 20, days_back: int = 30, spike: bool = False) -> dict:
    """One random transaction. spike=True makes it an unusually large purchase."""
    merchant, category = random.choice(MERCHANTS)
    occurred = datetime.now(UTC) - timedelta(
        days=random.randint(0, days_back), seconds=random.randint(0, 86_000)
    )
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, user_count):03d}",
        "type": random.choice(["card", "card", "card", "transfer"]),
        "amount_minor": (
            random.randint(30_000, 150_000)  # 300-1500 in major units: should trip the 3x rule
            if spike
            else max(1, int(random.lognormvariate(7, 1)))  # ~ pence; heavy tail
        ),
        "currency": random.choices(CURRENCIES, weights=[6, 2, 1, 1])[0],
        "merchant_name": merchant,
        "merchant_category": category,
        "occurred_at": min(occurred, datetime.now(UTC)).isoformat(),
    }


def corrupt(event: dict) -> bytes | dict:
    """Return a deliberately bad message."""
    kind = random.choice(["negative", "bad_currency", "missing", "garbage"])
    if kind == "negative":
        return {**event, "amount_minor": -abs(event["amount_minor"])}
    if kind == "bad_currency":
        return {**event, "currency": "gbp"}
    if kind == "missing":
        return {k: v for k, v in event.items() if k != "user_id"}
    return b"{this is not json"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=200)
    p.add_argument("--rate", type=float, default=20, help="events per second")
    p.add_argument("--dup-rate", type=float, default=0.02)
    p.add_argument("--bad-rate", type=float, default=0.02)
    p.add_argument(
        "--spike-rate", type=float, default=0.005, help="share of events that are large purchases"
    )
    args = p.parse_args()

    topic = os.getenv("TOPIC", "transactions.raw")
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")})

    def send(payload: bytes | dict, key: str) -> None:
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        producer.produce(topic, value=data, key=key)
        producer.poll(0)

    sent = dups = bad = 0
    for _ in range(args.count):
        ev = make_event(spike=random.random() < args.spike_rate)
        send(ev, ev["user_id"])
        sent += 1
        if random.random() < args.dup_rate:
            send(ev, ev["user_id"])  # same event_id again
            dups += 1
        if random.random() < args.bad_rate:
            send(corrupt(make_event()), "bad")
            bad += 1
        time.sleep(1 / args.rate)

    producer.flush()
    print(f"published {sent} events (+{dups} duplicates, +{bad} malformed) to {topic}")


if __name__ == "__main__":
    main()
