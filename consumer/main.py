"""Kafka consumer: validate -> idempotent write to Postgres -> DLQ for bad messages.

Offsets are committed only AFTER the DB transaction commits (at-least-once delivery);
ON CONFLICT DO NOTHING makes re-delivery harmless.
"""
import logging
import os

from confluent_kafka import Consumer, Producer

from consumer.db import connect, insert_transaction
from consumer.validate import InvalidEventError, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("TOPIC", "transactions.raw")
    dlq_topic = os.getenv("DLQ_TOPIC", "transactions.dlq")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "txn-consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    dlq = Producer({"bootstrap.servers": bootstrap})
    conn = connect()
    consumer.subscribe([topic])
    stats = {"inserted": 0, "duplicates": 0, "dlq": 0}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue

            try:
                event = validate_event(msg.value() or b"")
            except InvalidEventError as exc:
                dlq.produce(
                    dlq_topic,
                    value=msg.value(),
                    headers={"error_reason": exc.reason[:500].encode()},
                )
                dlq.poll(0)
                stats["dlq"] += 1
                log.warning("sent to DLQ: %s", exc.reason)
            else:
                inserted = insert_transaction(conn, event)
                conn.commit()
                stats["inserted" if inserted else "duplicates"] += 1

            consumer.commit(message=msg, asynchronous=False)
            if sum(stats.values()) % 100 == 0:
                log.info("stats: %s", stats)
    except KeyboardInterrupt:
        pass
    finally:
        dlq.flush()
        consumer.close()
        conn.close()
        log.info("final stats: %s", stats)


if __name__ == "__main__":
    main()
