import os

import psycopg

from consumer.models import TransactionEvent

INSERT_SQL = """
INSERT INTO transactions
  (event_id, user_id, type, amount_minor, currency, merchant_name, merchant_category, occurred_at)
VALUES
  (%(event_id)s, %(user_id)s, %(type)s, %(amount_minor)s, %(currency)s,
   %(merchant_name)s, %(merchant_category)s, %(occurred_at)s)
ON CONFLICT (event_id) DO NOTHING
"""


def connect() -> psycopg.Connection:
    return psycopg.connect(os.getenv("DATABASE_URL", "postgresql://txn:txn@localhost:5432/txn"))


def insert_transaction(conn: psycopg.Connection, ev: TransactionEvent) -> bool:
    """Insert one event. Returns True if inserted, False if it was a duplicate."""
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, ev.model_dump())
        return cur.rowcount == 1
