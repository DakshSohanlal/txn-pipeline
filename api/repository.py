"""Data access. The API depends on the Repository protocol, so tests can swap in a fake."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

import psycopg


@dataclass(frozen=True)
class SpendingRow:
    currency: str
    merchant_category: str
    total_minor: int
    txn_count: int


@dataclass(frozen=True)
class AnomalyRow:
    user_id: str
    currency: str
    day: date
    daily_total_minor: int
    trailing_avg_minor: int
    ratio: Decimal


class Repository(Protocol):
    def spending(self, user_id: str, start: datetime, end: datetime) -> list[SpendingRow]: ...

    def anomalies(
        self, day: date | None, user_id: str | None, limit: int, offset: int
    ) -> list[AnomalyRow]: ...


# Half-open range [start, end) lets Postgres use idx_txn_user_time (user_id, occurred_at).
SPENDING_SQL = """
SELECT currency,
       COALESCE(NULLIF(merchant_category, ''), 'unknown') AS merchant_category,
       SUM(amount_minor)::bigint AS total_minor,
       COUNT(*) AS txn_count
FROM transactions
WHERE user_id = %s AND occurred_at >= %s AND occurred_at < %s
GROUP BY currency, 2
ORDER BY currency, total_minor DESC
"""


class PostgresRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    def spending(self, user_id: str, start: datetime, end: datetime) -> list[SpendingRow]:
        with self._conn.cursor() as cur:
            cur.execute(SPENDING_SQL, (user_id, start, end))
            return [SpendingRow(*row) for row in cur.fetchall()]

    def anomalies(
        self, day: date | None, user_id: str | None, limit: int, offset: int
    ) -> list[AnomalyRow]:
        conditions: list[str] = []
        params: list = []
        if day is not None:
            conditions.append("day = %s")
            params.append(day)
        if user_id is not None:
            conditions.append("user_id = %s")
            params.append(user_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT user_id, currency, day, daily_total_minor, trailing_avg_minor, ratio
            FROM anomalies
            {where}
            ORDER BY day DESC, ratio DESC, user_id, currency
            LIMIT %s OFFSET %s
        """  # only fixed column names are interpolated; every value is a bound parameter
        with self._conn.cursor() as cur:
            cur.execute(sql, [*params, limit, offset])
            return [AnomalyRow(*row) for row in cur.fetchall()]
