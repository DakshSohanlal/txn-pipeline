"""Postgres implementation of the job's Store."""

import os
from datetime import UTC, date, datetime, time, timedelta

import psycopg

from pipeline.aggregates import DailySpend, Txn
from pipeline.anomalies import Anomaly

LOAD_SQL = """
SELECT user_id, currency, amount_minor, occurred_at, merchant_category
FROM transactions
WHERE occurred_at >= %s AND occurred_at < %s
"""

TXN_TOTALS_SQL = """
SELECT currency, SUM(amount_minor)::bigint, COUNT(*)
FROM transactions
WHERE occurred_at >= %s AND occurred_at < %s
GROUP BY currency
"""

SPEND_TOTALS_SQL = """
SELECT currency, SUM(total_minor)::bigint, SUM(txn_count)::bigint
FROM daily_spend
WHERE day = %s
GROUP BY currency
"""


class PostgresStore:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    @classmethod
    def connect(cls) -> "PostgresStore":
        url = os.getenv("DATABASE_URL", "postgresql://txn:txn@localhost:5432/txn")
        return cls(psycopg.connect(url, autocommit=True))

    def __enter__(self) -> "PostgresStore":
        return self

    def __exit__(self, *exc) -> None:
        self._conn.close()

    def load_transactions(self, start: datetime, end: datetime) -> list[Txn]:
        with self._conn.cursor() as cur:
            cur.execute(LOAD_SQL, (start, end))
            return [
                Txn(user_id=u, currency=c, amount_minor=a, occurred_at=t, merchant_category=m)
                for u, c, a, t, m in cur.fetchall()
            ]

    def save_results(
        self,
        window_start: date,
        target_day: date,
        daily_rows: list[DailySpend],
        anomalies: list[Anomaly],
    ) -> None:
        """Replace the window's daily_spend and the target day's anomalies in ONE transaction."""
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM daily_spend WHERE day BETWEEN %s AND %s", (window_start, target_day)
            )
            cur.executemany(
                "INSERT INTO daily_spend"
                " (user_id, currency, merchant_category, day, total_minor, txn_count)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (r.user_id, r.currency, r.merchant_category, r.day, r.total_minor, r.txn_count)
                    for r in daily_rows
                ],
            )
            cur.execute("DELETE FROM anomalies WHERE day = %s", (target_day,))
            cur.executemany(
                "INSERT INTO anomalies"
                " (user_id, currency, day, daily_total_minor, trailing_avg_minor, ratio)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (
                        a.user_id,
                        a.currency,
                        a.day,
                        a.daily_total_minor,
                        a.trailing_avg_minor,
                        a.ratio,
                    )
                    for a in anomalies
                ],
            )

    def transaction_totals(self, day: date) -> dict[str, tuple[int, int]]:
        """Per-currency (total_minor, count) from the raw transactions for one UTC day."""
        start = datetime.combine(day, time.min, tzinfo=UTC)
        return self._totals(TXN_TOTALS_SQL, (start, start + timedelta(days=1)))

    def daily_spend_totals(self, day: date) -> dict[str, tuple[int, int]]:
        """Per-currency (total_minor, count) from the aggregated daily_spend table."""
        return self._totals(SPEND_TOTALS_SQL, (day,))

    def _totals(self, sql: str, params: tuple) -> dict[str, tuple[int, int]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return {code: (int(total), int(count)) for code, total, count in cur.fetchall()}
