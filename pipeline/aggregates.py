"""Pure aggregation logic: raw transactions -> daily spend rows. No I/O.

A "day" is a UTC calendar day. Currencies are never merged, because summing GBP and EUR
amounts is meaningless without FX rates.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime

UNKNOWN_CATEGORY = "unknown"


@dataclass(frozen=True)
class Txn:
    user_id: str
    currency: str
    amount_minor: int
    occurred_at: datetime
    merchant_category: str | None = None


@dataclass(frozen=True)
class DailySpend:
    user_id: str
    currency: str
    merchant_category: str
    day: date
    total_minor: int
    txn_count: int


def utc_day(ts: datetime) -> date:
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return ts.astimezone(UTC).date()


def compute_daily_aggregates(txns: Iterable[Txn]) -> list[DailySpend]:
    """Group by (user, currency, category, UTC day). Output order is deterministic."""
    buckets: dict[tuple[str, str, str, date], list[int]] = defaultdict(lambda: [0, 0])
    for t in txns:
        category = t.merchant_category or UNKNOWN_CATEGORY
        key = (t.user_id, t.currency, category, utc_day(t.occurred_at))
        buckets[key][0] += t.amount_minor
        buckets[key][1] += 1
    return [
        DailySpend(user, currency, category, day, total, count)
        for (user, currency, category, day), (total, count) in sorted(buckets.items())
    ]
