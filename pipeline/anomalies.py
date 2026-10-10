"""Pure anomaly rule. No I/O.

Rule: for each (user, currency), flag the target day if its total spend is strictly more than
`multiplier` x the user's average daily spend over the previous `window_days` days.

Definitions (kept explicit so the behaviour is unambiguous):
- Baseline = mean of daily totals over the user's *active* days (days with at least one
  transaction) in [target_day - window_days, target_day - 1]. The target day is never included.
- Users need at least `min_history_days` active days in the window, otherwise they are skipped
  (too little data to call anything unusual).
- Comparison uses integer arithmetic, so there are no floating-point edge cases at the boundary.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

from pipeline.aggregates import DailySpend


@dataclass(frozen=True)
class Anomaly:
    user_id: str
    currency: str
    day: date
    daily_total_minor: int
    trailing_avg_minor: int
    ratio: Decimal


def detect_anomalies(
    daily_spend: Iterable[DailySpend],
    target_day: date,
    *,
    window_days: int = 30,
    multiplier: int = 3,
    min_history_days: int = 5,
) -> list[Anomaly]:
    # Several category rows can exist for the same user/currency/day; sum them first.
    per_day: dict[tuple[str, str, date], int] = defaultdict(int)
    for row in daily_spend:
        per_day[(row.user_id, row.currency, row.day)] += row.total_minor

    window_start = target_day - timedelta(days=window_days)
    today: dict[tuple[str, str], int] = {}
    history: dict[tuple[str, str], list[int]] = defaultdict(list)
    for (user, currency, day), total in per_day.items():
        if day == target_day:
            today[(user, currency)] = total
        elif window_start <= day < target_day:
            history[(user, currency)].append(total)

    anomalies: list[Anomaly] = []
    for key in sorted(today):
        past = history.get(key, [])
        n = len(past)
        if n < min_history_days:
            continue
        past_sum = sum(past)
        if past_sum <= 0:
            continue
        total_today = today[key]
        # total_today > multiplier * (past_sum / n), rearranged to stay in integers
        if total_today * n > multiplier * past_sum:
            ratio = (Decimal(total_today * n) / Decimal(past_sum)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            anomalies.append(
                Anomaly(
                    user_id=key[0],
                    currency=key[1],
                    day=target_day,
                    daily_total_minor=total_today,
                    trailing_avg_minor=(past_sum + n // 2) // n,
                    ratio=ratio,
                )
            )
    return anomalies
