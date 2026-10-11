"""Daily batch job: transactions -> daily_spend rows + anomaly flags.

Orchestration only. The maths lives in pipeline.aggregates / pipeline.anomalies and the SQL
lives in pipeline.store, so this module is testable with a fake Store. Airflow calls
run_for_day(); you can also run it by hand:

    python -m pipeline.job --day 2026-10-09 --backfill 14

Re-running a day is safe: results for the window are replaced, never appended.
"""

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol

from pipeline.aggregates import DailySpend, Txn, compute_daily_aggregates
from pipeline.anomalies import Anomaly, detect_anomalies

WINDOW_DAYS = 30


class Store(Protocol):
    def load_transactions(self, start: datetime, end: datetime) -> list[Txn]: ...

    def save_results(
        self,
        window_start: date,
        target_day: date,
        daily_rows: list[DailySpend],
        anomalies: list[Anomaly],
    ) -> None: ...


@dataclass(frozen=True)
class JobResult:
    day: date
    transactions: int
    daily_rows: int
    anomalies: int


def run_for_day(store: Store, target_day: date, window_days: int = WINDOW_DAYS) -> JobResult:
    window_start = target_day - timedelta(days=window_days)
    start = datetime.combine(window_start, time.min, tzinfo=UTC)
    end = datetime.combine(target_day + timedelta(days=1), time.min, tzinfo=UTC)

    txns = store.load_transactions(start, end)
    daily = compute_daily_aggregates(txns)
    found = detect_anomalies(daily, target_day, window_days=window_days)
    store.save_results(window_start, target_day, daily, found)
    return JobResult(target_day, len(txns), len(daily), len(found))


def days_to_run(end_day: date, backfill: int) -> list[date]:
    """The last `backfill` days ending on end_day, oldest first."""
    return [end_day - timedelta(days=i) for i in range(backfill - 1, -1, -1)]


def main(argv: list[str] | None = None) -> None:
    from pipeline.store import PostgresStore  # imported here so tests need no database driver

    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    parser = argparse.ArgumentParser(description="Compute daily spend and anomalies.")
    parser.add_argument("--day", type=date.fromisoformat, default=yesterday)
    parser.add_argument(
        "--backfill", type=int, default=1, help="run for this many days ending at --day"
    )
    args = parser.parse_args(argv)

    with PostgresStore.connect() as store:
        for day in days_to_run(args.day, args.backfill):
            r = run_for_day(store, day)
            print(
                f"{r.day}: {r.transactions} txns -> {r.daily_rows} daily rows, "
                f"{r.anomalies} anomalies"
            )


if __name__ == "__main__":
    main()
