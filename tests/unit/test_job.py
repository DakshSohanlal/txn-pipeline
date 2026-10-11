from datetime import UTC, date, datetime, time, timedelta

from pipeline.aggregates import Txn
from pipeline.job import days_to_run, run_for_day

TARGET = date(2026, 2, 15)


class FakeStore:
    """In-memory Store with the same replace-not-append semantics as the Postgres one."""

    def __init__(self, txns=()):
        self.txns = list(txns)
        self.daily = []
        self.anomalies = []
        self.load_args = None

    def load_transactions(self, start, end):
        self.load_args = (start, end)
        return [t for t in self.txns if start <= t.occurred_at < end]

    def save_results(self, window_start, target_day, daily_rows, anomalies):
        self.daily = [r for r in self.daily if not (window_start <= r.day <= target_day)]
        self.daily += list(daily_rows)
        self.anomalies = [a for a in self.anomalies if a.day != target_day] + list(anomalies)


def txn(day, amount, user="u1", currency="GBP"):
    ts = datetime.combine(day, time(12, 0), tzinfo=UTC)
    return Txn(user, currency, amount, ts, "food")


def history(amount=1000, days=5):
    return [txn(TARGET - timedelta(days=i + 1), amount) for i in range(days)]


def test_loads_the_30_day_window_ending_at_the_end_of_the_target_day():
    store = FakeStore()
    run_for_day(store, TARGET)
    start, end = store.load_args
    assert start == datetime(2026, 1, 16, tzinfo=UTC)
    assert end == datetime(2026, 2, 16, tzinfo=UTC)


def test_writes_daily_rows_and_flags_the_anomaly():
    store = FakeStore(history() + [txn(TARGET, 4000)])
    result = run_for_day(store, TARGET)
    assert len(store.daily) == 6
    assert [(a.user_id, a.daily_total_minor) for a in store.anomalies] == [("u1", 4000)]
    assert result.transactions == 6
    assert result.daily_rows == 6
    assert result.anomalies == 1


def test_no_anomaly_for_normal_spend():
    store = FakeStore(history() + [txn(TARGET, 1200)])
    run_for_day(store, TARGET)
    assert store.anomalies == []


def test_rerunning_is_idempotent():
    store = FakeStore(history() + [txn(TARGET, 4000)])
    run_for_day(store, TARGET)
    first = (list(store.daily), list(store.anomalies))
    run_for_day(store, TARGET)
    run_for_day(store, TARGET)
    assert (store.daily, store.anomalies) == first


def test_rerun_after_new_data_replaces_old_results_instead_of_duplicating():
    store = FakeStore(history() + [txn(TARGET, 1000)])
    run_for_day(store, TARGET)
    assert store.anomalies == []

    store.txns.append(txn(TARGET, 5000))  # late-arriving event pushes the day over 3x
    run_for_day(store, TARGET)
    target_rows = [r for r in store.daily if r.day == TARGET]
    assert len(target_rows) == 1
    assert target_rows[0].total_minor == 6000
    assert [a.daily_total_minor for a in store.anomalies] == [6000]


def test_stale_anomalies_are_cleared_when_the_day_is_no_longer_anomalous():
    store = FakeStore(history() + [txn(TARGET, 4000)])
    run_for_day(store, TARGET)
    assert len(store.anomalies) == 1

    store.txns = history() + [txn(TARGET, 1000)]  # data corrected
    run_for_day(store, TARGET)
    assert store.anomalies == []


def test_transactions_after_the_target_day_are_ignored():
    store = FakeStore(history() + [txn(TARGET, 1000), txn(TARGET + timedelta(days=1), 99_999)])
    result = run_for_day(store, TARGET)
    assert result.transactions == 6
    assert all(r.day <= TARGET for r in store.daily)


def test_empty_database_is_fine():
    store = FakeStore()
    result = run_for_day(store, TARGET)
    assert (result.transactions, result.daily_rows, result.anomalies) == (0, 0, 0)


def test_days_to_run_single_day():
    assert days_to_run(TARGET, 1) == [TARGET]


def test_days_to_run_backfill_is_oldest_first_and_ends_on_the_given_day():
    assert days_to_run(TARGET, 3) == [
        date(2026, 2, 13),
        date(2026, 2, 14),
        date(2026, 2, 15),
    ]
