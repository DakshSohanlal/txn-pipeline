from datetime import date, datetime, timedelta, timezone

import pytest

from pipeline.aggregates import DailySpend, Txn, compute_daily_aggregates


def txn(user="u1", currency="GBP", amount=1000, ts="2026-01-15T12:00:00+00:00", category="food"):
    return Txn(
        user_id=user,
        currency=currency,
        amount_minor=amount,
        occurred_at=datetime.fromisoformat(ts),
        merchant_category=category,
    )


def test_empty_input_gives_empty_output():
    assert compute_daily_aggregates([]) == []


def test_single_transaction_becomes_one_row():
    assert compute_daily_aggregates([txn(amount=1250)]) == [
        DailySpend("u1", "GBP", "food", date(2026, 1, 15), 1250, 1)
    ]


def test_same_user_currency_category_day_are_summed():
    rows = compute_daily_aggregates([txn(amount=1000), txn(amount=500), txn(amount=250)])
    assert len(rows) == 1
    assert rows[0].total_minor == 1750
    assert rows[0].txn_count == 3


def test_currencies_are_never_merged():
    rows = compute_daily_aggregates(
        [txn(currency="GBP", amount=1000), txn(currency="EUR", amount=1000)]
    )
    assert {(r.currency, r.total_minor) for r in rows} == {("GBP", 1000), ("EUR", 1000)}


def test_categories_get_separate_rows():
    rows = compute_daily_aggregates([txn(category="food"), txn(category="transport")])
    assert {r.merchant_category for r in rows} == {"food", "transport"}


def test_users_get_separate_rows():
    rows = compute_daily_aggregates([txn(user="u1"), txn(user="u2")])
    assert {r.user_id for r in rows} == {"u1", "u2"}


def test_different_days_get_separate_rows():
    rows = compute_daily_aggregates(
        [txn(ts="2026-01-15T12:00:00+00:00"), txn(ts="2026-01-16T12:00:00+00:00")]
    )
    assert [r.day for r in rows] == [date(2026, 1, 15), date(2026, 1, 16)]


@pytest.mark.parametrize("category", [None, ""])
def test_missing_category_becomes_unknown(category):
    rows = compute_daily_aggregates([txn(category=category)])
    assert rows[0].merchant_category == "unknown"


def test_day_boundary_is_utc_midnight():
    rows = compute_daily_aggregates(
        [txn(ts="2026-01-15T23:59:59+00:00"), txn(ts="2026-01-16T00:00:01+00:00")]
    )
    assert [r.day for r in rows] == [date(2026, 1, 15), date(2026, 1, 16)]


def test_non_utc_timestamps_are_converted_to_utc_before_bucketing():
    # 00:30 on 16 Jan in UTC+2 is 22:30 on 15 Jan in UTC
    plus_two = timezone(timedelta(hours=2))
    t = Txn("u1", "GBP", 100, datetime(2026, 1, 16, 0, 30, tzinfo=plus_two), "food")
    assert compute_daily_aggregates([t])[0].day == date(2026, 1, 15)


def test_naive_timestamp_is_rejected():
    t = Txn("u1", "GBP", 100, datetime(2026, 1, 15, 12, 0), "food")
    with pytest.raises(ValueError):
        compute_daily_aggregates([t])


def test_output_order_is_deterministic():
    a = [txn(user="u2"), txn(user="u1"), txn(currency="EUR")]
    assert compute_daily_aggregates(a) == compute_daily_aggregates(list(reversed(a)))


def test_accepts_any_iterable():
    rows = compute_daily_aggregates(txn(amount=100) for _ in range(3))
    assert rows[0].txn_count == 3
