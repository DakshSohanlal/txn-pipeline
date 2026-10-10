from datetime import date, timedelta
from decimal import Decimal

from pipeline.aggregates import DailySpend
from pipeline.anomalies import detect_anomalies

TARGET = date(2026, 2, 15)


def spend(day, total, user="u1", currency="GBP", category="food"):
    return DailySpend(user, currency, category, day, total, 1)


def history(amounts, user="u1", currency="GBP"):
    """One row per amount, on the days immediately before TARGET (yesterday first)."""
    return [
        spend(TARGET - timedelta(days=i + 1), amt, user, currency) for i, amt in enumerate(amounts)
    ]


def test_spend_over_three_times_average_is_flagged():
    rows = history([1000] * 5) + [spend(TARGET, 3001)]
    result = detect_anomalies(rows, TARGET)
    assert [(a.user_id, a.currency) for a in result] == [("u1", "GBP")]


def test_spend_exactly_three_times_average_is_not_flagged():
    rows = history([1000] * 5) + [spend(TARGET, 3000)]
    assert detect_anomalies(rows, TARGET) == []


def test_normal_spend_is_not_flagged():
    rows = history([1000] * 5) + [spend(TARGET, 1200)]
    assert detect_anomalies(rows, TARGET) == []


def test_result_reports_baseline_and_ratio():
    rows = history([1000] * 5) + [spend(TARGET, 4000)]
    (a,) = detect_anomalies(rows, TARGET)
    assert a.day == TARGET
    assert a.daily_total_minor == 4000
    assert a.trailing_avg_minor == 1000
    assert a.ratio == Decimal("4.00")


def test_too_little_history_is_not_flagged():
    rows = history([1000] * 4) + [spend(TARGET, 100_000)]
    assert detect_anomalies(rows, TARGET) == []


def test_min_history_is_configurable():
    rows = history([1000] * 2) + [spend(TARGET, 100_000)]
    assert len(detect_anomalies(rows, TARGET, min_history_days=2)) == 1


def test_target_day_is_excluded_from_its_own_baseline():
    # If today's huge spend leaked into the baseline, the average would be inflated and hide it.
    rows = history([1000] * 5) + [spend(TARGET, 50_000)]
    (a,) = detect_anomalies(rows, TARGET)
    assert a.trailing_avg_minor == 1000


def test_history_older_than_the_window_is_ignored():
    old = spend(TARGET - timedelta(days=31), 1_000_000)
    rows = history([1000] * 5) + [old, spend(TARGET, 3001)]
    (a,) = detect_anomalies(rows, TARGET)
    assert a.trailing_avg_minor == 1000


def test_window_is_configurable():
    rows = history([1000] * 5) + [spend(TARGET, 3001)]
    assert detect_anomalies(rows, TARGET, window_days=3, min_history_days=3) != []
    assert detect_anomalies(rows, TARGET, window_days=3) == []  # only 3 days < default minimum 5


def test_future_days_are_ignored():
    future = spend(TARGET + timedelta(days=1), 999_999)
    rows = history([1000] * 5) + [future, spend(TARGET, 3001)]
    (a,) = detect_anomalies(rows, TARGET)
    assert a.trailing_avg_minor == 1000


def test_currencies_are_judged_separately():
    rows = (
        history([1000] * 5, currency="GBP")
        + history([100_000] * 5, currency="EUR")
        + [spend(TARGET, 3001, currency="GBP"), spend(TARGET, 3001, currency="EUR")]
    )
    result = detect_anomalies(rows, TARGET)
    assert [a.currency for a in result] == ["GBP"]


def test_users_are_judged_separately():
    rows = (
        history([1000] * 5, user="u1")
        + history([5000] * 5, user="u2")
        + [spend(TARGET, 4000, user="u1"), spend(TARGET, 4000, user="u2")]
    )
    result = detect_anomalies(rows, TARGET)
    assert [a.user_id for a in result] == ["u1"]


def test_categories_on_the_same_day_are_summed_before_comparing():
    rows = history([1000] * 5) + [
        spend(TARGET, 1500, category="food"),
        spend(TARGET, 1600, category="transport"),
    ]
    (a,) = detect_anomalies(rows, TARGET)
    assert a.daily_total_minor == 3100


def test_user_with_no_activity_on_target_day_is_not_flagged():
    assert detect_anomalies(history([1000] * 5), TARGET) == []


def test_custom_multiplier():
    rows = history([1000] * 5) + [spend(TARGET, 2500)]
    assert detect_anomalies(rows, TARGET) == []
    assert len(detect_anomalies(rows, TARGET, multiplier=2)) == 1


def test_results_are_sorted_by_user_then_currency():
    rows = (
        history([1000] * 5, user="u2")
        + history([1000] * 5, user="u1")
        + [spend(TARGET, 9000, user="u2"), spend(TARGET, 9000, user="u1")]
    )
    assert [a.user_id for a in detect_anomalies(rows, TARGET)] == ["u1", "u2"]


def test_empty_input():
    assert detect_anomalies([], TARGET) == []
