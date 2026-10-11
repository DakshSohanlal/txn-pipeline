from pipeline.reconcile import Mismatch, find_mismatches


def test_matching_totals_give_no_mismatches():
    totals = {"GBP": (5000, 3), "EUR": (1200, 1)}
    assert find_mismatches(totals, dict(totals)) == []


def test_both_empty_is_fine():
    assert find_mismatches({}, {}) == []


def test_amount_difference_is_reported():
    result = find_mismatches({"GBP": (5000, 3)}, {"GBP": (4900, 3)})
    assert result == [Mismatch("GBP", (5000, 3), (4900, 3))]


def test_count_difference_is_reported():
    result = find_mismatches({"GBP": (5000, 3)}, {"GBP": (5000, 2)})
    assert [m.currency for m in result] == ["GBP"]


def test_currency_missing_from_aggregates_is_reported_against_zero():
    result = find_mismatches({"GBP": (5000, 3), "EUR": (700, 1)}, {"GBP": (5000, 3)})
    assert result == [Mismatch("EUR", (700, 1), (0, 0))]


def test_currency_only_in_aggregates_is_reported_against_zero():
    result = find_mismatches({}, {"USD": (900, 2)})
    assert result == [Mismatch("USD", (0, 0), (900, 2))]


def test_mismatches_are_sorted_by_currency():
    result = find_mismatches({"USD": (1, 1), "EUR": (1, 1)}, {})
    assert [m.currency for m in result] == ["EUR", "USD"]


def test_describe_is_human_readable():
    text = Mismatch("GBP", (5000, 3), (4900, 3)).describe()
    assert "GBP" in text
    assert "5000" in text
    assert "4900" in text
