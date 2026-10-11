from datetime import date

import pytest
from fastapi.testclient import TestClient

from api.main import app
from tests.integration.helpers import add_txn, at, run_job, seed_spike

pytestmark = pytest.mark.integration

TARGET = date(2026, 2, 15)


@pytest.fixture
def client():
    return TestClient(app)


def spending(client, user, start, end):
    return client.get(f"/users/{user}/spending", params={"from": start, "to": end}).json()


def test_spending_is_grouped_by_currency_and_category(conn, client):
    day = date(2026, 2, 10)
    add_txn(conn, at(day), 1000)
    add_txn(conn, at(day, 13), 500)
    add_txn(conn, at(day), 3000, category="transport")
    add_txn(conn, at(day), 700, currency="EUR")
    add_txn(conn, at(day), 999, user="u2")  # another user: must not appear

    body = spending(client, "u1", "2026-02-10", "2026-02-10")

    assert [c["currency"] for c in body["currencies"]] == ["EUR", "GBP"]
    gbp = body["currencies"][1]
    assert (gbp["total_minor"], gbp["txn_count"]) == (4500, 3)
    assert [(c["merchant_category"], c["total_minor"]) for c in gbp["categories"]] == [
        ("transport", 3000),
        ("food", 1500),
    ]


def test_spending_date_range_includes_the_whole_end_day_and_nothing_after(conn, client):
    add_txn(conn, at(date(2026, 2, 9), 23, 59), 800)  # day before: excluded
    add_txn(conn, at(date(2026, 2, 10), 0, 0), 100)  # first minute of the range: included
    add_txn(conn, at(date(2026, 2, 10), 23, 59), 200)  # last minute of the end date: included
    add_txn(conn, at(date(2026, 2, 11), 0, 0), 900)  # first minute after: excluded

    body = spending(client, "u1", "2026-02-10", "2026-02-10")

    assert body["currencies"][0]["total_minor"] == 300


def test_missing_and_empty_categories_are_reported_as_unknown(conn, client):
    day = date(2026, 2, 10)
    add_txn(conn, at(day), 100, category=None)
    add_txn(conn, at(day), 200, category="")

    body = spending(client, "u1", "2026-02-10", "2026-02-10")

    categories = body["currencies"][0]["categories"]
    assert [(c["merchant_category"], c["total_minor"]) for c in categories] == [("unknown", 300)]


def test_unknown_user_gets_an_empty_result(client):
    assert spending(client, "nobody", "2026-02-01", "2026-02-28")["currencies"] == []


def test_anomalies_endpoint_returns_what_the_job_stored(conn, client):
    seed_spike(conn, TARGET)
    run_job(TARGET)

    rows = client.get("/anomalies").json()

    assert rows == [
        {
            "user_id": "u1",
            "currency": "GBP",
            "day": "2026-02-15",
            "daily_total_minor": 4000,
            "trailing_avg_minor": 1000,
            "ratio": 4.0,
        }
    ]


def test_anomalies_filters_ordering_and_paging(conn, client):
    seed_spike(conn, TARGET, user="u1", spike=4000)  # ratio 4.0
    seed_spike(conn, TARGET, user="u2", spike=9000)  # ratio 9.0
    run_job(TARGET)

    everything = client.get("/anomalies").json()
    assert [r["user_id"] for r in everything] == ["u2", "u1"]  # highest ratio first

    assert [r["user_id"] for r in client.get("/anomalies", params={"user_id": "u1"}).json()] == [
        "u1"
    ]
    assert len(client.get("/anomalies", params={"day": "2026-02-15"}).json()) == 2
    assert client.get("/anomalies", params={"day": "2026-02-14"}).json() == []

    first_page = client.get("/anomalies", params={"limit": 1}).json()
    second_page = client.get("/anomalies", params={"limit": 1, "offset": 1}).json()
    assert [r["user_id"] for r in first_page + second_page] == ["u2", "u1"]
