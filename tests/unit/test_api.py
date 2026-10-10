from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from api.main import app, get_repo
from api.repository import AnomalyRow, SpendingRow


@dataclass
class FakeRepo:
    """In-memory stand-in for Postgres, so API tests need no database."""

    spending_rows: list[SpendingRow] = field(default_factory=list)
    anomaly_rows: list[AnomalyRow] = field(default_factory=list)
    calls: dict = field(default_factory=dict)

    def spending(self, user_id, start, end):
        self.calls["spending"] = (user_id, start, end)
        return self.spending_rows

    def anomalies(self, day, user_id, limit, offset):
        self.calls["anomalies"] = {"day": day, "user_id": user_id, "limit": limit, "offset": offset}
        return self.anomaly_rows


@pytest.fixture
def repo():
    return FakeRepo()


@pytest.fixture
def client(repo):
    app.dependency_overrides[get_repo] = lambda: repo
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---- /users/{id}/spending ----


def test_spending_groups_by_currency_and_category(client, repo):
    repo.spending_rows = [
        SpendingRow("GBP", "food", 1000, 2),
        SpendingRow("GBP", "transport", 3000, 1),
        SpendingRow("EUR", "food", 500, 1),
    ]
    r = client.get("/users/user_001/spending", params={"from": "2026-01-01", "to": "2026-01-31"})
    assert r.status_code == 200
    body = r.json()
    assert body["user_id"] == "user_001"
    assert [c["currency"] for c in body["currencies"]] == ["EUR", "GBP"]
    gbp = body["currencies"][1]
    assert gbp["total_minor"] == 4000
    assert gbp["txn_count"] == 3
    # biggest category first
    assert [c["merchant_category"] for c in gbp["categories"]] == ["transport", "food"]


def test_spending_date_range_is_inclusive_of_the_end_date(client, repo):
    client.get("/users/u1/spending", params={"from": "2026-01-01", "to": "2026-01-30"})
    user_id, start, end = repo.calls["spending"]
    assert user_id == "u1"
    assert start == datetime(2026, 1, 1, tzinfo=UTC)
    # half-open range: "< 31 Jan 00:00" covers all of 30 Jan
    assert end == datetime(2026, 1, 31, tzinfo=UTC)


def test_spending_single_day_range_is_allowed(client):
    r = client.get("/users/u1/spending", params={"from": "2026-01-01", "to": "2026-01-01"})
    assert r.status_code == 200


def test_spending_for_unknown_user_is_an_empty_result_not_an_error(client):
    r = client.get("/users/nobody/spending", params={"from": "2026-01-01", "to": "2026-01-31"})
    assert r.status_code == 200
    assert r.json()["currencies"] == []


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"from": "2026-01-01"},
        {"to": "2026-01-31"},
        {"from": "not-a-date", "to": "2026-01-31"},
        {"from": "2026-02-01", "to": "2026-01-01"},  # start after end
        {"from": "2025-01-01", "to": "2026-12-31"},  # range too long
    ],
)
def test_spending_rejects_bad_date_params(client, params):
    r = client.get("/users/u1/spending", params=params)
    assert r.status_code == 422


# ---- /anomalies ----


def test_anomalies_returns_rows(client, repo):
    repo.anomaly_rows = [AnomalyRow("u1", "GBP", date(2026, 2, 15), 4000, 1000, Decimal("4.00"))]
    r = client.get("/anomalies")
    assert r.status_code == 200
    assert r.json() == [
        {
            "user_id": "u1",
            "currency": "GBP",
            "day": "2026-02-15",
            "daily_total_minor": 4000,
            "trailing_avg_minor": 1000,
            "ratio": 4.0,
        }
    ]


def test_anomalies_empty(client):
    assert client.get("/anomalies").json() == []


def test_anomalies_defaults_and_filters_are_passed_to_the_repository(client, repo):
    client.get("/anomalies")
    assert repo.calls["anomalies"] == {"day": None, "user_id": None, "limit": 50, "offset": 0}

    params = {"day": "2026-02-15", "user_id": "u1", "limit": 10, "offset": 20}
    client.get("/anomalies", params=params)
    assert repo.calls["anomalies"] == {
        "day": date(2026, 2, 15),
        "user_id": "u1",
        "limit": 10,
        "offset": 20,
    }


@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 201}, {"offset": -1}, {"day": "yesterday"}],
)
def test_anomalies_rejects_bad_params(client, params):
    assert client.get("/anomalies", params=params).status_code == 422
