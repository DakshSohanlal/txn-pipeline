import uuid
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from consumer.db import insert_transaction
from consumer.validate import validate_event
from pipeline.reconcile import find_mismatches
from pipeline.store import PostgresStore
from tests.integration.helpers import add_txn, at, run_job, seed_spike

pytestmark = pytest.mark.integration

TARGET = date(2026, 2, 15)


def test_duplicate_event_is_stored_once(conn):
    event = validate_event(
        {
            "event_id": str(uuid.uuid4()),
            "user_id": "u1",
            "type": "card",
            "amount_minor": 1250,
            "currency": "GBP",
            "occurred_at": "2026-02-10T12:00:00+00:00",
        }
    )
    assert insert_transaction(conn, event) is True
    assert insert_transaction(conn, event) is False
    assert conn.execute("SELECT count(*) FROM transactions").fetchone()[0] == 1


def test_database_rejects_a_non_positive_amount(conn):
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO transactions"
            " (event_id, user_id, type, amount_minor, currency, occurred_at)"
            " VALUES (%s, 'u1', 'card', 0, 'GBP', now())",
            (uuid.uuid4(),),
        )


def test_load_window_includes_its_start_and_excludes_its_end(conn):
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = datetime(2026, 2, 16, tzinfo=UTC)
    add_txn(conn, start - timedelta(seconds=1), 333)  # just before the window: excluded
    add_txn(conn, start, 111)  # exactly at the start: included
    add_txn(conn, end - timedelta(seconds=1), 444)  # last second of the window: included
    add_txn(conn, end, 222)  # exactly at the end: excluded
    with PostgresStore.connect() as store:
        loaded = store.load_transactions(start, end)
    assert sorted(t.amount_minor for t in loaded) == [111, 444]


def test_job_results_are_committed_and_survive_the_connection_closing(conn, db_url):
    """Regression test for the missing-commit bug: results must be visible to OTHER connections."""
    seed_spike(conn, TARGET)

    result = run_job(TARGET)

    assert result.anomalies == 1
    with psycopg.connect(db_url) as other:  # a completely separate connection
        assert other.execute("SELECT count(*) FROM daily_spend").fetchone()[0] == 6
        row = other.execute("SELECT user_id, currency, daily_total_minor FROM anomalies").fetchone()
    assert row == ("u1", "GBP", 4000)


def test_rerunning_the_job_gives_identical_rows_without_duplicates(conn):
    seed_spike(conn, TARGET)

    run_job(TARGET)
    first = conn.execute("SELECT * FROM daily_spend ORDER BY day, user_id").fetchall()
    run_job(TARGET)
    run_job(TARGET)

    assert conn.execute("SELECT * FROM daily_spend ORDER BY day, user_id").fetchall() == first
    assert conn.execute("SELECT count(*) FROM anomalies").fetchone()[0] == 1


def test_stale_anomaly_is_removed_when_the_data_changes(conn):
    seed_spike(conn, TARGET)
    run_job(TARGET)
    assert conn.execute("SELECT count(*) FROM anomalies").fetchone()[0] == 1

    conn.execute("DELETE FROM transactions WHERE occurred_at >= %s", (at(TARGET, 0),))
    add_txn(conn, at(TARGET), 1000)  # corrected: an ordinary day
    run_job(TARGET)

    assert conn.execute("SELECT count(*) FROM anomalies").fetchone()[0] == 0


def test_reconciliation_passes_after_the_job_and_catches_tampering(conn):
    add_txn(conn, at(TARGET), 1000, currency="GBP", category="food")
    add_txn(conn, at(TARGET, 13), 2500, currency="GBP", category="transport")
    add_txn(conn, at(TARGET), 700, currency="EUR", category="food")
    run_job(TARGET)

    with PostgresStore.connect() as store:
        clean = find_mismatches(store.transaction_totals(TARGET), store.daily_spend_totals(TARGET))
    assert clean == []

    conn.execute("UPDATE daily_spend SET total_minor = total_minor + 1 WHERE currency = 'EUR'")
    with PostgresStore.connect() as store:
        mismatches = find_mismatches(
            store.transaction_totals(TARGET), store.daily_spend_totals(TARGET)
        )
    assert [m.currency for m in mismatches] == ["EUR"]
