"""Fixtures for tests that need a real PostgreSQL.

Each test session creates a throwaway database from db/schema.sql and drops it afterwards, so
your development data is never touched. Locally:  docker compose up -d postgres
Set REQUIRE_DB=1 (CI does) to fail instead of skip when PostgreSQL is not reachable.
"""

import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

ADMIN_URL = os.getenv("TEST_DATABASE_ADMIN_URL", "postgresql://txn:txn@localhost:5432/postgres")
SCHEMA = Path(__file__).resolve().parents[2] / "db" / "schema.sql"


@pytest.fixture(scope="session")
def db_url():
    try:
        admin = psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        if os.getenv("REQUIRE_DB"):
            raise
        pytest.skip(f"PostgreSQL not reachable ({exc}). Start it: docker compose up -d postgres")

    name = f"txn_test_{uuid.uuid4().hex[:12]}"
    admin.execute(f'CREATE DATABASE "{name}"')
    url = make_conninfo(ADMIN_URL, dbname=name)
    with psycopg.connect(url, autocommit=True) as setup:
        setup.execute(SCHEMA.read_text())
    try:
        yield url
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


@pytest.fixture(autouse=True)
def _point_app_at_test_db(db_url, monkeypatch):
    """The consumer, job and API all read DATABASE_URL, so they now hit the test database."""
    monkeypatch.setenv("DATABASE_URL", db_url)


@pytest.fixture(autouse=True)
def _clean_tables(db_url):
    with psycopg.connect(db_url, autocommit=True) as c:
        c.execute("TRUNCATE transactions, daily_spend, anomalies")


@pytest.fixture
def conn(db_url):
    with psycopg.connect(db_url, autocommit=True) as c:
        yield c
