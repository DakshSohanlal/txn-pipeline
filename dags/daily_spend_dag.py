"""Daily Airflow DAG: build spend aggregates and anomaly flags, then reconcile them.

This file is deliberately thin. All logic lives in pipeline/ and is unit tested without Airflow.
Written for Airflow 2.11; on Airflow 3 change the import to `from airflow.sdk import dag, task`.
"""

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="daily_spend_and_anomalies",
    description="Daily spend aggregates and 3x-baseline anomaly flags, then a reconciliation check",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,  # backfill explicitly with `airflow dags backfill` instead
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["transactions"],
)
def daily_spend_and_anomalies():
    @task
    def build_aggregates(ds=None):
        """Process the run's logical day. Idempotent: re-running replaces, never appends."""
        from pipeline.job import run_for_day
        from pipeline.store import PostgresStore

        day = date.fromisoformat(ds)
        with PostgresStore.connect() as store:
            result = run_for_day(store, day)
        print(
            f"{result.day}: {result.transactions} txns in window -> "
            f"{result.daily_rows} daily rows, {result.anomalies} anomalies"
        )
        return {
            "day": str(result.day),
            "daily_rows": result.daily_rows,
            "anomalies": result.anomalies,
        }

    @task
    def reconcile(ds=None):
        """Fail loudly if daily_spend does not add up to the raw transactions for the day."""
        from pipeline.reconcile import find_mismatches
        from pipeline.store import PostgresStore

        day = date.fromisoformat(ds)
        with PostgresStore.connect() as store:
            mismatches = find_mismatches(
                store.transaction_totals(day), store.daily_spend_totals(day)
            )
        if mismatches:
            details = "; ".join(m.describe() for m in mismatches)
            # No point retrying: the data disagrees, a human needs to look.
            raise AirflowFailException(f"Reconciliation failed for {day}: {details}")
        print(f"{day}: daily_spend reconciles with transactions")

    build_aggregates() >> reconcile()


daily_spend_and_anomalies()
