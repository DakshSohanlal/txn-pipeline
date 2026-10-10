import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query

from api.repository import PostgresRepository, Repository
from api.schemas import AnomalyOut, SpendingResponse
from api.service import build_spending

app = FastAPI(title="txn-pipeline API", version="0.1.0")

MAX_RANGE_DAYS = 366


def get_repo() -> Iterator[Repository]:
    conn = psycopg.connect(os.getenv("DATABASE_URL", "postgresql://txn:txn@localhost:5432/txn"))
    try:
        yield PostgresRepository(conn)
    finally:
        conn.close()


RepoDep = Annotated[Repository, Depends(get_repo)]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/users/{user_id}/spending", response_model=SpendingResponse)
def user_spending(
    user_id: str,
    repo: RepoDep,
    start: Annotated[date, Query(alias="from", description="First day, inclusive (UTC)")],
    end: Annotated[date, Query(alias="to", description="Last day, inclusive (UTC)")],
) -> SpendingResponse:
    if start > end:
        raise HTTPException(status_code=422, detail="'from' must not be after 'to'")
    if (end - start).days + 1 > MAX_RANGE_DAYS:
        raise HTTPException(status_code=422, detail=f"range must be at most {MAX_RANGE_DAYS} days")

    range_start = datetime.combine(start, time.min, tzinfo=UTC)
    range_end = datetime.combine(end + timedelta(days=1), time.min, tzinfo=UTC)
    return build_spending(user_id, start, end, repo.spending(user_id, range_start, range_end))


@app.get("/anomalies", response_model=list[AnomalyOut])
def list_anomalies(
    repo: RepoDep,
    day: date | None = None,
    user_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[AnomalyOut]:
    rows = repo.anomalies(day, user_id, limit, offset)
    return [
        AnomalyOut(
            user_id=r.user_id,
            currency=r.currency,
            day=r.day,
            daily_total_minor=r.daily_total_minor,
            trailing_avg_minor=r.trailing_avg_minor,
            ratio=float(r.ratio),
        )
        for r in rows
    ]
