from datetime import date

from pydantic import BaseModel


class CategorySpend(BaseModel):
    merchant_category: str
    total_minor: int
    txn_count: int


class CurrencySpend(BaseModel):
    currency: str
    total_minor: int
    txn_count: int
    categories: list[CategorySpend]


class SpendingResponse(BaseModel):
    user_id: str
    from_date: date
    to_date: date
    currencies: list[CurrencySpend]


class AnomalyOut(BaseModel):
    user_id: str
    currency: str
    day: date
    daily_total_minor: int
    trailing_avg_minor: int
    ratio: float
