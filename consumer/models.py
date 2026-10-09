from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictInt, StringConstraints, field_validator

# Subset of ISO 4217 codes the mock app supports.
SUPPORTED_CURRENCIES = {"GBP", "EUR", "USD", "PLN", "AED", "CHF"}
MAX_CLOCK_SKEW = timedelta(minutes=5)

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class TransactionEvent(BaseModel):
    """Contract for events on the transactions.raw topic. Amounts are integer minor units."""

    model_config = ConfigDict(extra="ignore")

    event_id: UUID
    user_id: NonEmptyStr
    type: Literal["card", "transfer"]
    amount_minor: StrictInt
    currency: str
    merchant_name: str | None = None
    merchant_category: str | None = None
    occurred_at: datetime

    @field_validator("amount_minor")
    @classmethod
    def amount_positive(cls, v: int) -> int:
        if isinstance(v, bool) or v <= 0:
            raise ValueError("amount_minor must be a positive integer")
        return v

    @field_validator("currency")
    @classmethod
    def currency_supported(cls, v: str) -> str:
        if v not in SUPPORTED_CURRENCIES:
            raise ValueError(f"unsupported currency: {v!r}")
        return v

    @field_validator("occurred_at")
    @classmethod
    def timestamp_sane(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        if v > datetime.now(UTC) + MAX_CLOCK_SKEW:
            raise ValueError("occurred_at is in the future")
        return v
