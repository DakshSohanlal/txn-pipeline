"""Pure response-shaping logic (no I/O)."""

from datetime import date

from api.repository import SpendingRow
from api.schemas import CategorySpend, CurrencySpend, SpendingResponse


def build_spending(
    user_id: str, start: date, end: date, rows: list[SpendingRow]
) -> SpendingResponse:
    by_currency: dict[str, CurrencySpend] = {}
    for r in rows:
        if r.currency not in by_currency:
            by_currency[r.currency] = CurrencySpend(
                currency=r.currency, total_minor=0, txn_count=0, categories=[]
            )
        group = by_currency[r.currency]
        group.total_minor += r.total_minor
        group.txn_count += r.txn_count
        group.categories.append(
            CategorySpend(
                merchant_category=r.merchant_category,
                total_minor=r.total_minor,
                txn_count=r.txn_count,
            )
        )
    for group in by_currency.values():
        group.categories.sort(key=lambda c: (-c.total_minor, c.merchant_category))
    return SpendingResponse(
        user_id=user_id,
        from_date=start,
        to_date=end,
        currencies=[by_currency[c] for c in sorted(by_currency)],
    )
