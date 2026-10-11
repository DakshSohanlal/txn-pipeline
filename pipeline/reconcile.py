"""Data-quality check: do the daily aggregates add up to the raw transactions?

Pure logic, no I/O. Totals are {currency: (total_minor, txn_count)}.
"""

from dataclasses import dataclass

Totals = dict[str, tuple[int, int]]


@dataclass(frozen=True)
class Mismatch:
    currency: str
    source: tuple[int, int]
    aggregated: tuple[int, int]

    def describe(self) -> str:
        return (
            f"{self.currency}: transactions={self.source[0]} minor units / {self.source[1]} txns, "
            f"daily_spend={self.aggregated[0]} minor units / {self.aggregated[1]} txns"
        )


def find_mismatches(source: Totals, aggregated: Totals) -> list[Mismatch]:
    """Compare per-currency totals. A currency missing on one side counts as (0, 0)."""
    mismatches = []
    for currency in sorted(set(source) | set(aggregated)):
        left = source.get(currency, (0, 0))
        right = aggregated.get(currency, (0, 0))
        if left != right:
            mismatches.append(Mismatch(currency, left, right))
    return mismatches
