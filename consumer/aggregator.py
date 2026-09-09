"""Real-time running average of order prices (requirement R3).

Deliberately has NO Kafka dependency: it is a plain class fed one price at a
time, which is what makes it unit-testable in phase 7 without a broker.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _Stat:
    """Running statistics for one key (globally, or for one product)."""

    count: int = 0
    total: float = 0.0
    average: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    def update(self, price: float) -> None:
        self.count += 1
        self.total += price

        # --- INCREMENTAL (Welford-style) MEAN UPDATE ---------------------------
        # avg_n = avg_{n-1} + (x_n - avg_{n-1}) / n
        #
        # Why this and not sum(prices) / len(prices) over a stored list:
        #
        #   1. Memory. A Kafka topic is an UNBOUNDED stream. Retaining every price
        #      to recompute the mean would grow without limit -- after a million
        #      orders the consumer holds a million floats per product. This form
        #      keeps O(1) state per key no matter how many messages arrive.
        #   2. Time. Recomputing from a list is O(n) per message, so total work is
        #      O(n^2) over a run. This is O(1) per message, O(n) overall.
        #   3. Numerical stability. It updates by a small CORRECTION term rather
        #      than dividing one large accumulated sum, so it does not lose
        #      precision as the running total grows large relative to each price.
        #
        # That combination -- bounded memory, constant work per event -- is what
        # makes this a genuinely STREAMING computation rather than a batch one
        # that happens to be run repeatedly.
        self.average += (price - self.average) / self.count

        self.minimum = price if self.minimum is None else min(self.minimum, price)
        self.maximum = price if self.maximum is None else max(self.maximum, price)

    def as_dict(self) -> dict:
        return {
            "count": self.count,
            "total": round(self.total, 2),
            "average": round(self.average, 2),
            "min": None if self.minimum is None else round(self.minimum, 2),
            "max": None if self.maximum is None else round(self.maximum, 2),
        }


@dataclass
class OrderAggregator:
    """Maintains a global running average plus one per product."""

    overall: _Stat = field(default_factory=_Stat)
    per_product: dict[str, _Stat] = field(default_factory=dict)

    def add(self, product: str, price: float) -> float:
        """Record one order; returns the new global running average."""
        self.overall.update(price)
        # setdefault keeps the first sighting of a product and its update in one
        # step, so a product never needs to be pre-registered.
        self.per_product.setdefault(product, _Stat()).update(price)
        return self.overall.average

    @property
    def count(self) -> int:
        return self.overall.count

    @property
    def running_average(self) -> float:
        """0.0 for an empty aggregator: the mean of no orders is undefined, but
        returning 0.0 keeps the log line printable rather than raising mid-demo."""
        return self.overall.average

    def snapshot(self) -> dict:
        """Serialisable view for logging."""
        return {
            "overall": self.overall.as_dict(),
            "per_product": {p: s.as_dict() for p, s in sorted(self.per_product.items())},
        }

    def format_summary(self) -> str:
        """Fixed-width table printed on shutdown (demo step 8)."""
        lines = [
            "",
            "=" * 62,
            "FINAL AGGREGATE SUMMARY",
            "=" * 62,
            f"{'PRODUCT':<12}{'COUNT':>7}{'TOTAL':>12}{'AVERAGE':>11}{'MIN':>10}{'MAX':>10}",
            "-" * 62,
        ]
        for product, stat in sorted(self.per_product.items()):
            lines.append(
                f"{product:<12}{stat.count:>7}{stat.total:>12.2f}"
                f"{stat.average:>11.2f}{stat.minimum:>10.2f}{stat.maximum:>10.2f}"
            )
        lines.append("-" * 62)
        o = self.overall
        if o.count:
            lines.append(
                f"{'ALL':<12}{o.count:>7}{o.total:>12.2f}"
                f"{o.average:>11.2f}{o.minimum:>10.2f}{o.maximum:>10.2f}"
            )
        else:
            lines.append(f"{'ALL':<12}{0:>7}{'  (no orders processed)':>43}")
        lines.append("=" * 62)
        return "\n".join(lines)
