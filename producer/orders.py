"""Order generation, kept separate from the Kafka plumbing.

Pure functions with no broker dependency, so the record-shaping logic can be unit
tested in phase 7 without a running cluster.
"""

from __future__ import annotations

import random
from typing import Iterator

# Fixed product pool from the brief. Five products with several orders each is
# what makes the per-product aggregation in phase 4 show meaningful averages.
PRODUCT_POOL = ("Item1", "Item2", "Item3", "Item4", "Item5")

# Sentinel product names that drive the consumer's failure paths.
#
# The Avro schema CANNOT express an invalid record: any record that fails
# validation would also fail serialization, so it could never reach the topic in
# the first place. Failures are therefore signalled in-band, by product name, and
# the consumer treats these two values as business-rule triggers. This is a
# deliberate, documented simulation (plan section 10), not a hack -- the brief
# defines no real downstream dependency that could fail on its own.
POISON_PRODUCT = "POISON"  # permanent failure -> straight to the DLQ
FLAKY_PRODUCT = "FLAKY"    # transient failure -> succeeds on retry

FIRST_ORDER_ID = 1001
MIN_PRICE = 5.0
MAX_PRICE = 500.0


def make_order(order_id: int, product: str, rng: random.Random) -> dict:
    """Build one Order record matching schemas/order.avsc."""
    return {
        "orderId": str(order_id),
        "product": product,
        # Rounded to 2dp to look like real currency. Note the schema field is a
        # 32-bit Avro float, so the value read back by the consumer may differ in
        # the ~7th significant digit; that precision caveat is documented in the
        # README rather than silently worked around by switching to double.
        "price": round(rng.uniform(MIN_PRICE, MAX_PRICE), 2),
    }


def generate_orders(
    count: int,
    poison_rate: float = 0.0,
    flaky_rate: float = 0.0,
    seed: int | None = None,
    start_id: int = FIRST_ORDER_ID,
) -> Iterator[dict]:
    """Yield `count` orders, some fraction of them failure-triggering.

    `seed` makes a run reproducible, which matters when rehearsing the demo: the
    same seed replays the same prices and therefore the same running average.
    """
    rng = random.Random(seed)

    for offset in range(count):
        draw = rng.random()
        # Checked in this order so that poison_rate + flaky_rate > 1.0 degrades
        # sensibly (poison wins) instead of raising.
        if draw < poison_rate:
            product = POISON_PRODUCT
        elif draw < poison_rate + flaky_rate:
            product = FLAKY_PRODUCT
        else:
            product = rng.choice(PRODUCT_POOL)

        yield make_order(start_id + offset, product, rng)
