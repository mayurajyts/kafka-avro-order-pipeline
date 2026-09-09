"""Injectable failure simulator for the demo (plan section 10).

The brief defines no real downstream dependency, and the Avro schema cannot
express an invalid record -- anything that failed validation would also fail
serialization and never reach the topic. So failures are simulated in-band, by
product name, and injected here rather than hidden inside the consumer loop.

Keeping this in its own module means the consumer's processing step depends on an
abstract "side effect that may fail" rather than on demo scaffolding, and phase 7
can unit test the retry policy by swapping in a different simulator.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from producer.orders import FLAKY_PRODUCT, POISON_PRODUCT


class TransientError(Exception):
    """A failure that may succeed if the same work is attempted again.

    Models a downstream timeout, a dropped connection, a momentarily unavailable
    database -- the state that caused it is expected to change on its own.
    """


class PermanentError(Exception):
    """A failure that will recur identically no matter how often it is retried.

    Models a business-rule violation or malformed content. Retrying is pure cost:
    the same input through the same code yields the same failure, so these go
    straight to the DLQ (phase 6) without consuming the retry budget.
    """


@dataclass
class FailureSimulator:
    """Simulates a downstream side effect that can fail.

    `flaky_attempts_needed` controls how many times a FLAKY order fails before
    succeeding. The default of 2 means attempt 1 and attempt 2 fail and attempt 3
    succeeds, which exercises the full 3-attempt budget and produces the
    "attempt 1 -> backoff -> attempt 2 -> backoff -> attempt 3 -> success" trace
    that the phase 5 checkpoint calls for.
    """

    flaky_attempts_needed: int = 2
    # Per-order attempt counter. Keyed by orderId so two different FLAKY orders
    # each get their own independent failure sequence rather than sharing one.
    _attempts: dict[str, int] = field(default_factory=dict)

    def process(self, order: dict) -> None:
        """The simulated side effect. Returns None on success, raises on failure."""
        product = order.get("product")
        order_id = order.get("orderId", "<unknown>")

        if product == POISON_PRODUCT:
            # Permanent by construction: no number of retries changes the product
            # name, so this fails identically forever.
            raise PermanentError(
                f"business rule violation: product '{POISON_PRODUCT}' is not sellable"
            )

        if product == FLAKY_PRODUCT:
            seen = self._attempts.get(order_id, 0) + 1
            self._attempts[order_id] = seen
            if seen <= self.flaky_attempts_needed:
                raise TransientError(
                    f"simulated downstream timeout (attempt {seen} of "
                    f"{self.flaky_attempts_needed + 1} before recovery)"
                )
            # Fall through: the downstream dependency has "recovered".

        return None

    def reset(self) -> None:
        """Clear attempt history; used between unit tests."""
        self._attempts.clear()
