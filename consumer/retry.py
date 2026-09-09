"""Exponential-backoff retry with error classification (requirement R4).

Deliberately a single reusable policy object rather than try/except scattered
through the poll loop: the loop states WHAT to do, the policy owns HOW MANY times
and HOW LONG to wait. That keeps the retry rules in one testable place (phase 7)
and out of the orchestration code.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable

from common.logging_setup import get_logger
from consumer.failures import PermanentError, TransientError

log = get_logger("retry")


def classify(exc: BaseException) -> str:
    """Sort an exception into 'transient' or 'permanent'.

    This is the decision that determines whether a message is retried or sent
    straight to the DLQ, so the rule is explicit rather than inferred.

    Default is PERMANENT for anything unrecognised. That is the safer default: an
    unknown error retried 3 times costs 3.5 seconds of the partition's throughput
    and still ends in the DLQ, whereas wrongly treating a permanent error as
    transient burns the budget on every single message. Unknown means "we have no
    evidence this will ever succeed", so we do not gamble on it.
    """
    if isinstance(exc, TransientError):
        return "transient"
    if isinstance(exc, PermanentError):
        return "permanent"

    # Real network/timeout errors from a genuine downstream would be transient.
    # Listed explicitly so the intent survives someone adding a new dependency.
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return "transient"

    return "permanent"


@dataclass
class RetryOutcome:
    """Result of running an operation under the policy.

    Returned rather than raised so the caller can branch on it without another
    try/except -- the poll loop reads `succeeded` and `should_dlq` as plain data.
    """

    succeeded: bool
    attempts: int
    error: BaseException | None = None
    classification: str | None = None

    @property
    def should_dlq(self) -> bool:
        """True when the message must be routed to the DLQ (phase 6).

        Both exhausted retries and permanent failures end here: in either case the
        pipeline has no remaining strategy for this message.
        """
        return not self.succeeded


@dataclass
class RetryPolicy:
    """Retry transient failures with exponential backoff and jitter.

    Defaults give attempt 1, then waits of 0.5s and 1.0s, then a final attempt --
    the 0.5 / 1 / 2 progression named in the plan, of which the first two waits
    are actually used by a 3-attempt budget.
    """

    max_attempts: int = 3
    base_delay: float = 0.5
    multiplier: float = 2.0
    max_delay: float = 30.0
    # Jitter spreads retries so that many consumers failing on the same downstream
    # at the same moment do not all retry in lockstep and re-overwhelm it (the
    # "thundering herd" problem). +/-25% is enough to decorrelate them.
    jitter: float = 0.25
    sleep: Callable[[float], None] = time.sleep
    rng: random.Random = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.rng is None:
            self.rng = random.Random()

    def backoff_for(self, attempt: int) -> float:
        """Delay before the retry that follows `attempt` (1-based).

        Exponential rather than fixed: if a downstream is briefly overloaded,
        retrying at a constant interval adds load at exactly the moment it is
        struggling. Doubling gives it geometrically more room to recover.
        """
        raw = self.base_delay * (self.multiplier ** (attempt - 1))
        raw = min(raw, self.max_delay)
        if self.jitter:
            spread = raw * self.jitter
            raw = self.rng.uniform(raw - spread, raw + spread)
        return max(0.0, raw)

    def execute(self, fn: Callable[[], Any], *, context: str = "") -> RetryOutcome:
        """Run `fn`, retrying transient failures up to `max_attempts` times."""
        last_exc: BaseException | None = None
        kind: str | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                fn()
                if attempt > 1:
                    log.info("recovered %s attempt=%d/%d", context, attempt, self.max_attempts)
                return RetryOutcome(succeeded=True, attempts=attempt)
            except Exception as exc:  # noqa: BLE001 - classification decides the handling
                last_exc = exc
                kind = classify(exc)

                if kind == "permanent":
                    # No backoff, no further attempts: the outcome is already known.
                    log.error("permanent failure %s attempt=%d/%d error=%s",
                              context, attempt, self.max_attempts, exc)
                    break

                if attempt == self.max_attempts:
                    log.error("retries exhausted %s attempt=%d/%d error=%s",
                              context, attempt, self.max_attempts, exc)
                    break

                delay = self.backoff_for(attempt)
                log.warning("attempt=%d/%d %s error=%s backoff=%.2fs",
                            attempt, self.max_attempts, context, exc, delay)
                self.sleep(delay)

        return RetryOutcome(
            succeeded=False,
            attempts=self.max_attempts if kind == "transient" else (
                1 if kind == "permanent" else self.max_attempts
            ),
            error=last_exc,
            classification=kind,
        )
