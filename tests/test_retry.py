"""Unit tests for the retry policy and error classification (requirement R4).

The policy takes its sleep function as a dependency, so these tests record the
backoff delays instead of actually waiting -- the whole suite runs in
milliseconds while still asserting the real 0.5s / 1s / 2s sequence.
"""

from __future__ import annotations

import random

import pytest

from consumer.failures import FailureSimulator, PermanentError, TransientError
from consumer.retry import RetryOutcome, RetryPolicy, classify


class RecordingSleep:
    """Fake clock: records requested delays instead of blocking."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def make_policy(**kwargs) -> tuple[RetryPolicy, RecordingSleep]:
    sleeper = RecordingSleep()
    kwargs.setdefault("jitter", 0.0)  # deterministic unless a test wants jitter
    kwargs.setdefault("rng", random.Random(0))
    return RetryPolicy(sleep=sleeper, **kwargs), sleeper


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

def test_transient_and_permanent_errors_are_classified():
    assert classify(TransientError("timeout")) == "transient"
    assert classify(PermanentError("bad rule")) == "permanent"


def test_real_network_errors_are_transient():
    assert classify(TimeoutError("timed out")) == "transient"
    assert classify(ConnectionError("connection reset")) == "transient"


def test_unknown_errors_default_to_permanent():
    """The safer asymmetry: an unknown error retried 3 times costs a few seconds
    and still lands in the DLQ, whereas treating a permanent error as transient
    burns the retry budget on every single message."""
    assert classify(ValueError("who knows")) == "permanent"
    assert classify(KeyError("missing")) == "permanent"


def test_validation_error_is_permanent():
    """ValidationError subclasses PermanentError so the policy needs no special
    case: retrying a negative price fails identically every time."""
    from consumer.consumer import ValidationError
    assert classify(ValidationError("price must be > 0")) == "permanent"


# --------------------------------------------------------------------------
# Attempt counts
# --------------------------------------------------------------------------

def test_success_on_first_attempt_does_not_retry_or_sleep():
    policy, sleeper = make_policy()
    outcome = policy.execute(lambda: None)
    assert outcome.succeeded and outcome.attempts == 1
    assert sleeper.delays == []
    assert outcome.should_dlq is False


def test_transient_failure_recovers_within_the_budget():
    """The phase 5 checkpoint: fail, fail, succeed on attempt 3."""
    policy, sleeper = make_policy(max_attempts=3)
    sim = FailureSimulator(flaky_attempts_needed=2)
    outcome = policy.execute(lambda: sim.process({"orderId": "A", "product": "FLAKY"}))

    assert outcome.succeeded is True
    assert outcome.attempts == 3
    assert len(sleeper.delays) == 2      # one backoff between each pair of attempts
    assert outcome.should_dlq is False


def test_permanent_failure_stops_immediately_without_backoff():
    """A permanent error must not consume the retry budget: one attempt, no sleep."""
    policy, sleeper = make_policy(max_attempts=3)
    sim = FailureSimulator()
    outcome = policy.execute(lambda: sim.process({"orderId": "B", "product": "POISON"}))

    assert outcome.succeeded is False
    assert outcome.attempts == 1
    assert outcome.classification == "permanent"
    assert sleeper.delays == []          # the key assertion: zero time wasted
    assert outcome.should_dlq is True


def test_retries_are_exhausted_when_recovery_never_happens():
    policy, sleeper = make_policy(max_attempts=3)
    sim = FailureSimulator(flaky_attempts_needed=99)   # never recovers
    outcome = policy.execute(lambda: sim.process({"orderId": "C", "product": "FLAKY"}))

    assert outcome.succeeded is False
    assert outcome.attempts == 3
    assert outcome.classification == "transient"
    assert len(sleeper.delays) == 2      # no backoff AFTER the final attempt
    assert outcome.should_dlq is True
    assert isinstance(outcome.error, TransientError)


def test_no_backoff_is_slept_after_the_final_attempt():
    """max_attempts=1 means a single try and no waiting at all."""
    policy, sleeper = make_policy(max_attempts=1)
    sim = FailureSimulator(flaky_attempts_needed=99)
    outcome = policy.execute(lambda: sim.process({"orderId": "D", "product": "FLAKY"}))
    assert outcome.attempts == 1
    assert sleeper.delays == []


def test_the_callable_is_invoked_once_per_attempt():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("not yet")

    policy, _ = make_policy(max_attempts=3)
    outcome = policy.execute(flaky)
    assert outcome.succeeded and len(calls) == 3


# --------------------------------------------------------------------------
# Backoff sequence
# --------------------------------------------------------------------------

def test_backoff_sequence_is_exponential():
    """The 0.5s / 1s / 2s progression named in the plan."""
    policy, _ = make_policy(base_delay=0.5, multiplier=2.0)
    assert policy.backoff_for(1) == pytest.approx(0.5)
    assert policy.backoff_for(2) == pytest.approx(1.0)
    assert policy.backoff_for(3) == pytest.approx(2.0)
    assert policy.backoff_for(4) == pytest.approx(4.0)


def test_a_three_attempt_run_sleeps_the_first_two_backoffs():
    policy, sleeper = make_policy(max_attempts=3, base_delay=0.5, multiplier=2.0)
    sim = FailureSimulator(flaky_attempts_needed=2)
    policy.execute(lambda: sim.process({"orderId": "E", "product": "FLAKY"}))
    assert sleeper.delays == pytest.approx([0.5, 1.0])


def test_backoff_is_capped_at_max_delay():
    """Prevents an unbounded wait on a long retry budget."""
    policy, _ = make_policy(base_delay=1.0, multiplier=10.0, max_delay=5.0)
    assert policy.backoff_for(1) == pytest.approx(1.0)
    assert policy.backoff_for(5) == pytest.approx(5.0)


def test_jitter_stays_within_its_configured_band():
    """Jitter decorrelates retries across consumers (thundering herd), but must
    not stray far enough to break the exponential shape."""
    policy, _ = make_policy(base_delay=1.0, multiplier=2.0, jitter=0.25,
                            rng=random.Random(99))
    samples = [policy.backoff_for(2) for _ in range(200)]
    assert all(1.5 <= s <= 2.5 for s in samples)     # 2.0 +/- 25%
    assert len(set(samples)) > 1                      # actually varying


def test_jitter_disabled_is_deterministic():
    policy, _ = make_policy(base_delay=0.5, jitter=0.0)
    assert {policy.backoff_for(2) for _ in range(20)} == {1.0}


def test_backoff_is_never_negative():
    policy, _ = make_policy(base_delay=0.01, jitter=1.0, rng=random.Random(3))
    assert all(policy.backoff_for(1) >= 0.0 for _ in range(200))


# --------------------------------------------------------------------------
# Outcome object
# --------------------------------------------------------------------------

def test_outcome_should_dlq_is_the_inverse_of_success():
    """The poll loop branches on this, so the relationship must be exact."""
    assert RetryOutcome(succeeded=True, attempts=1).should_dlq is False
    assert RetryOutcome(succeeded=False, attempts=3).should_dlq is True


def test_failure_simulator_tracks_orders_independently():
    """Two FLAKY orders must each get their own failure sequence rather than
    sharing one counter."""
    sim = FailureSimulator(flaky_attempts_needed=1)
    with pytest.raises(TransientError):
        sim.process({"orderId": "X", "product": "FLAKY"})
    with pytest.raises(TransientError):
        sim.process({"orderId": "Y", "product": "FLAKY"})   # not affected by X
    sim.process({"orderId": "X", "product": "FLAKY"})       # X now recovers


def test_normal_products_never_fail():
    sim = FailureSimulator()
    for product in ("Item1", "Item2", "Item3", "Item4", "Item5"):
        assert sim.process({"orderId": "1", "product": product}) is None
