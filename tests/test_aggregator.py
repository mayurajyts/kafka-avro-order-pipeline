"""Unit tests for the running-average aggregator (requirement R3).

No Kafka: the aggregator is a plain class fed one price at a time, which is
exactly why it was written without a broker dependency.
"""

from __future__ import annotations

import random

import pytest

from consumer.aggregator import OrderAggregator


def test_empty_aggregator_reports_zero_not_an_error():
    """The mean of no orders is undefined, but a consumer that has just started
    must still be able to log its state without raising mid-demo."""
    agg = OrderAggregator()
    assert agg.count == 0
    assert agg.running_average == 0.0
    assert agg.snapshot()["overall"]["count"] == 0
    assert agg.per_product == {}


def test_single_element_average_is_the_element():
    agg = OrderAggregator()
    assert agg.add("Item1", 42.5) == pytest.approx(42.5)
    assert agg.count == 1
    assert agg.running_average == pytest.approx(42.5)


def test_add_returns_the_running_average_after_each_message():
    """The consumer logs the value returned by add(), so it must be the average
    INCLUDING the message just added, not the previous one."""
    agg = OrderAggregator()
    assert agg.add("Item1", 10.0) == pytest.approx(10.0)
    assert agg.add("Item1", 20.0) == pytest.approx(15.0)
    assert agg.add("Item1", 30.0) == pytest.approx(20.0)


def test_incremental_average_matches_batch_recomputation():
    """The central correctness claim: the O(1) incremental update produces the
    same answer as the O(n) batch mean it replaces."""
    prices = [372.07, 369.55, 48.03, 51.38, 18.14, 212.66, 405.67, 350.58, 111.58, 50.91]
    agg = OrderAggregator()
    for p in prices:
        agg.add("Item1", p)
    assert agg.running_average == pytest.approx(sum(prices) / len(prices))


def test_incremental_average_matches_batch_over_many_random_prices():
    """Same claim at scale, where accumulated floating-point drift would show."""
    rng = random.Random(1234)
    prices = [rng.uniform(5.0, 500.0) for _ in range(10_000)]
    agg = OrderAggregator()
    for p in prices:
        agg.add("Item1", p)
    assert agg.running_average == pytest.approx(sum(prices) / len(prices), rel=1e-9)


def test_average_is_independent_of_arrival_order():
    """Kafka only guarantees ordering WITHIN a partition, so a multi-partition
    consumer sees prices interleaved differently between runs. The reported
    average must not depend on that."""
    prices = [10.0, 250.0, 3.5, 99.99, 42.0, 7.25]
    forward, backward = OrderAggregator(), OrderAggregator()
    for p in prices:
        forward.add("Item1", p)
    for p in reversed(prices):
        backward.add("Item1", p)
    assert forward.running_average == pytest.approx(backward.running_average)


def test_per_product_and_global_are_tracked_separately():
    agg = OrderAggregator()
    agg.add("Item1", 10.0)
    agg.add("Item1", 20.0)
    agg.add("Item2", 90.0)

    assert agg.count == 3
    assert agg.running_average == pytest.approx(40.0)          # (10+20+90)/3
    assert agg.per_product["Item1"].average == pytest.approx(15.0)
    assert agg.per_product["Item1"].count == 2
    assert agg.per_product["Item2"].average == pytest.approx(90.0)
    assert agg.per_product["Item2"].count == 1


def test_per_product_totals_sum_to_the_global_total():
    """Guards against a message being counted in one place but not the other."""
    agg = OrderAggregator()
    for product, price in [("A", 1.5), ("B", 2.5), ("A", 3.0), ("C", 4.0)]:
        agg.add(product, price)
    assert sum(s.total for s in agg.per_product.values()) == pytest.approx(agg.overall.total)
    assert sum(s.count for s in agg.per_product.values()) == agg.count


def test_min_and_max_are_tracked():
    agg = OrderAggregator()
    for p in [50.0, 5.0, 500.0, 100.0]:
        agg.add("Item1", p)
    assert agg.overall.minimum == pytest.approx(5.0)
    assert agg.overall.maximum == pytest.approx(500.0)


def test_new_product_appears_on_first_sighting():
    """Products are not pre-registered; the pool is discovered from the stream."""
    agg = OrderAggregator()
    assert "Surprise" not in agg.per_product
    agg.add("Surprise", 12.0)
    assert agg.per_product["Surprise"].count == 1


def test_snapshot_is_serialisable_and_rounded():
    agg = OrderAggregator()
    agg.add("Item1", 10.005)
    snap = agg.snapshot()
    assert set(snap) == {"overall", "per_product"}
    assert snap["per_product"]["Item1"]["count"] == 1
    assert isinstance(snap["overall"]["average"], float)


def test_summary_renders_for_empty_and_populated_aggregators():
    """format_summary() is printed on shutdown, including when the consumer is
    stopped before it ever received a message -- it must not raise."""
    assert "no orders processed" in OrderAggregator().format_summary()

    agg = OrderAggregator()
    agg.add("Item1", 10.0)
    out = agg.format_summary()
    assert "Item1" in out and "ALL" in out
