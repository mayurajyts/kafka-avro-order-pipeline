"""Avro order producer (requirements R1, R2).

Usage:
    python -m producer.producer --count 20 --rate 2
    python -m producer.producer --count 1 --product FLAKY     # demo retry
    python -m producer.producer --count 1 --product POISON    # demo DLQ
"""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from common.config import settings
from common.logging_setup import get_logger, setup_logging
from common.schema import build_avro_serializer, build_schema_registry_client
from producer.orders import (
    FIRST_ORDER_ID,
    FLAKY_PRODUCT,
    POISON_PRODUCT,
    PRODUCT_POOL,
    generate_orders,
    make_order,
)

log = get_logger("producer")


def delivery_report(err, msg) -> None:
    """Called once per message when the broker acks it (or the send fails).

    Produce is asynchronous: `produce()` only enqueues into librdkafka's internal
    buffer and returns immediately. This callback is the ONLY place a delivery is
    actually confirmed, which is why the partition and offset are logged here and
    not next to the produce call -- at produce time they do not yet exist.
    """
    if err is not None:
        log.error("delivery FAILED error=%s", err)
        return
    key = msg.key().decode("utf-8") if msg.key() else "<none>"
    log.info(
        "delivered orderId=%s topic=%s partition=%d offset=%d",
        key, msg.topic(), msg.partition(), msg.offset(),
    )


def build_producer() -> SerializingProducer:
    """SerializingProducer wired to the Registry-backed Avro serializer."""
    client = build_schema_registry_client()
    return SerializingProducer({
        "bootstrap.servers": settings.bootstrap_servers,
        # Key is the orderId as a plain string. Kafka's default partitioner hashes
        # the key, so every event carrying the same orderId lands on the same
        # partition and therefore keeps its relative order. An Avro-encoded key
        # would work too but would add a second Registry subject for no benefit.
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": build_avro_serializer(client),
        # Wait for the full in-sync replica set to acknowledge. On this
        # single-broker cluster that is one replica, but 'all' is the setting a
        # production cluster needs and costs nothing to state correctly here.
        "acks": "all",
    })


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce Avro order messages.")
    parser.add_argument("--count", type=int, default=100,
                        help="number of orders to produce (default: 100)")
    parser.add_argument("--rate", type=float, default=5.0,
                        help="messages per second; 0 = as fast as possible (default: 5)")
    parser.add_argument("--poison-rate", type=float, default=0.1,
                        help="fraction of orders sent as POISON (default: 0.1)")
    parser.add_argument("--flaky-rate", type=float, default=0.0,
                        help="fraction of orders sent as FLAKY (default: 0.0)")
    parser.add_argument("--product", choices=[*PRODUCT_POOL, POISON_PRODUCT, FLAKY_PRODUCT],
                        help="force every order to this product; overrides the rate flags. "
                             "Used in the demo to send one FLAKY or one POISON order.")
    parser.add_argument("--start-id", type=int, default=FIRST_ORDER_ID,
                        help=f"first orderId (default: {FIRST_ORDER_ID})")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed; makes prices reproducible across runs")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging()

    if not 0.0 <= args.poison_rate <= 1.0 or not 0.0 <= args.flaky_rate <= 1.0:
        log.error("--poison-rate and --flaky-rate must be between 0.0 and 1.0")
        return 2

    producer = build_producer()

    if args.product:
        # Forced-product mode: deterministic single-product run for the demo.
        import random
        rng = random.Random(args.seed)
        orders = (make_order(args.start_id + i, args.product, rng) for i in range(args.count))
    else:
        orders = generate_orders(
            count=args.count,
            poison_rate=args.poison_rate,
            flaky_rate=args.flaky_rate,
            seed=args.seed,
            start_id=args.start_id,
        )

    # rate=0 means no throttle; otherwise sleep this long between produces.
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    log.info(
        "producing count=%d rate=%s topic=%s poison_rate=%.2f flaky_rate=%.2f",
        args.count, args.rate or "unthrottled", settings.orders_topic,
        0.0 if args.product else args.poison_rate,
        0.0 if args.product else args.flaky_rate,
    )

    sent = 0
    try:
        for order in orders:
            producer.produce(
                topic=settings.orders_topic,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )
            # Serves delivery callbacks for messages already acked. Without this
            # the callback queue would only drain at flush(), so the demo would
            # show every "delivered" line in a burst at the very end instead of
            # interleaved with production.
            producer.poll(0)
            sent += 1
            log.info("queued    orderId=%s product=%-6s price=%.2f",
                     order["orderId"], order["product"], order["price"])
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        log.warning("interrupted after %d message(s); flushing", sent)
    finally:
        # Blocks until every buffered message is delivered or fails. Skipping this
        # would silently drop in-flight messages when the process exits.
        remaining = producer.flush(timeout=30)
        if remaining:
            log.error("flush timed out with %d message(s) undelivered", remaining)
            return 1

    log.info("done: %d message(s) produced to %s", sent, settings.orders_topic)
    return 0


if __name__ == "__main__":
    sys.exit(main())
