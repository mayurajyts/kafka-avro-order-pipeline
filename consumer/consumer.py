"""Avro order consumer with real-time aggregation (requirements R1, R3).

Phase 4 scope: poll, deserialize, validate, aggregate, commit. Retry (phase 5)
and DLQ routing (phase 6) are added to the same loop later.

Usage:
    python -m consumer.consumer
    python -m consumer.consumer --summary-every 5
"""

from __future__ import annotations

import argparse
import signal
import sys

from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

from common.config import settings
from common.logging_setup import get_logger, setup_logging
from common.schema import build_avro_deserializer, build_schema_registry_client
from consumer.aggregator import OrderAggregator

log = get_logger("consumer")


class ValidationError(ValueError):
    """Record decoded cleanly but violates a business rule.

    Distinct from a deserialization failure: the bytes were fine, the CONTENT is
    not. Phase 5 classifies this as permanent -- retrying a negative price would
    fail identically every time.
    """


def validate(order: dict) -> None:
    """Business-rule checks. Raises ValidationError on the first failure."""
    if not order.get("orderId"):
        raise ValidationError("orderId is empty")
    if not order.get("product"):
        raise ValidationError("product is empty")
    price = order.get("price")
    if price is None:
        raise ValidationError("price is missing")
    if price <= 0:
        raise ValidationError(f"price must be > 0, got {price}")


def build_consumer(group_id: str | None = None) -> DeserializingConsumer:
    client = build_schema_registry_client()
    return DeserializingConsumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id or settings.consumer_group_id,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": build_avro_deserializer(client),

        # Start at the beginning of the topic when this group has no committed
        # offset, so a fresh demo run sees every message already produced.
        "auto.offset.reset": "earliest",

        # MANUAL COMMITS. Auto-commit would advance the offset on a timer,
        # independently of whether the message was actually processed -- a crash
        # mid-processing would silently skip records (at-most-once). Committing
        # explicitly after processing gives at-least-once delivery: the unit of
        # work and the offset advance are tied together.
        "enable.auto.commit": False,
    })


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Avro orders and aggregate prices.")
    parser.add_argument("--summary-every", type=int, default=10,
                        help="print the per-product table every N messages (default: 10)")
    parser.add_argument("--group-id", default=None,
                        help="override the consumer group id (default: from .env)")
    parser.add_argument("--poll-timeout", type=float, default=1.0,
                        help="seconds to block per poll (default: 1.0)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging()

    aggregator = OrderAggregator()
    consumer = build_consumer(args.group_id)
    consumer.subscribe([settings.orders_topic])

    # SIGINT flips a flag rather than raising, so the loop finishes the message in
    # hand and commits it before shutting down. Raising mid-processing could leave
    # work done but uncommitted, causing a duplicate on the next run.
    running = True

    def _stop(signum, frame):
        nonlocal running
        log.warning("shutdown signal received; finishing current message")
        running = False

    signal.signal(signal.SIGINT, _stop)

    log.info("consuming topic=%s group=%s (Ctrl+C to stop)",
             settings.orders_topic, args.group_id or settings.consumer_group_id)

    processed = 0
    failed = 0
    try:
        while running:
            msg = consumer.poll(args.poll_timeout)
            if msg is None:
                continue  # idle tick, no message available

            if msg.error():
                # PARTITION_EOF is informational, not a failure.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            order = msg.value()
            key = msg.key()

            # A tombstone (null value) is not an order; skip but still commit so
            # the offset does not stall.
            if order is None:
                log.warning("null value at partition=%d offset=%d; skipping",
                            msg.partition(), msg.offset())
                consumer.commit(asynchronous=False)
                continue

            try:
                validate(order)
                average = aggregator.add(order["product"], order["price"])
                processed += 1
                log.info(
                    "processed orderId=%-5s product=%-6s price=%7.2f | "
                    "count=%-4d running_avg=%8.2f | partition=%d offset=%d",
                    order["orderId"], order["product"], order["price"],
                    aggregator.count, average, msg.partition(), msg.offset(),
                )
            except ValidationError as exc:
                # Phase 4 only logs and moves on. Phase 6 routes this to the DLQ.
                failed += 1
                log.error("INVALID orderId=%s error=%s | partition=%d offset=%d",
                          order.get("orderId"), exc, msg.partition(), msg.offset())

            # Committed on BOTH paths. A record that can never succeed must not be
            # retried forever: leaving it uncommitted would make the consumer
            # re-read it on every restart and block the partition permanently.
            # This is the same reasoning that governs DLQ routing in phase 6.
            consumer.commit(asynchronous=False)

            if args.summary_every and processed and processed % args.summary_every == 0:
                log.info("interim summary after %d message(s):%s",
                         processed, aggregator.format_summary())
    except KeyboardInterrupt:
        # Belt and braces alongside the SIGINT handler above. On Windows, Python
        # raises KeyboardInterrupt in the main thread for CTRL_C_EVENT even when a
        # handler is installed, and if that lands while blocked inside poll() it
        # unwinds the loop directly. Catching it here means the finally block still
        # closes the consumer and prints the summary rather than dumping a
        # traceback over the final slide of the demo.
        log.warning("interrupted; shutting down")
    except KafkaException as exc:
        log.error("kafka error: %s", exc)
        return 1
    finally:
        # ORDER MATTERS. The summary is printed BEFORE close().
        #
        # On Windows, Ctrl+C delivers CTRL_C_EVENT to the whole process group,
        # including librdkafka's native threads. If that arrives while the C
        # library is shutting down inside close(), the process can be torn down
        # before any later Python statement runs -- which silently swallowed this
        # summary until the ordering was flipped. The final aggregate table is
        # demo step 8 and the visible evidence for R3, so it must not depend on a
        # clean return from native code.
        log.info("consumed=%d invalid=%d", processed, failed)
        print(aggregator.format_summary(), flush=True)

        # close() leaves the consumer group cleanly, so the coordinator reassigns
        # partitions immediately rather than waiting for the session timeout.
        try:
            consumer.close()
        except Exception:  # noqa: BLE001 - shutdown must never mask the summary
            log.warning("consumer close was interrupted", exc_info=False)

    return 0


if __name__ == "__main__":
    sys.exit(main())
