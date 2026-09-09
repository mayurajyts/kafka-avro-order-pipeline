"""Avro order consumer with real-time aggregation (requirements R1, R3).

Full pipeline: poll, deserialize, validate, process under a retry policy,
aggregate, route permanent failures to the DLQ, commit.

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
from consumer.dlq import DeadLetterQueue
from consumer.failures import FailureSimulator, PermanentError
from consumer.retry import RetryPolicy

log = get_logger("consumer")


class ValidationError(PermanentError):
    """Record decoded cleanly but violates a business rule.

    Distinct from a deserialization failure: the bytes were fine, the CONTENT is
    not. Subclasses PermanentError so the retry policy classifies it without a
    special case: retrying a negative price would fail identically every time,
    so it must not consume the retry budget.
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
    parser.add_argument("--max-attempts", type=int, default=3,
                        help="attempts per message before giving up (default: 3)")
    parser.add_argument("--base-delay", type=float, default=0.5,
                        help="first retry backoff in seconds, doubled each attempt (default: 0.5)")
    parser.add_argument("--flaky-attempts", type=int, default=2,
                        help="how many times a FLAKY order fails before succeeding (default: 2)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging()

    aggregator = OrderAggregator()

    # The simulated downstream side effect, and the policy that governs retrying
    # it. Both are injected into the loop rather than constructed inside it, so
    # phase 7 can unit test the policy with a fake clock and no Kafka.
    simulator = FailureSimulator(flaky_attempts_needed=args.flaky_attempts)
    retry_policy = RetryPolicy(max_attempts=args.max_attempts, base_delay=args.base_delay)
    dlq = DeadLetterQueue()

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

            context = (f"orderId={order.get('orderId')} "
                       f"partition={msg.partition()} offset={msg.offset()}")

            def process() -> None:
                """One unit of work: validate, run the side effect, aggregate.

                Passed to the retry policy as a single callable so that a retry
                repeats the WHOLE unit. Validation is inside it because a
                ValidationError raised here is classified as permanent and so
                stops the policy immediately -- it costs no backoff.

                The aggregator is updated LAST, and only on success. If it were
                updated before the side effect, a message that failed twice and
                then succeeded would be counted three times and would corrupt the
                running average.
                """
                validate(order)
                simulator.process(order)
                aggregator.add(order["product"], order["price"])

            outcome = retry_policy.execute(process, context=context)

            if outcome.succeeded:
                processed += 1
                log.info(
                    "processed orderId=%-5s product=%-6s price=%7.2f | "
                    "count=%-4d running_avg=%8.2f | attempts=%d | partition=%d offset=%d",
                    order["orderId"], order["product"], order["price"],
                    aggregator.count, aggregator.running_average, outcome.attempts,
                    msg.partition(), msg.offset(),
                )
            else:
                # No strategy remains for this message: it either failed
                # permanently or exhausted its retries. Route it to the DLQ so the
                # failure is recorded durably, then let the offset commit below.
                failed += 1
                log.error(
                    "FAILED orderId=%s classification=%s attempts=%d error=%s | "
                    "partition=%d offset=%d",
                    order.get("orderId"), outcome.classification, outcome.attempts,
                    outcome.error, msg.partition(), msg.offset(),
                )
                dlq.publish(
                    order=order,
                    key=key,
                    original_topic=msg.topic(),
                    original_partition=msg.partition(),
                    original_offset=msg.offset(),
                    error=outcome.error,
                    retry_count=outcome.attempts,
                )

            # ================================================================
            # WHY THE OFFSET IS COMMITTED EVEN AFTER ROUTING TO THE DLQ
            # ================================================================
            # Committed on BOTH paths -- success and DLQ alike.
            #
            # Kafka offsets are a single monotonic position per partition, not a
            # per-message acknowledgement: there is no way to mark one record as
            # "done" and leave an earlier one outstanding. So NOT committing a
            # poison message does not retry just that message -- it pins the whole
            # partition at that offset. The next poll returns the same record, it
            # fails the same way, forever. That is head-of-line blocking: one bad
            # message halts every good message queued behind it on that partition.
            #
            # Routing to the DLQ is what makes committing safe. The record is not
            # discarded -- it has been durably written to orders.DLQ with the
            # metadata needed to diagnose and replay it. Responsibility for the
            # message has been TRANSFERRED, not abandoned, so the main stream is
            # free to advance.
            #
            # Ordering matters and is deliberate: DLQ publish first, commit second.
            # If the process died between them the message would be reprocessed and
            # re-sent to the DLQ -- a duplicate, which is recoverable. Committing
            # first would risk losing the record entirely if the DLQ write failed.
            # At-least-once beats at-most-once when the payload is a failure report.
            #
            # This also happens AFTER the retry policy returns, so a message
            # retried three times commits exactly ONCE, not once per attempt:
            # retries live inside the unit of work, not around it.
            # ================================================================
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
        # Flush the DLQ BEFORE anything else can interrupt shutdown. Offsets for
        # these messages are already committed, so an unflushed DLQ record would
        # be lost with no way to recover it from the source topic.
        outstanding = dlq.flush(timeout=10.0)
        if outstanding:
            log.error("DLQ flush left %d message(s) undelivered", outstanding)

        log.info("consumed=%d failed=%d dlq_published=%d", processed, failed, dlq.published)
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
