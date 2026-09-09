"""Dead Letter Queue publisher (requirement R5).

A message reaches the DLQ when the pipeline has no remaining strategy for it:
either it failed permanently, or it exhausted its retry budget. The DLQ is the
durable record of that failure, carrying enough metadata to diagnose and replay
the message WITHOUT needing the consumer's logs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from confluent_kafka import Producer

from common.config import settings
from common.logging_setup import get_logger

log = get_logger("dlq")

# Header names. The "x-" prefix marks these as application metadata rather than
# anything Kafka itself interprets. Defined as constants so the consumer that
# writes them and tools/read_dlq.py that reads them cannot drift apart.
HDR_ORIGINAL_TOPIC = "x-original-topic"
HDR_ORIGINAL_PARTITION = "x-original-partition"
HDR_ORIGINAL_OFFSET = "x-original-offset"
HDR_ERROR_CLASS = "x-error-class"
HDR_ERROR_MESSAGE = "x-error-message"
HDR_RETRY_COUNT = "x-retry-count"
HDR_FAILED_AT = "x-failed-at"

ALL_HEADERS = (
    HDR_ORIGINAL_TOPIC,
    HDR_ORIGINAL_PARTITION,
    HDR_ORIGINAL_OFFSET,
    HDR_ERROR_CLASS,
    HDR_ERROR_MESSAGE,
    HDR_RETRY_COUNT,
    HDR_FAILED_AT,
)


def build_headers(
    *,
    original_topic: str,
    original_partition: int,
    original_offset: int,
    error: BaseException,
    retry_count: int,
    failed_at: datetime | None = None,
) -> list[tuple[str, bytes]]:
    """Assemble the DLQ error metadata.

    Metadata goes in HEADERS rather than being merged into the payload for three
    reasons:
      1. The payload stays byte-identical to what the pipeline tried to process,
         so a replay tool can re-emit it to the original topic untouched.
      2. Headers can be inspected without deserializing the value -- useful when
         the value is exactly what failed to deserialize.
      3. It keeps failure metadata out of the Avro schema, which describes an
         Order, not an error.

    Kafka header values are raw bytes, so every field is encoded to UTF-8 here.
    """
    stamp = (failed_at or datetime.now(timezone.utc)).isoformat()
    return [
        # Origin coordinates: topic/partition/offset locate the exact record on
        # the source topic, which is what makes a targeted replay possible.
        (HDR_ORIGINAL_TOPIC, original_topic.encode("utf-8")),
        (HDR_ORIGINAL_PARTITION, str(original_partition).encode("utf-8")),
        (HDR_ORIGINAL_OFFSET, str(original_offset).encode("utf-8")),
        # Cause: the class name allows grouping failures by type; the message
        # carries the human-readable detail.
        (HDR_ERROR_CLASS, type(error).__name__.encode("utf-8")),
        (HDR_ERROR_MESSAGE, str(error)[:1000].encode("utf-8")),
        # How much effort was already spent. Distinguishes "failed instantly and
        # permanently" from "retried three times and never recovered".
        (HDR_RETRY_COUNT, str(retry_count).encode("utf-8")),
        (HDR_FAILED_AT, stamp.encode("utf-8")),
    ]


def build_payload(order: dict | None) -> bytes:
    """Serialise the failed record for the DLQ.

    A JSON envelope, NOT Avro, and deliberately so. The ideal is to republish the
    original Avro bytes untouched, but confluent-kafka's DeserializingConsumer
    decodes the value in place before the application sees it -- Message.value()
    returns the decoded dict and the original bytes are unrecoverable (the raw
    frame is not retained anywhere on the Message object).

    JSON is the right fallback rather than re-serialising to Avro because:
      - A message may be here precisely BECAUSE it could not be decoded against
        the schema; re-encoding it would either fail or silently alter it.
      - The DLQ is read by a human during triage. JSON is directly readable with
        no Registry lookup, which matters when the Registry may be the thing that
        failed.
      - The DLQ therefore needs no schema subject of its own.
    """
    if order is None:
        return json.dumps({"error": "value was null or could not be decoded"}).encode("utf-8")
    return json.dumps(order, default=str).encode("utf-8")


class DeadLetterQueue:
    """Publishes permanently-failed messages to the DLQ topic."""

    def __init__(self, topic: str | None = None, bootstrap_servers: str | None = None):
        self.topic = topic or settings.dlq_topic
        # A PLAIN Producer, not a SerializingProducer: the payload is already
        # bytes (JSON), so no serializer and no Registry subject are involved.
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers or settings.bootstrap_servers,
            # The DLQ is the last line of defence -- if this write is lost the
            # failure record is lost with it, so wait for full acknowledgement.
            "acks": "all",
        })
        self.published = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            # A failed DLQ write is serious: the offset is about to be committed,
            # so the message would be lost entirely. Logged at ERROR so it is
            # impossible to miss during the demo.
            log.error("DLQ DELIVERY FAILED error=%s", err)
        else:
            log.info("DLQ delivered partition=%d offset=%d", msg.partition(), msg.offset())

    def publish(
        self,
        *,
        order: dict | None,
        key: str | None,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        error: BaseException,
        retry_count: int,
    ) -> None:
        """Route one failed message to the DLQ."""
        headers = build_headers(
            original_topic=original_topic,
            original_partition=original_partition,
            original_offset=original_offset,
            error=error,
            retry_count=retry_count,
        )

        self._producer.produce(
            topic=self.topic,
            # Same key as the original, so a given orderId keeps landing on one
            # partition and its failures stay grouped and ordered.
            key=key.encode("utf-8") if key else None,
            value=build_payload(order),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        # Serve the delivery callback promptly so DLQ confirmations interleave
        # with consumption instead of arriving in a burst at shutdown.
        self._producer.poll(0)
        self.published += 1

        log.warning(
            "routed to DLQ orderId=%s error_class=%s retry_count=%d "
            "origin=%s/%d/%d",
            (order or {}).get("orderId"), type(error).__name__, retry_count,
            original_topic, original_partition, original_offset,
        )

    def flush(self, timeout: float = 10.0) -> int:
        """Block until buffered DLQ messages are delivered.

        MUST be called before the consumer exits. An undelivered DLQ message
        whose offset was already committed is a silently lost record.
        """
        return self._producer.flush(timeout)
