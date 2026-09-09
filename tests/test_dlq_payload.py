"""Unit tests for DLQ payload and header construction (requirement R5).

build_headers() and build_payload() are module-level functions with no producer
dependency, so the marks-earning metadata can be asserted without a broker.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from consumer.dlq import (
    ALL_HEADERS,
    HDR_ERROR_CLASS,
    HDR_ERROR_MESSAGE,
    HDR_FAILED_AT,
    HDR_ORIGINAL_OFFSET,
    HDR_ORIGINAL_PARTITION,
    HDR_ORIGINAL_TOPIC,
    HDR_RETRY_COUNT,
    build_headers,
    build_payload,
)
from consumer.failures import PermanentError, TransientError


def headers_as_dict(headers) -> dict[str, str]:
    return {k: v.decode("utf-8") for k, v in headers}


def sample_headers(**overrides):
    kwargs = dict(
        original_topic="orders",
        original_partition=1,
        original_offset=3,
        error=PermanentError("product 'POISON' is not sellable"),
        retry_count=1,
    )
    kwargs.update(overrides)
    return build_headers(**kwargs)


# --------------------------------------------------------------------------
# Header completeness -- the phase 6 checkpoint
# --------------------------------------------------------------------------

def test_all_seven_required_headers_are_present():
    """The checkpoint requires every header populated, not merely most of them."""
    got = headers_as_dict(sample_headers())
    assert set(got) == set(ALL_HEADERS)
    assert len(ALL_HEADERS) == 7


def test_no_header_value_is_empty():
    for name, value in sample_headers():
        assert value, f"header {name} is empty"


def test_header_values_are_bytes():
    """Kafka header values must be bytes; a str would raise at produce time."""
    for name, value in sample_headers():
        assert isinstance(value, bytes), f"header {name} is {type(value).__name__}"


# --------------------------------------------------------------------------
# Origin coordinates -- what makes a targeted replay possible
# --------------------------------------------------------------------------

def test_origin_coordinates_locate_the_original_record():
    got = headers_as_dict(sample_headers(
        original_topic="orders", original_partition=2, original_offset=41))
    assert got[HDR_ORIGINAL_TOPIC] == "orders"
    assert got[HDR_ORIGINAL_PARTITION] == "2"
    assert got[HDR_ORIGINAL_OFFSET] == "41"


def test_offset_zero_is_recorded_not_dropped():
    """Guards against a falsy-value bug: offset 0 is a real, valid offset."""
    got = headers_as_dict(sample_headers(original_offset=0, original_partition=0))
    assert got[HDR_ORIGINAL_OFFSET] == "0"
    assert got[HDR_ORIGINAL_PARTITION] == "0"


# --------------------------------------------------------------------------
# Error metadata
# --------------------------------------------------------------------------

def test_error_class_and_message_are_captured():
    got = headers_as_dict(sample_headers(error=PermanentError("not sellable")))
    assert got[HDR_ERROR_CLASS] == "PermanentError"
    assert got[HDR_ERROR_MESSAGE] == "not sellable"


def test_error_class_distinguishes_the_two_dlq_entry_paths():
    """A permanent failure and an exhausted retry both reach the DLQ, and the
    headers must tell them apart: 'never had a chance' vs 'tried hard and failed'."""
    permanent = headers_as_dict(sample_headers(
        error=PermanentError("bad product"), retry_count=1))
    exhausted = headers_as_dict(sample_headers(
        error=TransientError("downstream timeout"), retry_count=3))

    assert permanent[HDR_ERROR_CLASS] == "PermanentError"
    assert permanent[HDR_RETRY_COUNT] == "1"
    assert exhausted[HDR_ERROR_CLASS] == "TransientError"
    assert exhausted[HDR_RETRY_COUNT] == "3"


def test_long_error_messages_are_truncated():
    """An unbounded error string would inflate every DLQ record; Kafka also caps
    total message size."""
    got = headers_as_dict(sample_headers(error=ValueError("x" * 5000)))
    assert len(got[HDR_ERROR_MESSAGE]) <= 1000


def test_error_with_no_message_still_produces_a_header():
    got = headers_as_dict(sample_headers(error=PermanentError()))
    assert got[HDR_ERROR_CLASS] == "PermanentError"
    assert HDR_ERROR_MESSAGE in got


# --------------------------------------------------------------------------
# Timestamp
# --------------------------------------------------------------------------

def test_failed_at_is_iso8601_and_parseable():
    got = headers_as_dict(sample_headers())
    parsed = datetime.fromisoformat(got[HDR_FAILED_AT])
    assert parsed.tzinfo is not None, "timestamp must carry a timezone"


def test_failed_at_can_be_injected_for_deterministic_tests():
    stamp = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    got = headers_as_dict(sample_headers(failed_at=stamp))
    assert got[HDR_FAILED_AT] == stamp.isoformat()


def test_failed_at_defaults_to_utc_now():
    before = datetime.now(timezone.utc)
    got = headers_as_dict(sample_headers())
    after = datetime.now(timezone.utc)
    assert before <= datetime.fromisoformat(got[HDR_FAILED_AT]) <= after


# --------------------------------------------------------------------------
# Payload
# --------------------------------------------------------------------------

def test_payload_round_trips_the_failed_order():
    order = {"orderId": "3001", "product": "POISON", "price": 38.36}
    assert json.loads(build_payload(order).decode("utf-8")) == order


def test_payload_is_bytes():
    assert isinstance(build_payload({"orderId": "1"}), bytes)


def test_null_value_produces_a_descriptive_envelope():
    """A message may reach the DLQ precisely because its value could not be
    decoded; the payload must still say something useful."""
    payload = json.loads(build_payload(None).decode("utf-8"))
    assert "error" in payload


def test_payload_is_valid_json_for_triage_without_a_registry():
    """The DLQ is read by a human during triage, possibly while the Schema
    Registry itself is the thing that failed -- so no Registry lookup is needed."""
    raw = build_payload({"orderId": "1", "product": "Item1", "price": 1.5})
    assert json.loads(raw.decode("utf-8"))["product"] == "Item1"


def test_payload_survives_non_serialisable_values():
    """default=str keeps an unexpected type from crashing the DLQ write, which is
    the last line of defence and must not itself fail."""
    raw = build_payload({"orderId": "1", "when": datetime.now(timezone.utc)})
    assert isinstance(json.loads(raw.decode("utf-8"))["when"], str)


def test_header_names_use_the_x_prefix_convention():
    """The x- prefix marks these as application metadata rather than anything
    Kafka itself interprets."""
    assert all(name.startswith("x-") for name in ALL_HEADERS)
