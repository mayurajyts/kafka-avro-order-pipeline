"""Schema Registry client and the Avro (de)serializers built from order.avsc.

Centralised here so the producer, the consumer and the DLQ reader all encode and
decode with the same schema text and the same Registry endpoint. This is the
module that satisfies requirement R2.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from common.config import Settings, settings


def build_schema_registry_client(cfg: Settings = settings) -> SchemaRegistryClient:
    """Client for the Registry's REST API (schema register/lookup)."""
    return SchemaRegistryClient({"url": cfg.schema_registry_url})


def order_to_dict(order: Any, ctx: Any = None) -> dict:
    """Convert an Order to the plain dict the Avro serializer expects.

    confluent-kafka calls this on every produce. Accepting a dict unchanged means
    the producer can pass either a dict or an object with the right attributes.
    """
    if isinstance(order, dict):
        return order
    return {
        "orderId": order.orderId,
        "product": order.product,
        "price": order.price,
    }


def dict_to_order(obj: dict, ctx: Any = None) -> dict:
    """Identity hook on the consume side.

    The pipeline works with plain dicts rather than a generated class: the record
    has three fields, and a dict keeps the aggregator and the DLQ payload builder
    free of any Avro-specific type. Kept as an explicit named function so the
    deserializer's contract is visible rather than a bare `lambda o, c: o`.
    """
    return obj


def build_avro_serializer(
    client: SchemaRegistryClient | None = None,
    cfg: Settings = settings,
) -> AvroSerializer:
    """Serializer for producing Orders.

    On first use this REGISTERS the schema under subject "<topic>-value" and
    prefixes every message with a 5-byte header: one magic byte 0x00 followed by
    the 4-byte schema id. That header is why a consumer can decode a message
    without being shipped a copy of the schema, and it is the evidence checked at
    the Phase 3 checkpoint.
    """
    client = client or build_schema_registry_client(cfg)
    return AvroSerializer(
        schema_registry_client=client,
        schema_str=cfg.load_schema_str(),
        to_dict=order_to_dict,
    )


def build_avro_deserializer(
    client: SchemaRegistryClient | None = None,
    cfg: Settings = settings,
) -> AvroDeserializer:
    """Deserializer for consuming Orders.

    The schema id embedded in each message is used to fetch the WRITER's schema
    from the Registry, which is then read into this READER's schema. That
    indirection is what makes schema evolution possible: an older consumer can
    still read messages written with a newer compatible schema.
    """
    client = client or build_schema_registry_client(cfg)
    return AvroDeserializer(
        schema_registry_client=client,
        schema_str=cfg.load_schema_str(),
        from_dict=dict_to_order,
    )
