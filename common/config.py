"""Single source of truth for every broker address, port and topic name.

IMPLEMENTATION_PLAN.md section 11 forbids hardcoding these anywhere else, so
producer/, consumer/ and tools/ all import Settings from here and never read
os.environ directly. Phase 2 extends this module with the schema path and the
serializer configuration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Repository root, derived from this file's location rather than the process's
# current working directory. This is what lets `python -m producer.producer` and
# `pytest` both resolve schemas/order.avsc no matter where they are invoked from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Reads .env into the process environment if present. override=False means a
# variable already exported in the shell wins over the file, which is what lets
# the demo point at a different broker without editing any file.
load_dotenv(override=False)


def _require(name: str, default: str) -> str:
    """Read one setting, falling back to the value shipped in .env.example.

    Defaults live here rather than at each call site so that a missing .env
    degrades to a working local cluster instead of a crash mid-demo.
    """
    value = os.getenv(name, default)
    if not value:
        raise ValueError(f"Configuration {name} is set but empty")
    return value


@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of configuration. Frozen so no component can mutate
    shared config at runtime and leave another component reading a stale value."""

    bootstrap_servers: str
    schema_registry_url: str
    orders_topic: str
    dlq_topic: str
    consumer_group_id: str
    log_level: str

    # Single-broker development cluster (section 10). Kept here rather than as a
    # literal in tools/create_topics.py so the value is stated in exactly one place.
    replication_factor: int = 1
    orders_partitions: int = 3
    dlq_partitions: int = 1

    # Path to the Avro schema. Kept in config so no module hardcodes a filename.
    schema_path: Path = PROJECT_ROOT / "schemas" / "order.avsc"

    @property
    def orders_value_subject(self) -> str:
        """Schema Registry subject for the orders topic value.

        Derived, not configured: confluent-kafka's default TopicNameStrategy
        names the subject "<topic>-value". Computing it here rather than storing
        a literal guarantees this string cannot drift out of sync with the topic
        name if ORDERS_TOPIC is changed in .env.
        """
        return f"{self.orders_topic}-value"

    def load_schema_str(self) -> str:
        """Read order.avsc as text.

        The Avro serializer takes the schema as a *string*, not a parsed dict,
        because the Registry stores and compares the canonical text form.
        """
        return self.schema_path.read_text(encoding="utf-8")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=_require("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            schema_registry_url=_require("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            orders_topic=_require("ORDERS_TOPIC", "orders"),
            dlq_topic=_require("DLQ_TOPIC", "orders.DLQ"),
            consumer_group_id=_require("CONSUMER_GROUP_ID", "orders-consumer-group"),
            log_level=_require("LOG_LEVEL", "INFO"),
        )


settings = Settings.from_env()
