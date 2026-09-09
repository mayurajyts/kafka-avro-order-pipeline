"""Create the pipeline's topics. Idempotent: safe to re-run before every demo.

Run:  python -m tools.create_topics

Topic auto-creation is disabled on the broker (see docker-compose.yml), so this
script is the only thing that defines partition counts. That is deliberate: with
auto-creation on, a typo'd topic name would silently produce to a brand-new
1-partition topic and the mistake would not surface until the demo.
"""

from __future__ import annotations

import sys

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import settings


def main() -> int:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})

    topics = [
        # 3 partitions: the message key is orderId, so Kafka's default partitioner
        # hashes each order to a fixed partition. That gives per-order ordering
        # while still allowing the consumer group to scale out to 3 members.
        NewTopic(
            settings.orders_topic,
            num_partitions=settings.orders_partitions,
            replication_factor=settings.replication_factor,
        ),
        # 1 partition: the DLQ is read by a human during triage, and a single
        # partition gives a single total ordering of failures. Volume is tiny.
        NewTopic(
            settings.dlq_topic,
            num_partitions=settings.dlq_partitions,
            replication_factor=settings.replication_factor,
        ),
    ]

    print(f"Connecting to {settings.bootstrap_servers} ...")
    futures = admin.create_topics(topics)

    exit_code = 0
    for name, future in futures.items():
        try:
            future.result()  # blocks until the broker confirms creation
            print(f"  created  {name}")
        except Exception as exc:  # noqa: BLE001 - message text is the classifier
            # TOPIC_ALREADY_EXISTS is the expected outcome on a re-run and must
            # not be an error, otherwise the script could only ever be run once.
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc):
                print(f"  exists   {name}")
            else:
                print(f"  FAILED   {name}: {exc}", file=sys.stderr)
                exit_code = 1

    metadata = admin.list_topics(timeout=10)
    print("\nTopics on the cluster:")
    for name in sorted(metadata.topics):
        if name.startswith("__"):
            continue  # skip Kafka's internal topics (__consumer_offsets etc.)
        partitions = len(metadata.topics[name].partitions)
        print(f"  {name}  (partitions={partitions})")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
