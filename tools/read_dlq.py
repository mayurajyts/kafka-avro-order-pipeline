"""Print Dead Letter Queue messages with their error metadata (evidence for R5).

Run:  python -m tools.read_dlq
      python -m tools.read_dlq --follow      # stay attached and print new failures

Reads with a random consumer group and does NOT commit offsets, so running it is
non-destructive: it can be run repeatedly during the demo and always shows the
full DLQ contents from the beginning.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from confluent_kafka import Consumer, KafkaError

from common.config import settings
from common.logging_setup import setup_logging
from consumer.dlq import ALL_HEADERS


def decode_headers(raw) -> dict[str, str]:
    """Kafka headers arrive as a list of (key, bytes) pairs."""
    if not raw:
        return {}
    return {k: (v.decode("utf-8", errors="replace") if v is not None else "") for k, v in raw}


def format_message(index: int, msg) -> str:
    headers = decode_headers(msg.headers())
    key = msg.key().decode("utf-8", errors="replace") if msg.key() else "<none>"

    try:
        payload = json.dumps(json.loads(msg.value().decode("utf-8")), indent=2)
    except Exception:  # noqa: BLE001 - a DLQ payload may be anything, including junk
        payload = repr(msg.value())

    lines = [
        "",
        "=" * 72,
        f"DLQ MESSAGE #{index}   (dlq partition={msg.partition()} offset={msg.offset()})",
        "=" * 72,
        f"  key: {key}",
        "  ---- error metadata (headers) " + "-" * 39,
    ]
    # Print the known headers in a fixed order so the output is comparable
    # between runs, then anything unexpected afterwards.
    for name in ALL_HEADERS:
        lines.append(f"  {name:<24} {headers.get(name, '<MISSING>')}")
    for name, value in headers.items():
        if name not in ALL_HEADERS:
            lines.append(f"  {name:<24} {value}   (unexpected)")

    missing = [h for h in ALL_HEADERS if h not in headers]
    lines.append("  " + "-" * 69)
    lines.append(f"  header check: {len(ALL_HEADERS) - len(missing)}/{len(ALL_HEADERS)} present"
                 + (f"  MISSING: {', '.join(missing)}" if missing else "  (all present)"))
    lines.append("  ---- original payload " + "-" * 47)
    for line in payload.splitlines():
        lines.append(f"  {line}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print messages in the DLQ topic.")
    parser.add_argument("--follow", action="store_true",
                        help="keep waiting for new DLQ messages instead of exiting")
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="seconds to wait for messages before giving up (default: 5)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging("WARNING")  # quiet: the point of this tool is its own output

    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        # A throwaway group id each run, so this tool always reads the DLQ from
        # the beginning and never interferes with the real consumer's offsets.
        "group.id": f"dlq-reader-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([settings.dlq_topic])

    print(f"Reading {settings.dlq_topic} from {settings.bootstrap_servers} ...")

    count = 0
    idle = 0.0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1.0
                if not args.follow and idle >= args.timeout:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"error: {msg.error()}", file=sys.stderr)
                break

            idle = 0.0
            count += 1
            print(format_message(count, msg))
    except KeyboardInterrupt:
        pass
    finally:
        print("\n" + "=" * 72)
        print(f"TOTAL: {count} message(s) in {settings.dlq_topic}")
        print("=" * 72)
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
