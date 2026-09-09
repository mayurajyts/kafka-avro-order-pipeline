"""Single-line structured logging, tuned for reading live during the demo.

Section 2 of the plan calls for "structured single-line records": every event fits
on one terminal line so the running average and the retry attempts stay readable
while messages scroll past.
"""

from __future__ import annotations

import logging
import sys

from common.config import settings

# Time first so events sort visually, then level, then logger name. Milliseconds
# are included because retry backoffs are sub-second and the whole point of the
# Phase 5 checkpoint is seeing the 0.5s / 1s / 2s gaps in the timestamps.
_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)-5s %(name)-18s %(message)s"
_DATEFMT = "%H:%M:%S"


def setup_logging(level: str | None = None) -> None:
    """Configure the root logger. Safe to call once per process entry point."""
    resolved = (level or settings.log_level).upper()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))

    root = logging.getLogger()
    # Replace rather than add: calling setup twice would otherwise attach a second
    # handler and print every line twice, which looks like duplicate consumption
    # during a demo and is confusing to explain.
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved)

    # librdkafka's Python wrapper is chatty at INFO and would bury the pipeline's
    # own output. Warnings and errors from the client still surface.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
