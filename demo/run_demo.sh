#!/usr/bin/env bash
#
# Guided walkthrough of the order pipeline (requirement R6).
#
# This drives the PRODUCER side of the demo and pauses between steps so the
# consumer output can be narrated. Start the consumer in a separate terminal
# first -- the script reminds you if you have not.
#
# Usage:
#   bash demo/run_demo.sh            # interactive, pauses between steps
#   bash demo/run_demo.sh --auto     # no pauses, for a recorded run
#
set -euo pipefail

cd "$(dirname "$0")/.."

AUTO=0
[[ "${1:-}" == "--auto" ]] && AUTO=1

# Resolve the interpreter: prefer the project venv, fall back to PATH so the
# script still works if the marker used a different environment layout.
if [[ -x ".venv/Scripts/python.exe" ]]; then
    PY=".venv/Scripts/python.exe"      # Windows venv
elif [[ -x ".venv/bin/python" ]]; then
    PY=".venv/bin/python"              # Linux/macOS venv
else
    PY="python"
fi

# Load .env only to display the topic names; every module reads its own config
# through common/config.py. Nothing here hardcodes a broker or topic.
ORDERS_TOPIC=$("$PY" -c "from common.config import settings; print(settings.orders_topic)")
DLQ_TOPIC=$("$PY" -c "from common.config import settings; print(settings.dlq_topic)")

bold() { printf "\n\033[1m%s\033[0m\n" "$*"; }
rule() { printf "%s\n" "======================================================================"; }

pause() {
    if [[ $AUTO -eq 1 ]]; then sleep 3; return; fi
    printf "\n  -- press ENTER for the next step --"
    read -r
}

step() {
    rule
    bold "STEP $1 — $2"
    rule
}

# ----------------------------------------------------------------------------
rule
bold "KAFKA + AVRO ORDER PIPELINE — LIVE DEMO"
rule
cat <<EOF

  Before starting, in a SEPARATE terminal run the consumer:

      $PY -m consumer.consumer

  Have the Kafka UI open at http://localhost:8080 and the Schema
  Registry at http://localhost:8081/subjects

  Topics: $ORDERS_TOPIC (main stream), $DLQ_TOPIC (failures)

EOF
pause

# ----------------------------------------------------------------------------
step 1 "Baseline — 20 clean orders (R1, R2, R3)"
cat <<EOF
  Producing 20 valid orders at 2/sec with no failures injected.

  Watch the consumer: the running average updates on EVERY message.
  Watch the Registry: subject "${ORDERS_TOPIC}-value" is now registered at
  version 1 -- the producer registered it automatically on first send.
EOF
pause
"$PY" -m producer.producer --count 20 --rate 2 --poison-rate 0

bold "Schema Registry subjects:"
curl -s http://localhost:8081/subjects || echo "  (curl unavailable -- check http://localhost:8081/subjects in a browser)"
echo
pause

# ----------------------------------------------------------------------------
step 2 "Retry — one transient failure (R4)"
cat <<EOF
  Producing ONE order with product "FLAKY". The consumer's simulated
  downstream fails it twice, then succeeds.

  Expect in the consumer:
      attempt=1/3 ... backoff=0.5s
      attempt=2/3 ... backoff=1.0s
      recovered ... attempt=3/3

  Note the timestamps: the backoff is real, and it doubles.
  Note also the running average counts this order ONCE, not three times.
EOF
pause
"$PY" -m producer.producer --count 1 --product FLAKY --start-id 7001 --rate 0
pause

# ----------------------------------------------------------------------------
step 3 "Dead Letter Queue — one permanent failure (R5)"
cat <<EOF
  Producing ONE order with product "POISON" -- a business-rule violation
  that no amount of retrying can fix.

  Expect in the consumer:
      permanent failure ... attempt=1/3     <- ONE attempt, no backoff
      routed to DLQ ...
      and then the offset is COMMITTED anyway.
EOF
pause
"$PY" -m producer.producer --count 1 --product POISON --start-id 8001 --rate 0
pause

bold "Reading the DLQ with its error metadata:"
"$PY" -m tools.read_dlq --timeout 5
pause

# ----------------------------------------------------------------------------
step 4 "Proof the poison did not block the partition"
cat <<EOF
  This is the step that proves the DLQ design works. Producing 5 more
  normal orders.

  If the poison message had blocked its partition, these would never be
  processed. Watch the consumer pick them up immediately and continue
  updating the running average.
EOF
pause
"$PY" -m producer.producer --count 5 --rate 2 --poison-rate 0 --start-id 9001
pause

# ----------------------------------------------------------------------------
step 5 "Wrap up"
cat <<EOF
  Now, in the consumer terminal, press Ctrl+C.

  It will finish the message in hand, commit, flush the DLQ producer and
  print the final aggregate summary -- global and per-product averages.

  Then show the phased commit history:

      git log --oneline

EOF
rule
bold "DEMO COMPLETE"
rule
