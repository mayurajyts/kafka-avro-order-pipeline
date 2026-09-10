# Kafka + Avro Order Pipeline

Big Data module assignment. A Kafka producer/consumer system using Avro
serialization over Confluent Schema Registry, with real-time aggregation,
exponential-backoff retry for transient failures, and a Dead Letter Queue for
permanent ones.

---

## 1. Overview and requirements traceability

| #  | Requirement | Where it is satisfied | Demo evidence |
|----|-------------|-----------------------|---------------|
| R1 | Produce & consume `order` messages | [`producer/producer.py`](producer/producer.py), [`consumer/consumer.py`](consumer/consumer.py) | Live terminal output |
| R2 | Avro serialization | [`schemas/order.avsc`](schemas/order.avsc) + Schema Registry serializers | Registry shows registered schema; raw bytes begin `0x00` and are not JSON |
| R3 | Real-time aggregation (running average of prices) | [`consumer/aggregator.py`](consumer/aggregator.py) | Rolling average printed per message |
| R4 | Retry logic for temporary failures | [`consumer/retry.py`](consumer/retry.py) | `FLAKY` order retried and recovered on attempt 3 |
| R5 | Dead Letter Queue | [`consumer/dlq.py`](consumer/dlq.py) → topic `orders.DLQ` | `POISON` order in DLQ with 7/7 error headers |
| R6 | Live demonstration | [`demo/run_demo.sh`](demo/run_demo.sh) + §6 below | Guided walkthrough |
| R7 | Git repository | Phased commit history, README, `.gitignore` | `git log --oneline` |
| R8 | Free choice of language | Python 3.11 (justified in §5) | — |

---

## 2. Architecture

```
                    ┌──────────────────────┐
                    │   Schema Registry    │  subject: orders-value
                    │      :8081           │  (schema id embedded in each message)
                    └──────────┬───────────┘
                         ▲     │
              register   │     │  fetch writer schema by id
                         │     ▼
   ┌─────────────┐   ┌───┴──────────┐   ┌──────────────────────────────┐
   │  producer   │──▶│ topic:orders │──▶│          consumer            │
   │             │   │ 3 partitions │   │                              │
   │ key=orderId │   └──────────────┘   │  1. deserialize (Avro)       │
   │ value=Avro  │                      │  2. validate                 │
   └─────────────┘                      │  3. process w/ RetryPolicy   │
                                        │       ├─ transient → retry   │
                                        │       └─ permanent → DLQ     │
                                        │  4. aggregate (running avg)  │
                                        │  5. commit offset ALWAYS     │
                                        └───────────┬──────────────────┘
                                                    │ on failure
                                                    ▼
                                        ┌──────────────────────────────┐
                                        │   topic: orders.DLQ          │
                                        │   JSON payload + 7 headers   │
                                        │   (origin, error, retries)   │
                                        └──────────────────────────────┘
```

Message key is `orderId` (plain string), so Kafka's default partitioner hashes
every event for an order onto the same partition, preserving per-order ordering.
The value is Avro, prefixed by a 5-byte Confluent header: magic byte `0x00` plus
the 4-byte schema id.

---

## 3. Prerequisites and setup

- Docker Desktop (Kafka, Schema Registry and Kafka UI all run in Compose)
- Python 3.11

> **Python 3.11 is required, not merely recommended.** The pinned
> `confluent-kafka==2.5.0` and `fastavro==1.9.4` publish no CPython 3.13 wheels,
> so on 3.13 pip falls back to building both from source and fails unless the
> Microsoft C++ Build Tools are installed. On 3.11 both install as prebuilt
> wheels with no compiler. If `py -0` does not list 3.11:
> `winget install -e --id Python.Python.3.11`

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

`.env` is gitignored; `.env.example` is committed so a clean clone has every
broker address, port and topic name it needs.

Start the infrastructure and create the topics:

```powershell
docker compose up -d          # wait for kafka + schema-registry to report (healthy)
docker compose ps
python -m tools.create_topics # idempotent; safe to re-run before every demo
```

Verify:

| Check | Command | Expected |
|---|---|---|
| Registry up, no schemas yet | `curl localhost:8081/subjects` | `[]` |
| Topics created | Kafka UI at http://localhost:8080 | `orders` (3 partitions), `orders.DLQ` (1) |

> **Kafka UI lags ~6 seconds** behind the broker while it builds its cluster
> cache. If topics do not appear immediately after creating them, refresh once.

---

## 4. Running the producer and consumer

### Consumer

```powershell
python -m consumer.consumer
```

| Flag | Default | Purpose |
|---|---|---|
| `--summary-every N` | 10 | Print the per-product table every N messages (0 disables) |
| `--group-id ID` | from `.env` | Override the consumer group |
| `--poll-timeout S` | 1.0 | Seconds to block per poll |
| `--max-attempts N` | 3 | Attempts per message before giving up |
| `--base-delay S` | 0.5 | First retry backoff, doubled each attempt |
| `--flaky-attempts N` | 2 | How many times a `FLAKY` order fails before succeeding |

Stop with **Ctrl+C**: it finishes the message in hand, commits, flushes the DLQ
producer and prints the final aggregate summary.

### Producer

```powershell
python -m producer.producer --count 20 --rate 2 --poison-rate 0
```

| Flag | Default | Purpose |
|---|---|---|
| `--count N` | 100 | Number of orders to produce |
| `--rate R` | 5 | Messages per second (0 = unthrottled) |
| `--poison-rate F` | 0.1 | Fraction sent as `POISON` (permanent failure) |
| `--flaky-rate F` | 0.0 | Fraction sent as `FLAKY` (transient failure) |
| `--product P` | — | Force every order to one product; overrides the rate flags |
| `--start-id N` | 1001 | First `orderId` |
| `--seed N` | — | RNG seed; makes prices reproducible across runs |

### DLQ reader

```powershell
python -m tools.read_dlq              # print everything currently in the DLQ
python -m tools.read_dlq --follow     # stay attached and print new failures
```

Uses a throwaway consumer group per run and commits nothing, so it is
non-destructive and always shows the DLQ from the beginning.

---

## 5. Design decisions and justifications

### 5.1 Why Avro + Schema Registry over JSON

The schema lives in the Registry, not in the message. Each message carries only a
5-byte header (magic byte + schema id) and then the field *values* — no field
names on the wire. In this project a full order serializes to **20 bytes**; the
equivalent JSON is roughly 50.

More importantly it gives a **contract**. The Registry enforces compatibility, so
a producer cannot silently ship a breaking change; a consumer fetches the
*writer's* schema by embedded id and reads it into its own *reader's* schema,
which is the mechanism that makes schema evolution possible at all. JSON has no
equivalent — a field rename is only discovered when something downstream breaks.

The cost is a runtime dependency on the Registry and a payload that is not
human-readable without it. That trade is why the **DLQ deliberately uses JSON**
instead (§5.5).

### 5.2 Incremental averaging vs. batch recomputation

[`aggregator.py`](consumer/aggregator.py) updates the mean as:

```python
avg += (price - avg) / count
```

rather than storing prices and computing `sum(prices) / len(prices)`. Three
reasons:

1. **Memory.** A Kafka topic is an *unbounded* stream. Retaining every price to
   recompute grows without limit — after a million orders the consumer holds a
   million floats *per product*. The incremental form keeps **O(1) state per
   key** regardless of message count.
2. **Time.** Recomputing from a list is O(n) per message, so O(n²) over a run.
   This is **O(1) per message**, O(n) overall.
3. **Numerical stability.** It updates by a small *correction term* rather than
   dividing one large accumulated sum, so precision does not degrade as the
   running total grows relative to each individual price.

Bounded memory plus constant work per event is what makes this a genuinely
*streaming* computation rather than a batch one run repeatedly.

It is also **order-independent**, which matters because Kafka only guarantees
ordering *within* a partition — a 3-partition consumer sees prices interleaved
differently on every run. `test_average_is_independent_of_arrival_order` and
`test_incremental_average_matches_batch_recomputation` assert both properties.

### 5.3 Retry classification: transient vs permanent

| Classification | Examples | Handling |
|---|---|---|
| **Transient** | Downstream timeout, connection reset, `FLAKY` product | Retry up to 3 attempts with exponential backoff |
| **Permanent** | Validation failure, negative price, `POISON` product, deserialization failure | Straight to the DLQ, **no retries** |

The distinction is *whether the state that caused the failure can change on its
own*. A timeout might succeed a second later; a negative price will be negative
forever, so retrying it three times wastes ~1.8 seconds of that partition's
throughput and still ends in the DLQ.

**Unknown exceptions default to permanent** ([`retry.py`](consumer/retry.py)).
This asymmetry is deliberate: an unknown error wrongly treated as permanent costs
one trip to the DLQ, whereas one wrongly treated as transient burns the retry
budget on *every single message*. "Unknown" means no evidence it will ever
succeed, so the pipeline does not gamble.

Backoff is `0.5s → 1.0s → 2.0s` with **±25% jitter**. Exponential because a
struggling downstream needs geometrically more room to recover, not a constant
drumbeat of retries. Jitter because many consumers failing on the same dependency
would otherwise retry in lockstep and re-overwhelm it — the thundering-herd
problem.

The policy is a single `RetryPolicy.execute()` returning an outcome object,
rather than `try/except` scattered through the poll loop: the loop states *what*
to do, the policy owns *how many times* and *how long to wait*.

### 5.4 Why offsets are committed after DLQ routing

**This is the design decision most likely to be questioned, so it is stated in
full.**

A Kafka offset is a **single monotonic position per partition**, not a
per-message acknowledgement. There is no way to mark one record as done while
leaving an earlier one outstanding.

So *not* committing a poison message does not retry only that message — it pins
the entire partition at that offset. The next poll returns the same record, it
fails identically, and this repeats forever. Every valid message queued behind it
on that partition is blocked. That is **head-of-line blocking**: one bad record
halts a partition permanently.

Routing to the DLQ is precisely what makes committing safe. The record is not
discarded — it has been durably written to `orders.DLQ` with the metadata needed
to diagnose and replay it. Responsibility has been **transferred, not
abandoned**, so the main stream is free to advance.

**Ordering within the handler is deliberate: DLQ publish first, commit second.**
If the process died between the two, the message would be reprocessed and
re-sent to the DLQ — a duplicate, which is recoverable by de-duplicating on
`orderId`. Committing first would risk losing the record entirely if the DLQ
write then failed. At-least-once beats at-most-once when the payload is a failure
report.

Verified end to end: after a `POISON` message was DLQ'd, its partition committed
one offset *past* it, and resuming the same consumer group processed 5 newly
produced orders with `failed=0` — never re-encountering the poison.

### 5.5 DLQ payload format, and why it is JSON not Avro

The ideal is to republish the original Avro bytes untouched. That is not
achievable here: `DeserializingConsumer` decodes the value **in place** before the
application sees it — `Message.value()` returns the decoded dict and the raw
frame is not retained anywhere on the message object. JSON is the documented
fallback, and is arguably the better choice regardless:

- A message may be in the DLQ *precisely because* it could not be decoded against
  the schema. Re-encoding it would either fail or silently alter it.
- The DLQ is read by a human during triage, possibly while the Registry itself is
  the thing that failed. JSON needs no Registry lookup.
- The DLQ therefore needs no schema subject of its own.

Failure metadata goes in **headers**, not merged into the payload, so the payload
stays byte-identical to what the pipeline tried to process (a replay tool can
re-emit it untouched), headers can be inspected without deserializing the value,
and the Avro schema continues to describe an *Order* rather than an error.

| Header | Purpose |
|---|---|
| `x-original-topic` / `x-original-partition` / `x-original-offset` | Locate the exact source record — makes targeted replay possible |
| `x-error-class` | Group failures by type; distinguishes the two DLQ entry paths |
| `x-error-message` | Human-readable detail (truncated to 1000 chars) |
| `x-retry-count` | Separates "tried 3 times and failed" from "never had a chance" |
| `x-failed-at` | ISO-8601 UTC timestamp |

### 5.6 Manual commit vs auto-commit

`enable.auto.commit=False`. Auto-commit advances the offset **on a timer**,
independently of whether the message was actually processed — a crash between the
timer firing and processing completing silently skips records (at-most-once).
Committing explicitly after processing ties the unit of work to the offset
advance, giving **at-least-once** delivery.

Note the commit happens *after* the retry policy returns, so a message retried
three times commits **exactly once**, not once per attempt: retries live inside
the unit of work, not around it.

### 5.7 Failure injection by product name

The Avro schema **cannot express an invalid record** — anything that failed
validation would also fail serialization and never reach the topic. Since the
brief defines no real downstream dependency, failures are simulated in-band by
product name: `POISON` triggers a permanent business-rule failure, `FLAKY` a
transient one. This is isolated in [`consumer/failures.py`](consumer/failures.py)
so the consumer depends on an abstract "side effect that may fail" rather than on
demo scaffolding.

### 5.8 Why Python 3.11 and confluent-kafka

`confluent-kafka` wraps librdkafka, the officially supported C client, and is the
only Python client with native Schema Registry integration —
`kafka-python` has none and could not satisfy R2. Python 3.11 rather than 3.12+
because the pinned versions ship prebuilt wheels for it (see §3).

---

## 6. Demo walkthrough

Three terminals. `demo/run_demo.sh` drives the producer side and pauses between
steps so the consumer output can be narrated:

```bash
bash demo/run_demo.sh          # interactive
bash demo/run_demo.sh --auto   # unattended, for a recording
```

| # | Terminal | Action | What to point at |
|---|---|---|---|
| 1 | T1 | `docker compose up -d` | Kafka + Schema Registry report `(healthy)` |
| 2 | Browser | Kafka UI :8080, Registry :8081/subjects | Topics empty, `[]` — no subjects yet |
| 3 | T2 | `python -m consumer.consumer` | Idles, waiting |
| 4 | T3 | `--count 20 --rate 2 --poison-rate 0` | **R1/R3:** running average updates per message. **R2:** `orders-value` now registered at version 1 |
| 5 | T3 | `--count 1 --product FLAKY` | **R4:** `attempt=1/3` → backoff → `attempt=2/3` → `recovered`. Timestamps show the backoff doubling. Average counts it **once** |
| 6 | T3 | `--count 1 --product POISON` | **R5:** `permanent failure attempt=1/3` (no backoff) → `routed to DLQ` → offset committed |
| 7 | T3 | `python -m tools.read_dlq` | 7/7 headers populated, origin coordinates recorded |
| 8 | T3 | `--count 5 --poison-rate 0` | Consumer keeps working — **proves the poison did not block the partition** |
| 9 | T2 | Ctrl+C | Final aggregate summary: global + per-product averages |
| 10 | T3 | `git log --oneline` | Phased commit history |

To show the Avro bytes directly — the same three messages read two ways:

```bash
# Without the Registry: opaque binary, note the leading \0 magic byte
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic orders --from-beginning --max-messages 3 | od -c | head

# With the Registry: decoded records
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic orders --from-beginning \
  --max-messages 3 --property schema.registry.url=http://localhost:8081
```

### Resetting between rehearsals

```powershell
docker compose down -v        # wipes all topic data and the Registry
docker compose up -d
python -m tools.create_topics
```

> **Warm the Registry before the assessment.** On a cold cluster the *first*
> schema registration can fail with `SchemaRegistryError 50002` — the Registry's
> write to its internal `_schemas` topic times out after 500ms. Re-running
> succeeds immediately. Produce one message before the examiner is watching.

---

## 7. Testing

```powershell
pytest -q
```

**49 tests, no Kafka required** — the aggregator, retry policy and DLQ header
builder were all written without broker dependencies, so the suite runs on a
machine with Docker stopped (verified).

| File | Tests | Covers |
|---|---|---|
| [`tests/test_aggregator.py`](tests/test_aggregator.py) | 12 | Empty and single-element cases, batch equivalence (incl. 10,000 random prices), order-independence, per-product isolation |
| [`tests/test_retry.py`](tests/test_retry.py) | 19 | Classification, attempt counts, the 0.5/1/2 backoff sequence, jitter bounds, exhaustion |
| [`tests/test_dlq_payload.py`](tests/test_dlq_payload.py) | 18 | All 7 headers present and non-empty, both DLQ entry paths, payload round-trip, offset-zero handling |

The retry tests inject a **fake clock** rather than sleeping, so the full suite
runs in ~0.3s while still asserting multi-second backoff behaviour.

---

## 8. Known limitations and possible extensions

**Limitations**

- **Single broker, replication factor 1.** A development cluster, not production
  HA. A real deployment needs RF≥3 and `min.insync.replicas=2`.
- **Aggregation state is in-memory and lost on restart.** The running average
  starts from zero each run. Production would use Kafka Streams state stores or
  an external store.
- **Avro `float` is 32-bit**, as the brief specifies. Prices therefore lose
  precision in about the 7th significant digit — a price produced as `38.36`
  reads back as `38.36000061035156`, visible in the DLQ output. `double` would
  fix it; the brief's field type was kept deliberately.
- **Failure injection is simulated** via product names, since the brief defines
  no real downstream dependency.
- **The DLQ has no automated replay path.** Messages carry enough metadata to be
  replayed, but a replay tool is not implemented.
- **At-least-once, not exactly-once.** A crash between DLQ publish and offset
  commit produces a duplicate DLQ record. Exactly-once would need a transactional
  producer with `sendOffsetsToTransaction`.

**Possible extensions**

- Kafka Streams (or Faust) for aggregation, giving durable state stores and
  windowed averages instead of a single cumulative figure.
- Schema evolution demo: add a nullable `currency` field as version 2 and show a
  v1 consumer still reading v2 messages.
- Tiered retry topics (`orders.retry.5s`, `orders.retry.1m`) so retries happen
  off the main partition instead of blocking it during backoff.
- Consumer group scale-out across the 3 partitions to show parallel consumption.
- Prometheus metrics for DLQ rate, retry rate and consumer lag.
