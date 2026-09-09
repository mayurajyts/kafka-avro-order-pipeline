# Kafka + Avro Order Pipeline — Implementation Plan

> Big Data module assignment. Build a Kafka producer/consumer system using Avro
> serialization, with real-time aggregation, retry logic, and a Dead Letter Queue.
> Must be demonstrated live and submitted as a Git repository.

---

## 1. Requirements Traceability

Every requirement from the assignment brief, mapped to where it is satisfied.

| # | Requirement | Where it is satisfied | Demo evidence |
|---|---|---|---|
| R1 | Produce & consume `order` messages | `producer/producer.py`, `consumer/consumer.py` | Live terminal output |
| R2 | Avro serialization | `schemas/order.avsc` + Schema Registry serializers | Registry UI shows registered schema; raw bytes are not JSON |
| R3 | Real-time aggregation (running average of prices) | `consumer/aggregator.py` | Rolling average printed per message |
| R4 | Retry logic for temporary failures | `consumer/retry.py` | Injected transient failure retried and recovered |
| R5 | Dead Letter Queue | `consumer/dlq.py` → topic `orders.DLQ` | Poison message appears in DLQ topic with error metadata |
| R6 | Live demonstration | `demo/run_demo.sh` + `README.md` demo script | Recorded/live walkthrough |
| R7 | Git repository | Repo with clean commit history, README, `.gitignore` | Repo link |
| R8 | Free choice of language | Python 3.11 (justified in README) | — |

**Rule for the implementer:** do not mark a phase complete until its row above has
real, reproducible evidence — not just code that compiles.

---

## 2. Technology Decisions

| Concern | Choice | Reason |
|---|---|---|
| Language | Python 3.11 | Fastest path to a working Avro + Kafka demo; `confluent-kafka` is the officially supported client |
| Kafka client | `confluent-kafka[avro]` (librdkafka) | Native Schema Registry integration via `AvroSerializer` / `AvroDeserializer`. Do **not** use `kafka-python` — it has no Schema Registry support |
| Schema management | Confluent Schema Registry | Assignment requires Avro; Registry gives schema IDs, versioning, and compatibility checks |
| Infrastructure | Docker Compose (Kafka KRaft mode, Schema Registry, Kafka UI) | Reproducible on any marker's machine; no ZooKeeper needed |
| Config | `.env` + `config.py` dataclass | No hardcoded brokers/topics anywhere in logic |
| Logging | stdlib `logging`, structured single-line records | Readable during the live demo |
| Tests | `pytest` | Unit tests for aggregator + retry policy (no Kafka needed) |

### Pinned versions
```
confluent-kafka[avro,schemaregistry]==2.5.0
fastavro==1.9.4
python-dotenv==1.0.1
pytest==8.2.0
```

---

## 3. Target Repository Structure

```
kafka-avro-orders/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md                     # setup + demo script + screenshots
├── schemas/
│   └── order.avsc
├── common/
│   ├── __init__.py
│   ├── config.py                 # env-driven settings dataclass
│   ├── logging_setup.py
│   └── schema.py                 # loads order.avsc, builds (de)serializers
├── producer/
│   ├── __init__.py
│   └── producer.py               # CLI: --count, --rate, --poison-rate
├── consumer/
│   ├── __init__.py
│   ├── consumer.py               # main poll loop, orchestration
│   ├── aggregator.py             # running average (global + per-product)
│   ├── retry.py                  # exponential backoff + classification
│   ├── dlq.py                    # DLQ publisher with error headers
│   └── failures.py               # injectable failure simulator for the demo
├── tools/
│   ├── create_topics.py
│   └── read_dlq.py               # prints DLQ messages + headers
├── tests/
│   ├── test_aggregator.py
│   ├── test_retry.py
│   └── test_dlq_payload.py
└── demo/
    └── run_demo.sh
```

---

## 4. Avro Schema

`schemas/order.avsc` — exactly the three fields required by the brief:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.university.bigdata.orders",
  "doc": "A single purchase transaction.",
  "fields": [
    { "name": "orderId", "type": "string", "doc": "Unique identifier, e.g. \"1001\"" },
    { "name": "product", "type": "string", "doc": "Purchased item name, e.g. \"Item1\"" },
    { "name": "price",   "type": "float",  "doc": "Product price (randomized)" }
  ]
}
```

Notes:
- `float` in Avro is 32-bit. Keep it `float` because the brief specifies it — mention
  the precision implication in the README rather than silently switching to `double`.
- Message **key** = `orderId` (String serializer) so all events for an order land on
  the same partition. **Value** = Avro-serialized `Order`.
- Subject naming strategy: default `TopicNameStrategy` → subject `orders-value`.

---

## 5. Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `orders` | 3 | Main order stream |
| `orders.DLQ` | 1 | Permanently failed messages |

Create with `tools/create_topics.py` using `AdminClient` (idempotent — ignore
`TOPIC_ALREADY_EXISTS`). Replication factor 1 for the single-broker dev cluster.

---

## 6. Component Specifications

### 6.1 Producer (`producer/producer.py`)

- CLI flags: `--count` (default 100), `--rate` msgs/sec (default 5),
  `--poison-rate` (default 0.1) to deliberately emit malformed/failing orders.
- Generates orders: incrementing `orderId` from 1001, `product` from a fixed pool
  (`Item1`..`Item5`), `price` random in `[5.0, 500.0]` rounded to 2dp.
- Uses `SerializingProducer` with `AvroSerializer(schema_registry_client, schema_str)`.
- Delivery report callback logs partition/offset per message; `flush()` on exit.
- **Poison message strategy:** the schema cannot express an invalid record, so
  simulate failures by product name — a product `"POISON"` triggers a permanent
  business-rule failure in the consumer, and `"FLAKY"` triggers a transient failure.
  Document this clearly; it is what makes R4/R5 demonstrable.

### 6.2 Aggregator (`consumer/aggregator.py`)

- Pure class, no Kafka dependency → easily unit tested.
- Maintains: `count`, `sum`, `running_average` globally, plus a `dict[product] →
  (count, sum, avg)`.
- Use **incremental (Welford-style) update**, not recomputation over a stored list:
  `avg += (price - avg) / count`. Mention this in the README as the "streaming"
  property — O(1) memory per key.
- `snapshot()` returns a dict for logging; consumer prints it every message and a
  formatted table every N messages.

### 6.3 Retry logic (`consumer/retry.py`)

- Classify exceptions into `TransientError` vs `PermanentError`.
  - Transient: simulated downstream/network timeouts, `FLAKY` products.
  - Permanent: deserialization failures, validation failures, `POISON` products,
    negative prices.
- Retry policy: max 3 attempts, exponential backoff `0.5s, 1s, 2s` with jitter.
- Implement as a decorator or a `RetryPolicy.execute(fn)` helper returning an
  outcome object — do not scatter `try/except` through the poll loop.
- Log every attempt: `attempt=2/3 orderId=1004 error=... backoff=1.0s`.

### 6.4 DLQ (`consumer/dlq.py`)

- On permanent failure, or on exhausting retries, publish to `orders.DLQ`.
- **Payload:** the original Avro-serialized value re-published as-is where possible;
  if the original bytes are unavailable, publish a JSON envelope.
- **Headers (this is the marks-earning detail):**
  `x-original-topic`, `x-original-partition`, `x-original-offset`,
  `x-error-class`, `x-error-message`, `x-retry-count`, `x-failed-at` (ISO-8601).
- The main consumer must **still commit the offset** after routing to DLQ —
  otherwise the poison message blocks the partition forever. Say this explicitly
  in the README; examiners commonly ask about it.

### 6.5 Consumer (`consumer/consumer.py`)

- `DeserializingConsumer` with `AvroDeserializer`, `enable.auto.commit=False`,
  `auto.offset.reset=earliest`, group id from config.
- Loop per message:
  1. Deserialize (failure here → straight to DLQ, it is permanent).
  2. Validate (`price > 0`, non-empty `orderId`/`product`).
  3. `process()` under `RetryPolicy` — process = update aggregator + simulated side effect.
  4. On success → log running average. On permanent/exhausted → DLQ.
  5. `consumer.commit(asynchronous=False)`.
- Graceful shutdown on SIGINT: close consumer, flush DLQ producer, print final
  aggregate summary.

---

## 7. Build Phases

Work strictly in this order. Each phase ends with a **verifiable checkpoint** and a
**single git commit**.

### Phase 0 — Repo scaffold
- Init git, `.gitignore` (venv, `__pycache__`, `.env`, logs), `requirements.txt`, README skeleton.
- ✅ Checkpoint: `pip install -r requirements.txt` succeeds in a fresh venv.
- Commit: `chore: project scaffold and dependencies`

### Phase 1 — Infrastructure
- `docker-compose.yml`: Kafka in KRaft mode, Schema Registry (port 8081),
  Kafka UI (port 8080). Healthchecks on both Kafka and Registry.
- `tools/create_topics.py`.
- ✅ Checkpoint: `docker compose up -d`, then `curl localhost:8081/subjects` returns `[]`,
  and both topics are visible in Kafka UI.
- Commit: `feat: kafka + schema registry docker environment`

### Phase 2 — Schema & shared config
- `schemas/order.avsc`, `common/config.py`, `common/schema.py`, `common/logging_setup.py`.
- ✅ Checkpoint: a scratch script registers the schema; `curl localhost:8081/subjects`
  returns `["orders-value"]`.
- Commit: `feat: avro order schema and shared config`

### Phase 3 — Producer
- Full producer with CLI flags and delivery reports.
- ✅ Checkpoint: produce 10 messages; confirm in Kafka UI that values are Avro-decoded
  (Registry-aware view) and that raw bytes start with magic byte `0x00`.
- Commit: `feat: avro order producer`

### Phase 4 — Consumer + aggregation
- Consumer skeleton + aggregator, no retry/DLQ yet.
- ✅ Checkpoint: running average printed per message and matches a hand-computed
  average of the produced prices.
- Commit: `feat: consumer with real-time running average aggregation`

### Phase 5 — Retry
- `retry.py`, `failures.py`, error classification wired into the loop.
- ✅ Checkpoint: a `FLAKY` message logs attempts 1–3 and then succeeds; offset commits once.
- Commit: `feat: exponential backoff retry for transient failures`

### Phase 6 — DLQ
- `dlq.py`, headers, offset-commit-after-DLQ behaviour, `tools/read_dlq.py`.
- ✅ Checkpoint: a `POISON` message lands in `orders.DLQ` with all headers populated,
  and the consumer continues processing subsequent messages without stalling.
- Commit: `feat: dead letter queue with error metadata headers`

### Phase 7 — Tests
- `pytest` unit tests: aggregator correctness (incl. empty + single-element cases),
  retry attempt counts and backoff sequence, DLQ header construction.
- ✅ Checkpoint: `pytest -q` all green.
- Commit: `test: unit coverage for aggregator, retry and dlq`

### Phase 8 — Demo & documentation
- `demo/run_demo.sh` orchestrating the full walkthrough (below).
- README: architecture diagram (ASCII or Mermaid), setup steps, design decisions,
  screenshots, known limitations.
- ✅ Checkpoint: a clean-clone run reproduces the whole demo from the README alone.
- Commit: `docs: readme, architecture and demo script`

---

## 8. Live Demo Script

Run this end to end when demonstrating. Three terminals.

1. **T1** — `docker compose up` — show Kafka + Schema Registry healthy.
2. **Browser** — Kafka UI: show `orders` and `orders.DLQ` topics empty, Registry has no subjects.
3. **T2** — start consumer. It idles waiting for messages.
4. **T3** — `python -m producer.producer --count 20 --rate 2 --poison-rate 0`.
   - Point at the consumer output: running average updating per message.
   - Point at Registry: `orders-value` subject now registered, version 1.
5. **T3** — produce one `FLAKY` order.
   - Consumer logs attempt 1 → backoff → attempt 2 → success. **Retry proven.**
6. **T3** — produce one `POISON` order.
   - Consumer logs permanent failure → routed to DLQ → offset committed.
   - `python -m tools.read_dlq` shows the message with error headers. **DLQ proven.**
7. **T3** — produce 5 more normal orders → consumer keeps working, average continues.
   **Proves the poison message did not block the partition.**
8. **T2** — Ctrl+C → final aggregate summary table (global + per-product averages).
9. Show `git log --oneline` — clean, phased commit history.

Rehearse this at least once before the assessment; the ordering above is designed so
each requirement is demonstrated in isolation and is unmistakable to the marker.

---

## 9. README Contents (required sections)

1. Project overview and the assignment requirements table (reuse §1).
2. Architecture diagram — producer → `orders` → consumer → {aggregator, DLQ}.
3. Prerequisites and setup (Docker, Python 3.11, `pip install -r requirements.txt`).
4. How to run producer and consumer, with all CLI flags documented.
5. Design decisions and justifications:
   - Why Avro + Schema Registry over JSON.
   - Incremental averaging vs. batch recomputation.
   - Retry classification: which errors are transient vs permanent, and why.
   - Why offsets are committed after DLQ routing.
   - Manual commit vs auto-commit.
6. Demo walkthrough (reuse §8).
7. Testing instructions.
8. Known limitations and possible extensions (e.g. Kafka Streams for aggregation,
   schema evolution with a nullable `currency` field, tiered retry topics).

---

## 10. Assumptions to State Explicitly

- Single-broker, replication factor 1 — development cluster only, not production HA.
- Aggregation state is in-memory and lost on restart; a production system would use
  Kafka Streams state stores or an external store. Say this rather than hiding it.
- Failure injection is deliberate and controlled via product names / CLI flags,
  since the brief does not define a real downstream dependency.

---

## 11. Definition of Done

- [ ] Every row in §1 has reproducible evidence.
- [ ] `docker compose up` + README steps work from a clean clone on another machine.
- [ ] `pytest -q` passes.
- [ ] No hardcoded brokers, ports, or topic names outside `common/config.py`.
- [ ] `.env` is gitignored; `.env.example` is committed.
- [ ] Demo rehearsed end to end without errors.
- [ ] Git history is phased and readable (~9 meaningful commits, not one dump).
