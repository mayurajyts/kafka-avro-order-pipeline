# Kafka + Avro Order Pipeline

Big Data module assignment. A Kafka producer/consumer system using Avro
serialization over Confluent Schema Registry, with real-time aggregation,
exponential-backoff retry for transient failures, and a Dead Letter Queue for
permanent ones.

> **Build status: Phase 0 — repo scaffold.**
> Sections below are filled in by their owning phase (see
> `IMPLEMENTATION_PLAN.md` §7); Phase 8 completes the document.

---

## 1. Overview and requirements traceability

| #  | Requirement | Where it is satisfied | Demo evidence |
|----|-------------|-----------------------|---------------|
| R1 | Produce & consume `order` messages | `producer/producer.py`, `consumer/consumer.py` | Live terminal output |
| R2 | Avro serialization | `schemas/order.avsc` + Schema Registry serializers | Registry shows registered schema; raw bytes are not JSON |
| R3 | Real-time aggregation (running average of prices) | `consumer/aggregator.py` | Rolling average printed per message |
| R4 | Retry logic for temporary failures | `consumer/retry.py` | Injected transient failure retried and recovered |
| R5 | Dead Letter Queue | `consumer/dlq.py` → topic `orders.DLQ` | Poison message appears in DLQ with error metadata |
| R6 | Live demonstration | `demo/run_demo.sh` + §6 below | Live walkthrough |
| R7 | Git repository | Phased commit history, README, `.gitignore` | Repo link |
| R8 | Free choice of language | Python 3.11 (justified in §5) | — |

---

## 2. Architecture

_Phase 8._ Producer → `orders` topic → consumer → {aggregator, DLQ}.

---

## 3. Prerequisites and setup

- Docker Desktop (Kafka, Schema Registry and Kafka UI run in Compose — Phase 1)
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

---

## 4. Running the producer and consumer

_Phases 3 and 4. All CLI flags documented here._

---

## 5. Design decisions and justifications

_Phase 8 writes these up in full. Each one is a question an examiner is likely
to ask, so none of them may be left as "it just works"._

- **Why Avro + Schema Registry over JSON** — _Phase 2._
- **Incremental averaging vs. batch recomputation** — _Phase 4._ Why the running
  average is updated as `avg += (price - avg) / count` rather than by storing all
  prices and re-summing: the consumer is an unbounded stream, so per-key memory
  must be O(1) and per-message cost O(1). Batch recomputation would grow without
  bound and is not a streaming computation at all.
- **Retry classification: transient vs permanent, and why** — _Phase 5._
- **Why offsets are committed after DLQ routing** — _Phase 6._ Once a message has
  been safely handed to `orders.DLQ`, it has been dealt with. Not committing would
  make the consumer re-read the same poison message forever on the next poll,
  stalling that partition and blocking every later message behind it. The DLQ
  publish is the durable record of the failure; the commit is what lets the stream
  make progress.
- **Manual commit vs auto-commit** — _Phase 6._

---

## 6. Demo walkthrough

_Phase 8._

---

## 7. Testing

_Phase 7._

```powershell
pytest -q
```

---

## 8. Known limitations and possible extensions

_Phase 8._
