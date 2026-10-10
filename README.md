# Message Deduplication & Idempotency Framework

[![CI](https://github.com/Parth-Vasave/Message-Deduplication-Idempotency-Framework/actions/workflows/ci.yml/badge.svg)](https://github.com/Parth-Vasave/Message-Deduplication-Idempotency-Framework/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Coverage](https://img.shields.io/badge/coverage-%E2%89%A585%25-brightgreen)

An async Python framework for exactly-once message processing on top of Kafka. It deduplicates messages with Redis, makes arbitrary business logic idempotent, and publishes reliably through a transactional outbox.

Kafka delivers at least once, so network hiccups, broker failovers and consumer crashes all produce duplicate messages. This framework makes sure each message's side effects run once, which prevents double orders, double charges and corrupted data.

**Topics:** `kafka` · `redis` · `idempotency` · `deduplication` · `exactly-once` · `transactional-outbox` · `asyncio` · `python` · `prometheus` · `kubernetes`

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [How It Works](#how-it-works)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Usage](#usage)
- [Testing](#testing)
- [Observability](#observability)
- [Kubernetes Deployment](#kubernetes-deployment)
- [CI/CD](#cicd)
- [Design Decisions](#design-decisions)
- [Contributing](#contributing)

---

## Features

- **`DedupConsumer`** — an `aiokafka` consumer that runs the full claim → handle → complete → commit lifecycle, with retries, backoff and a dead-letter queue (DLQ).
- **Pluggable stores** — `RedisDeduplicationStore` for production, `InMemoryDeduplicationStore` for development and tests, or your own `DeduplicationStore` implementation.
- **Atomic claims** — a server-side Redis Lua script guarantees that exactly one consumer wins a message, even across instances.
- **Crash recovery** — a `PROCESSING` claim is a lease. If its consumer dies mid-message, the message becomes claimable again once the lease expires, rather than being blocked for the whole TTL.
- **Guarded state transitions** — a Lua script stops a late retry from overwriting a `COMPLETED` record.
- **`IdempotencyGuard` and `@idempotent`** — make any async code idempotent, with or without Kafka.
- **Transactional outbox** — `OutboxWriter` and `OutboxRelay` for atomic "write to the DB and publish to Kafka".
- **Observability** — Prometheus metrics, a provisioned Grafana dashboard, Alertmanager rules and structured JSON logs.
- **Production-ready deployment** — a Dockerfile, Docker Compose stack, Kubernetes manifests (Kustomize, HPA, PDB, Ingress) and GitHub Actions CI/CD.

---

## Architecture Overview

```
Kafka Topic
    |
    v
DedupConsumer
    |
    |-- Extract message_id (X-Message-Id header, or payload "message_id" / "id")
    |-- claim atomically (Redis Lua script)
    |       |-- claimable: new, FAILED with retries left (retry_failed),
    |       |              or PROCESSING with an expired lease --> continue
    |       |-- otherwise: COMPLETED, or PROCESSING with a live lease --> skip, commit offset
    |
    |-- handle(payload)                      <-- your business logic
    |
    |-- success: mark_completed(result) --> commit offset
    |-- failure: mark_failed() --> backoff --> retry (up to max_retries)
    |                                     \--> DLQ topic --> commit offset
```

Redis is the single source of truth for message state. Claims and status transitions run as Lua scripts on the server, so concurrent consumer instances can't race each other.

---

## How It Works

### Message ID

`DedupConsumer` looks for the message's ID in this order:

1. The Kafka header `X-Message-Id`
2. The `message_id` field of the JSON payload
3. The `id` field of the JSON payload

If it finds none, the message is processed **without** deduplication and counted under the `no_id` status.

The recommended ID format, which the framework doesn't enforce, is:

```
Format:  {source}-{timestamp_ms}-{sequence_id}
Example: order-1712000000000-001

Redis key: dedup:{message_id}
TTL:       86400 seconds (24 hours, configurable)
```

### Status State Machine

```
(unseen) --> PROCESSING --> COMPLETED
                       \--> FAILED --> (retry) --> PROCESSING
                                              \--> DLQ
```

- `PROCESSING` — a consumer has claimed the message, so no other instance will process it until the claim's lease (`processing_timeout_seconds`) expires. An expired lease means the owner crashed, and the message can be claimed again.
- `COMPLETED` — the business logic succeeded and its result is stored. This state is terminal.
- `FAILED` — the handler raised an exception. The message can be retried up to `max_retries` times.
- `SKIPPED` — terminal marker for messages that were deliberately not processed.

### Atomic Claim

`claim()` runs a Lua script against `dedup:{id}`. It succeeds when the key is missing, when the record is `FAILED` and still has retries left, or when the record is `PROCESSING` and its lease has expired. Redis runs scripts atomically, so when two consumers race on the same `message_id`, only one of them can win. Lease times come from the Redis server clock (`TIME`), so clock skew between consumers doesn't matter.

If `handle()` succeeds but `mark_completed()` then fails (for example, Redis is unreachable or the result can't be JSON-serialised), the consumer logs the error and commits the offset anyway. It never re-runs `handle()`, because that would repeat the side effects. `@idempotent` behaves the same way.

`mark_completed()` and `mark_failed()` use a Lua script that changes the status only while the record is still `PROCESSING`, which stops a late retry from overwriting a `COMPLETED` result.

---

## Project Structure

```
Message-Deduplication-Idempotency-Framework/
├── src/
│   ├── dedup/
│   │   ├── store.py            # Abstract DeduplicationStore interface
│   │   ├── redis_store.py      # Redis-backed implementation (SET NX + Lua)
│   │   ├── memory_store.py     # In-memory implementation for dev/test
│   │   └── models.py           # ProcessingStatus, StatusValue, DeduplicationConfig
│   ├── consumer/
│   │   ├── base.py             # BaseConsumer (abstract)
│   │   ├── __main__.py         # `python -m src.consumer` entrypoint
│   │   ├── dedup_consumer.py   # DedupConsumer — full dedup lifecycle
│   │   └── handlers/           # Example handlers (user-owned stubs)
│   │       ├── order_handler.py
│   │       ├── payment_handler.py
│   │       └── shipment_handler.py
│   ├── idempotency/
│   │   ├── guard.py            # IdempotencyGuard context manager + @idempotent decorator
│   │   └── outbox.py           # Transactional outbox: OutboxWriter + OutboxRelay
│   ├── metrics/
│   │   └── dedup_metrics.py    # Prometheus counters, histograms, gauges
│   ├── api.py                  # FastAPI health, readiness and metrics endpoints
│   ├── config.py               # Pydantic settings (reads from environment / .env)
│   └── logging_config.py       # Structured logging via structlog
├── tests/
│   ├── unit/                   # Pure unit tests, no Docker required
│   ├── chaos/                  # Fault-injection and race-condition tests (fakeredis)
│   ├── integration/            # Real Redis, Kafka, Postgres via testcontainers
│   └── e2e/                    # Full-stack tests via testcontainers
├── k8s/                        # Kubernetes manifests (Kustomize)
├── grafana/                    # Dashboard JSON + provisioning
├── prometheus/                 # prometheus.yml, alert_rules.yml, alertmanager.yml
├── scripts/
│   └── init_db.sql             # Postgres schema (outbox table)
├── .github/workflows/          # ci.yml, cd.yml
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

---

## Tech Stack

| Layer | Choice |
|---|---|
| Language | Python 3.11+ |
| Kafka client | `aiokafka` (async) |
| Dedup store | Redis 7 via `redis-py` (`redis.asyncio`) |
| Database (outbox) | PostgreSQL 16 via `asyncpg` |
| API / Health | FastAPI + Uvicorn |
| Metrics | `prometheus-client` |
| Tracing | `opentelemetry-sdk` |
| Logging | `structlog` (structured JSON) |
| Container | Docker + Docker Compose |
| Orchestration | Kubernetes (Kustomize manifests) |
| Testing | `pytest`, `pytest-asyncio`, `fakeredis`, `testcontainers` |
| Linting | `ruff`, `mypy` (strict) |

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker and Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/Parth-Vasave/Message-Deduplication-Idempotency-Framework.git
cd Message-Deduplication-Idempotency-Framework
```

### 2. Configure environment

```bash
cp .env.example .env
# The defaults work for the local Docker Compose stack
```

### 3. Start the infrastructure

```bash
docker compose up -d
```

This starts Zookeeper, Kafka, Kafka UI, Redis, PostgreSQL (with `scripts/init_db.sql` applied), the FastAPI metrics/health API, Prometheus, Alertmanager and Grafana.

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| FastAPI metrics/health | http://localhost:9090 |
| Prometheus | http://localhost:9091 |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 |
| Kafka (from host) | `localhost:9094` |
| Redis | `localhost:6379` |
| PostgreSQL | `localhost:5432` |

Grafana's default credentials are `admin` / `dedup_secret`.

### 4. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 5. Run a consumer

To run the bundled order, payment and shipment handlers (configured from the environment variables below, with metrics served on `:9090`):

```bash
python -m src.consumer
```

This is also the command that the Kubernetes consumer deployment runs. For your own handlers, write a consumer script. See [Usage](#usage) for a complete example.

---

## Configuration

Settings come from environment variables or a `.env` file, through Pydantic Settings (`src/config.py`).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `KAFKA_GROUP_ID` | `dedup-consumer-group` | Consumer group ID |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `POSTGRES_DSN` | `postgresql://dedup:dedup_secret@localhost:5432/kafkadedup` | PostgreSQL DSN |
| `DEDUP_TTL_SECONDS` | `86400` | How long dedup records are retained (seconds) |
| `DEDUP_MAX_RETRIES` | `3` | Max attempts before sending to the DLQ |
| `DEDUP_RETRY_BACKOFF_MS` | `500` | Base backoff between retries (multiplied by attempt number) |
| `DEDUP_PROCESSING_TIMEOUT_SECONDS` | `300` | Lease on a `PROCESSING` claim. After it expires, a crashed consumer's message can be claimed again. Set it longer than your slowest `handle()` |
| `LOG_LEVEL` | `INFO` | Logging level |

You can also set dedup behaviour per consumer or store with `DeduplicationConfig`:

```python
from src.dedup.models import DeduplicationConfig

config = DeduplicationConfig(
    ttl_seconds=86400,
    max_retries=3,
    retry_backoff_ms=500,
    retry_failed=True,   # treat a previous FAILED status as retriable, not a duplicate
    processing_timeout_seconds=300,   # lease before a crashed claim can be retried
)
```

---

## Usage

### DedupConsumer

Subclass `DedupConsumer` and override `handle()`. It receives the decoded JSON payload, and its return value is stored as the result. The framework takes care of the rest of the dedup lifecycle.

```python
import asyncio
from typing import Any

from src.consumer.dedup_consumer import DedupConsumer
from src.dedup import RedisDeduplicationStore


class OrderConsumer(DedupConsumer):
    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        # Runs once per message_id, even if Kafka redelivers the message
        order = await create_order(message)
        return {"order_id": order.id}


async def main() -> None:
    store = RedisDeduplicationStore.from_url("redis://localhost:6379/0")
    consumer = OrderConsumer(
        topics=["orders"],
        store=store,
        bootstrap_servers="localhost:9094",   # defaults to KAFKA_BOOTSTRAP_SERVERS
        group_id="order-consumer-group",      # defaults to KAFKA_GROUP_ID
        dlq_topic="orders.dlq",               # optional; failures are dropped if unset
    )
    await consumer.start()   # also connects the store
    try:
        await consumer.run()
    finally:
        await consumer.stop()


asyncio.run(main())
```

Ready-made examples are in `src/consumer/handlers/`.

### IdempotencyGuard (context manager)

Use the guard when you need idempotency outside the Kafka consumer lifecycle, for example in an HTTP handler.

```python
from src.idempotency import IdempotencyGuard

async with IdempotencyGuard(store, idempotency_key="payment-abc-123") as guard:
    if guard.already_processed:
        return guard.previous_result
    result = await charge_payment(amount=99.00)
    guard.result = result   # persisted as COMPLETED on exit
```

If the block raises, the key is marked `FAILED` and the exception propagates. If another caller is still `PROCESSING` the same key, `already_processed` is `True` and `previous_result` is `None`.

### @idempotent decorator

```python
from src.idempotency import idempotent

@idempotent(store=store, key_fn=lambda order_id, **_: f"create-order-{order_id}")
async def create_order(order_id: str, payload: dict) -> dict:
    # Runs once per order_id, however many times it is called
    ...

# Without key_fn, the first positional argument is used as the key
@idempotent(store=store)
async def process_payment(message_id: str, payload: dict) -> dict:
    ...
```

### Transactional outbox

With the outbox, a database write and a Kafka publish either both happen or neither does. You write the outbox row in the same transaction as your data, and a background relay publishes it.

```python
from src.idempotency.outbox import OutboxEntry, OutboxWriter, OutboxRelay

writer = OutboxWriter()
async with pool.acquire() as conn, conn.transaction():
    await conn.execute("INSERT INTO orders ...")
    await writer.write(conn, OutboxEntry(topic="orders", message_id="order-123", payload={...}))

relay = OutboxRelay(pool=pool, producer=producer)
relay_task = asyncio.create_task(relay.start())   # start() runs the polling loop until stop()
...
await relay.stop()
```

The outbox table schema is in `scripts/init_db.sql`.

### Implementing a custom DeduplicationStore

The `DeduplicationStore` abstract base class in `src/dedup/store.py` defines the full interface. Implement it to back deduplication with any storage system.

```python
from typing import Any

from src.dedup.models import ProcessingStatus
from src.dedup.store import DeduplicationStore

class MyStore(DeduplicationStore):
    async def claim(self, message_id: str) -> bool: ...
    async def is_duplicate(self, message_id: str) -> bool: ...
    async def mark_completed(self, message_id: str, result: Any = None) -> None: ...
    async def mark_failed(self, message_id: str, error: str, attempts: int = 1) -> None: ...
    async def get_status(self, message_id: str) -> ProcessingStatus | None: ...
    async def delete(self, message_id: str) -> None: ...
    async def close(self) -> None: ...
```

`claim()` must be atomic. When several callers race on the same `message_id`, exactly one of them may get `True`.

---

## Testing

The test suite has four layers, each with different infrastructure needs.

### Unit and chaos tests (no Docker)

```bash
pytest tests/unit tests/chaos -v
```

These tests run against `fakeredis`, with Lua support from `lupa`, instead of a real Redis. The chaos tests cover thundering-herd claims (up to 500 concurrent claimers), races between Lua status transitions, crashes mid-handle, intermittent handler failures and store exceptions.

### Integration tests (requires Docker)

```bash
pytest tests/integration -m integration --no-cov -v
```

`testcontainers` starts real Redis, Kafka and PostgreSQL containers, and the tests run the store, consumer and outbox against them.

### End-to-end tests (requires Docker)

```bash
pytest tests/e2e -m e2e --no-cov -v
```

These tests start all three services with `testcontainers`, produce real Kafka messages, run the consumer, and check for exactly-once processing across the full stack.

### Coverage

The pytest defaults in `pyproject.toml` enforce **85%** coverage, measured against the unit and chaos suites. Pass `--no-cov` when you run the integration or e2e suites on their own. The coverage measurement leaves out the handler stubs (`src/consumer/handlers/`), `src/api.py` and `src/logging_config.py`.

```bash
pytest tests/unit tests/chaos --cov-report=html
open htmlcov/index.html
```

---

## Observability

### Health and metrics endpoints

The FastAPI service (`uvicorn src.api:app --port 9090`) exposes:

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check (always 200 while the process is alive) |
| `GET /ready` | Readiness check (verifies Redis connectivity) |
| `GET /metrics` | Prometheus metrics |

### Prometheus metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `dedup_messages_total` | Counter | `topic`, `status` | Messages seen; `status` is `processed`, `duplicate`, `claim_lost`, `no_id` or `dlq` |
| `dedup_processing_duration_seconds` | Histogram | `topic` | Time from claim to commit |
| `dedup_store_operation_duration_seconds` | Histogram | `operation` | Latency of each store operation |
| `dedup_active_processing` | Gauge | `topic` | Messages currently between claim and commit |
| `dedup_retry_attempts_total` | Counter | `topic` | Retry attempts (excludes the first attempt) |
| `dedup_dlq_messages_total` | Counter | `topic` | Messages routed to the DLQ |
| `dedup_consumer_errors_total` | Counter | `topic`, `error_type` | Handler and consumer errors |

### Grafana dashboard

The **Kafka Deduplication Framework** dashboard is provisioned automatically at http://localhost:3000. It shows the duplicate rate, DLQ volume, processing latency percentiles, Redis operation latency, throughput by status, consumer errors by type and the retry rate.

### Alerting

Alert rules live in `prometheus/alert_rules.yml`:

- `HighDuplicateRate` — an unusually high share of duplicate messages on a topic
- `DLQMessagesIncreasing` — messages are being routed to the DLQ
- `HighProcessingLatency` — message processing is slow
- `HighStoreOperationLatency` — Redis dedup store operations are slow
- `ConsumerErrorSpike` — consumer errors are spiking
- `ConsumerStalled` — no messages processed on a topic for 10 minutes

Alertmanager routing is configured in `prometheus/alertmanager.yml`.

### Structured logging

`structlog` (`src/logging_config.py`) writes logs as JSON. The consumer includes `message_id`, `topic`, `offset` and the attempt number in its log events.

---

## Kubernetes Deployment

The manifests are in `k8s/` and managed with Kustomize. Before you deploy:

1. Replace `ghcr.io/YOUR_ORG/kafkadedup` in `k8s/kustomization.yaml` and the deployment manifests with your image.
2. Put your base64-encoded credentials in `k8s/secret.yaml`. **Do not commit real secrets.**
3. Set your domain in `k8s/ingress.yaml`. It is `dedup.example.com` by default.

```bash
kubectl apply -k k8s/

kubectl rollout status deployment/kafkadedup-api -n kafkadedup
kubectl rollout status deployment/kafkadedup-consumer -n kafkadedup
```

| File | Description |
|---|---|
| `namespace.yaml` | `kafkadedup` namespace |
| `deployment-api.yaml` | FastAPI metrics/health deployment |
| `service-api.yaml` | Service for the API |
| `ingress.yaml` | Ingress for the API (cert-manager TLS) |
| `deployment-consumer.yaml` | Kafka consumer deployment |
| `hpa-consumer.yaml` | Horizontal Pod Autoscaler for the consumer (2–12 replicas; cap at the partition count) |
| `poddisruptionbudget.yaml` | Keeps consumer and API replicas available during disruptions |
| `configmap.yaml` | Non-secret configuration |
| `secret.yaml` | Redis URL, Postgres DSN and Kafka credentials (placeholders) |
| `servicemonitor.yaml` | Prometheus Operator ServiceMonitor |
| `rbac.yaml` | Service account and RBAC roles |
| `kustomization.yaml` | Ties the manifests together and sets the image tag |

---

## CI/CD

GitHub Actions workflows live in `.github/workflows/`:

- **CI** (`ci.yml`) runs on every pull request and every push to `main`. It runs `ruff` lint and format checks and `mypy --strict`, then the unit and chaos tests with the 85% coverage gate, then the Docker-backed integration tests.
- **CD** (`cd.yml`) runs on every push to `main`. It reuses CI as a gate, builds a multi-arch (`amd64`/`arm64`) image, pushes it to GHCR, deploys to Kubernetes and smoke-tests `/health`. It needs a `KUBE_CONFIG` repository secret and a `production` environment.

---

## Design Decisions

**Why Redis for deduplication and not the database?**
Redis `SET NX` is a single atomic round trip. A database `INSERT ... ON CONFLICT` also works, but it adds latency and load to the database on the hot path. Redis suits short-lived, high-throughput key checks with TTL-based cleanup.

**Why `SET NX` for claims and Lua for transitions?**
A Redis `GET` followed by a `SET` takes two operations and leaves a TOCTOU (time-of-check to time-of-use) race. The claim is a single conditional `SET NX PX`, so it is atomic without a script. Status transitions need to read a value and then write it, so they run as a Lua script. The script executes atomically on the server and refuses to overwrite a record that is no longer `PROCESSING`.

**Why commit the offset only after mark_completed?**
If the offset is committed before the handler finishes, a crash leaves the message unprocessed but already committed, and the message is lost for good. Committing after the handler guarantees that Kafka redelivers the message after a crash, and the dedup layer absorbs the resulting duplicate.

**Why not use Kafka's built-in idempotent producer?**
The idempotent producer only prevents duplicates from a single producer session. It doesn't help when the same logical event is published from several sessions, retried by upstream systems, or reprocessed after a consumer crash. You need application-level deduplication either way.

**Why TTL-based cleanup instead of explicit deletion?**
Explicit deletion needs a separate cleanup job and risks leaving orphaned records behind. A TTL cleans up automatically and can be tuned per store. The 24-hour default covers realistic retry windows without letting Redis grow without limit.

---

## Contributing

The framework owns the deduplication infrastructure. Business logic handlers (`src/consumer/handlers/`) belong to users and don't count toward the coverage requirement. To extend the framework:

1. Fork the repository and create a feature branch.
2. Run `ruff check src tests`, `ruff format --check src tests` and `mypy src/` before you open a PR.
3. Add unit tests for new infrastructure code, and integration tests for storage changes.
4. Keep the coverage threshold at 85% or higher.

---

## License

MIT
