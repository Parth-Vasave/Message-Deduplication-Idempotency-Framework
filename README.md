# Message Deduplication & Idempotency Framework

A production-grade deduplication and idempotency framework for Kafka consumers written in Python. Kafka guarantees at-least-once delivery, meaning network hiccups, broker failovers, and consumer crashes all produce duplicate messages. This framework ensures each message is processed exactly once, preventing double-orders, double-charges, and data corruption.

---

## Table of Contents

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
- [Design Decisions](#design-decisions)

---

## Architecture Overview

```
Kafka Topic
    |
    v
DedupConsumer
    |
    |-- Extract message_id
    |-- Check Redis (is_duplicate?)
    |       |-- YES --> skip, commit offset
    |       |-- NO  --> claim atomically (Lua script)
    |
    |-- Execute business logic (idempotent handler)
    |
    |-- mark_completed()  --|
    |-- Commit offset      |-- success path
    |
    |-- mark_failed()     --|
    |-- Retry / DLQ        |-- error path
```

The Redis deduplication store is the single source of truth. All check-and-set operations are performed atomically via Lua scripts, preventing any race condition between concurrent consumer instances.

---

## How It Works

### Message ID Schema

Every message must carry a structured identifier:

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

- `PROCESSING` — claimed by a consumer; no other instance will process this message.
- `COMPLETED` — business logic ran successfully; result is stored.
- `FAILED` — handler raised an exception; eligible for retry up to `max_retries`.
- `SKIPPED` — duplicate detected; offset committed without reprocessing.

### Atomic Claim

The core safety guarantee is the atomic `claim()` operation. The Redis-backed store uses a Lua script to perform a conditional SET in a single round-trip, so two consumer instances racing on the same message_id can never both win.

---

## Project Structure

```
kafkadeduplication/
├── src/
│   ├── dedup/
│   │   ├── store.py            # Abstract DeduplicationStore interface
│   │   ├── redis_store.py      # Redis-backed implementation (Lua scripts)
│   │   ├── memory_store.py     # In-memory implementation for dev/test
│   │   └── models.py           # ProcessingStatus, DeduplicationConfig (Pydantic)
│   ├── consumer/
│   │   ├── base.py             # BaseConsumer (abstract)
│   │   ├── dedup_consumer.py   # DedupConsumer — full dedup lifecycle
│   │   └── handlers/
│   │       ├── order_handler.py
│   │       ├── payment_handler.py
│   │       └── shipment_handler.py
│   ├── idempotency/
│   │   ├── guard.py            # IdempotencyGuard context manager + @idempotent decorator
│   │   └── outbox.py           # Transactional outbox pattern helper
│   ├── metrics/
│   │   └── dedup_metrics.py    # Prometheus counters, histograms, gauges
│   ├── api.py                  # FastAPI health + metrics endpoints
│   ├── config.py               # Pydantic settings (reads from environment)
│   └── logging_config.py       # Structured logging via structlog
├── tests/
│   ├── unit/                   # Pure unit tests, no Docker required
│   ├── integration/            # Real Redis, Kafka, Postgres via testcontainers
│   ├── chaos/                  # Fault-injection and resilience tests
│   └── e2e/                    # Full-stack tests requiring all services
├── k8s/                        # Kubernetes manifests (Kustomize)
├── grafana/                    # Dashboard provisioning JSON
├── prometheus/                 # prometheus.yml, alert_rules.yml, alertmanager.yml
├── scripts/
│   └── seed_kafka.py           # Seed topics with sample messages
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
| Database | PostgreSQL 16 via `asyncpg` |
| API / Health | FastAPI + Uvicorn |
| Metrics | `prometheus-client` |
| Tracing | `opentelemetry-sdk` |
| Logging | `structlog` (structured JSON) |
| Container | Docker + Docker Compose |
| Orchestration | Kubernetes (Kustomize manifests) |
| Testing | `pytest`, `pytest-asyncio`, `testcontainers` |
| Linting | `ruff`, `mypy` (strict) |

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker and Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/your-username/kafka-dedup.git
cd kafka-dedup
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your values if needed — defaults work for local Docker
```

### 3. Start all services

```bash
docker compose up -d
```

This starts: Zookeeper, Kafka, Redis, PostgreSQL, the FastAPI metrics API, Prometheus, Alertmanager, and Grafana.

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| FastAPI metrics/health | http://localhost:9090 |
| Prometheus | http://localhost:9091 |
| Alertmanager | http://localhost:9093 |
| Grafana | http://localhost:3000 |

Grafana default credentials: `admin` / `dedup_secret`

### 4. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 5. Run the consumer

```bash
python -m src.consumer.dedup_consumer
```

---

## Configuration

All settings are read from environment variables (or a `.env` file) via Pydantic Settings.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Kafka broker address |
| `KAFKA_GROUP_ID` | `dedup-consumer-group` | Consumer group ID |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `POSTGRES_DSN` | `postgresql://dedup:dedup_secret@localhost:5432/kafkadedup` | PostgreSQL DSN |
| `DEDUP_TTL_SECONDS` | `86400` | How long dedup records are retained (seconds) |
| `DEDUP_MAX_RETRIES` | `3` | Max retry attempts before sending to DLQ |
| `DEDUP_RETRY_BACKOFF_MS` | `500` | Backoff delay between retries |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## Usage

### DedupConsumer

Subclass `DedupConsumer` and implement `handle_message`. The framework handles the entire dedup lifecycle automatically.

```python
from src.consumer.dedup_consumer import DedupConsumer
from src.dedup.redis_store import RedisDeduplicationStore

class OrderConsumer(DedupConsumer):
    async def handle_message(self, message_id: str, payload: dict) -> dict:
        # Your business logic here — will never run twice for the same message_id
        order = await create_order(payload)
        return {"order_id": order.id}

store = RedisDeduplicationStore(redis_url="redis://localhost:6379/0")
consumer = OrderConsumer(
    topics=["orders"],
    bootstrap_servers="localhost:9094",
    group_id="order-consumer-group",
    store=store,
)
await consumer.run()
```

### IdempotencyGuard (context manager)

Use directly when you need idempotency outside of the Kafka consumer lifecycle.

```python
from src.idempotency.guard import IdempotencyGuard

async with IdempotencyGuard(store, idempotency_key="payment-abc-123") as guard:
    if guard.already_processed:
        return guard.previous_result
    result = await charge_payment(amount=99.00)
    guard.result = result
```

### @idempotent decorator

```python
from src.idempotency.guard import idempotent

@idempotent(store=store, key_fn=lambda order_id, **_: f"create-order-{order_id}")
async def create_order(order_id: str, payload: dict) -> dict:
    # Runs exactly once per order_id regardless of how many times it is called
    ...
```

### Implementing a custom DeduplicationStore

The `DeduplicationStore` abstract base class in `src/dedup/store.py` defines the full interface. Implement it to back deduplication with any storage system.

```python
from src.dedup.store import DeduplicationStore

class MyStore(DeduplicationStore):
    async def claim(self, message_id: str) -> bool: ...
    async def is_duplicate(self, message_id: str) -> bool: ...
    async def mark_completed(self, message_id: str, result=None) -> None: ...
    async def mark_failed(self, message_id: str, error: str, attempts: int = 1) -> None: ...
    async def get_status(self, message_id: str): ...
    async def delete(self, message_id: str) -> None: ...
```

---

## Testing

The test suite is split into four layers with different infrastructure requirements.

### Unit and chaos tests (no Docker)

```bash
pytest tests/unit tests/chaos -v
```

Runs with `fakeredis` in place of a real Redis instance. Chaos tests inject faults (network errors, Redis timeouts, partial failures) to verify the state machine behaves correctly under adversarial conditions.

### Integration tests (requires Docker)

```bash
pytest tests/integration -m integration -v
```

Spins up real Redis, Kafka, and PostgreSQL containers via `testcontainers-python`. Tests the full claim/complete/fail lifecycle against actual services.

### End-to-end tests (requires all services running)

```bash
docker compose up -d
pytest tests/e2e -m e2e -v
```

Produces real Kafka messages, runs the consumer, and asserts exactly-once delivery across the full stack.

### Coverage

The default `pytest` run enforces 85% coverage minimum. Business logic handler stubs (`src/consumer/handlers/`) are excluded since they are user-owned.

```bash
pytest --cov=src --cov-report=html
open htmlcov/index.html
```

---

## Observability

### Health and metrics endpoints

The FastAPI service exposes:

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /ready` | Readiness check (verifies Redis and Kafka connectivity) |
| `GET /metrics` | Prometheus metrics |

### Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `dedup_messages_total` | Counter | Total messages seen, labeled by `status` (processed, duplicate, failed) |
| `dedup_processing_duration_seconds` | Histogram | Handler execution time |
| `dedup_store_operation_duration_seconds` | Histogram | Redis operation latency |
| `dedup_active_processing` | Gauge | Messages currently in PROCESSING state |
| `dedup_retry_attempts_total` | Counter | Total retry attempts |

### Grafana dashboard

A pre-built dashboard is provisioned automatically at `http://localhost:3000`. It shows duplicate rate, processing throughput, Redis latency, retry rate, and consumer lag.

### Alerting

Alertmanager rules are defined in `prometheus/alert_rules.yml`. Default alerts:

- `HighDuplicateRate` — duplicate rate exceeds 5% over 5 minutes
- `HighFailureRate` — failure rate exceeds 1% over 5 minutes
- `ConsumerLag` — Kafka consumer lag exceeds threshold
- `RedisLatencyHigh` — p99 Redis latency exceeds 100ms

### Structured logging

All log output is JSON via `structlog`, including `message_id`, `status`, `duration_ms`, and `attempt` on every event.

---

## Kubernetes Deployment

Manifests are in `k8s/` and managed with Kustomize.

```bash
# Deploy to a cluster
kubectl apply -k k8s/

# Check rollout
kubectl rollout status deployment/dedup-api -n kafkadedup
kubectl rollout status deployment/dedup-consumer -n kafkadedup
```

Key manifests:

| File | Description |
|---|---|
| `namespace.yaml` | `kafkadedup` namespace |
| `deployment-api.yaml` | FastAPI metrics/health service |
| `deployment-consumer.yaml` | Kafka consumer deployment |
| `hpa-consumer.yaml` | Horizontal Pod Autoscaler for consumer |
| `configmap.yaml` | Non-secret configuration |
| `secret.yaml` | Redis URL, Postgres DSN, Kafka credentials |
| `servicemonitor.yaml` | Prometheus ServiceMonitor for scraping |
| `poddisruptionbudget.yaml` | Ensures at least one consumer replica stays up during rollouts |
| `rbac.yaml` | RBAC roles for the consumer service account |

Before deploying, update `k8s/secret.yaml` with your base64-encoded production credentials. Do not commit real secrets to source control.

---

## Design Decisions

**Why Redis for deduplication and not the database?**
Redis SET NX is a single-round-trip atomic operation. A database `INSERT ... ON CONFLICT` works but adds latency and database load on the hot path. Redis is the right tool for ephemeral, high-throughput key checks with TTL-based cleanup.

**Why Lua scripts for atomicity?**
A Redis GET followed by a SET is two operations and has a TOCTOU race. A Lua script executes atomically on the Redis server, eliminating the window between check and claim.

**Why commit the offset only after mark_completed?**
Committing before the handler finishes means a crash leaves the message unprocessed but the offset committed — permanent loss. Committing after guarantees redelivery on crash. The dedup layer handles the resulting duplicate on the redelivered message.

**Why not use Kafka's built-in idempotent producer?**
The idempotent producer prevents duplicates on the producer side within a single session. It does not help when the same logical event is published multiple times from different sessions, retried by upstream systems, or reprocessed from a consumer crash. Application-level deduplication is required regardless.

**Why TTL-based cleanup instead of explicit deletion?**
Explicit deletion requires a separate cleanup job and risks leaving orphaned records. TTL is automatic and tunable per topic. 24 hours covers all realistic retry windows without unbounded growth.

---

## Contributing

This framework owns the deduplication infrastructure. Business logic handlers (`src/consumer/handlers/`) are user-owned and excluded from coverage requirements. When extending the framework:

1. Fork and create a feature branch.
2. Run `ruff check` and `mypy src/` before opening a PR.
3. New infrastructure code requires unit tests; integration tests for storage changes.
4. Do not lower the 85% coverage threshold.

---

## License

MIT
