"""
Testcontainers fixtures for integration tests.

Containers are session-scoped (started once per pytest session) so the
startup cost is paid only once.  Per-test state (connections, stores) uses
function scope so each test gets a clean slate.
"""

from __future__ import annotations

import asyncpg
import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from src.dedup import RedisDeduplicationStore

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def redis_container():
    """Start a real Redis 7.2 container for the test session."""
    with RedisContainer("redis:7.2-alpine") as container:
        yield container


@pytest.fixture
async def redis_store(redis_container):
    """
    RedisDeduplicationStore connected to the session-scoped container.
    Each test gets a fresh logical database (FLUSHDB after test).
    """
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    store = RedisDeduplicationStore.from_url(url)
    await store.connect()
    yield store

    # Clean up: flush the DB so the next test starts empty
    await store._client.flushdb()
    await store.close()


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container():
    """Start a real Kafka container for the test session."""
    with KafkaContainer() as container:
        yield container


@pytest.fixture
def kafka_bootstrap(kafka_container) -> str:
    """Return the external bootstrap-servers string for the running container."""
    return kafka_container.get_bootstrap_server()


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container():
    """Start a real Postgres 16 container and initialise the schema."""
    with PostgresContainer(
        image="postgres:16-alpine",
        username="dedup",
        password="dedup_secret",
        dbname="kafkadedup",
    ) as container:
        yield container


@pytest.fixture
async def pg_pool(postgres_container):
    """
    asyncpg connection pool pointing at the test Postgres container.
    Schema is created fresh per test (inside a transaction that is rolled back).
    """
    dsn = postgres_container.get_connection_url()
    # asyncpg uses plain postgresql:// (not postgresql+psycopg2://)
    dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")

    pool = await asyncpg.create_pool(dsn)

    # Create schema
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id           BIGSERIAL PRIMARY KEY,
                topic        TEXT        NOT NULL,
                message_id   TEXT        NOT NULL,
                payload      JSONB       NOT NULL,
                status       TEXT        NOT NULL DEFAULT 'PENDING',
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                published_at TIMESTAMPTZ
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_message_id
                ON outbox (message_id);
        """)

    yield pool

    # Truncate tables between tests
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE outbox RESTART IDENTITY")

    await pool.close()
