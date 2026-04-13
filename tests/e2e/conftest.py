"""
E2E test fixtures — all three services (Kafka, Redis, Postgres) running together.

Containers are session-scoped; per-test state (topics, DB rows) is cleaned
between tests using truncation and unique topic names.
"""

from __future__ import annotations

import asyncpg
import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from src.dedup import RedisDeduplicationStore

# Re-use session-scoped container fixtures from integration conftest
# (pytest discovers conftest.py fixtures transitively)


@pytest.fixture(scope="session")
def e2e_redis_container():
    with RedisContainer("redis:7.2-alpine") as c:
        yield c


@pytest.fixture(scope="session")
def e2e_kafka_container():
    with KafkaContainer() as c:
        yield c


@pytest.fixture(scope="session")
def e2e_postgres_container():
    with PostgresContainer(
        image="postgres:16-alpine",
        username="dedup",
        password="dedup_secret",
        dbname="kafkadedup",
    ) as c:
        yield c


@pytest.fixture
async def e2e_redis_store(e2e_redis_container):
    host = e2e_redis_container.get_container_host_ip()
    port = e2e_redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    store = RedisDeduplicationStore.from_url(url)
    await store.connect()
    yield store

    await store._client.flushdb()
    await store.close()


@pytest.fixture
def e2e_bootstrap(e2e_kafka_container) -> str:
    return e2e_kafka_container.get_bootstrap_server()


@pytest.fixture
async def e2e_pg_pool(e2e_postgres_container):
    dsn = e2e_postgres_container.get_connection_url()
    dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")

    pool = await asyncpg.create_pool(dsn)

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id               BIGSERIAL PRIMARY KEY,
                dedup_message_id TEXT       UNIQUE NOT NULL,
                customer_id      TEXT       NOT NULL,
                amount           NUMERIC(12,2) NOT NULL,
                status           TEXT       NOT NULL DEFAULT 'CREATED',
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
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

    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE orders, outbox RESTART IDENTITY CASCADE")

    await pool.close()
