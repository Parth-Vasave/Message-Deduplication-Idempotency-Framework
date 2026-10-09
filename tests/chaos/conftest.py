"""
Shared fixtures for chaos / resilience tests.

Chaos tests run without Docker containers — they use fakeredis (with Lua
support via lupa) and unittest.mock for external dependencies.  This keeps
them fast and deterministic while still exercising real concurrency logic.
"""

from __future__ import annotations

import fakeredis
import fakeredis.aioredis
import pytest

from src.dedup import InMemoryDeduplicationStore, RedisDeduplicationStore


@pytest.fixture
def memory_store() -> InMemoryDeduplicationStore:
    return InMemoryDeduplicationStore()


@pytest.fixture
async def fake_redis_store() -> RedisDeduplicationStore:
    """
    RedisDeduplicationStore backed by fakeredis with Lua support (lupa required).
    Tests atomicity of Lua scripts without a real Redis daemon.
    """
    server = fakeredis.FakeServer(version=(7, 2))
    # redis-py >= 8 caps the pool at 100 connections and raises MaxConnectionsError
    # instead of waiting; size it above the largest thundering-herd fan-out (500).
    client = fakeredis.aioredis.FakeRedis(
        server=server, decode_responses=True, max_connections=1000
    )
    store = RedisDeduplicationStore(client)
    await store.connect()
    yield store
    await store.close()
