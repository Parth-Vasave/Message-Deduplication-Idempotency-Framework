"""Unit tests for the ``python -m src.consumer`` entrypoint (no Kafka / Redis)."""

import asyncio
import os
import signal
from unittest.mock import AsyncMock

import pytest

from src.consumer import __main__ as entrypoint
from src.consumer.dedup_consumer import DedupConsumer
from src.dedup import InMemoryDeduplicationStore


@pytest.fixture
def lifecycle(monkeypatch):
    monkeypatch.setattr(
        entrypoint.RedisDeduplicationStore,
        "from_url",
        classmethod(lambda cls, url, config=None: InMemoryDeduplicationStore(config)),
    )
    start = AsyncMock()
    stop = AsyncMock()
    monkeypatch.setattr(DedupConsumer, "start", start)
    monkeypatch.setattr(DedupConsumer, "stop", stop)
    return start, stop


async def test_sigterm_stops_all_consumers(lifecycle, monkeypatch):
    start, stop = lifecycle

    async def run_forever(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(DedupConsumer, "run", run_forever)
    asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGTERM)

    assert await entrypoint.main() == 0
    assert start.await_count == 3
    assert stop.await_count == 3


async def test_crashed_consumer_exits_non_zero(lifecycle, monkeypatch):
    _, stop = lifecycle

    async def crash(self):
        raise ConnectionError("Kafka gone")

    monkeypatch.setattr(DedupConsumer, "run", crash)

    assert await entrypoint.main() == 1
    assert stop.await_count == 3


async def test_consumers_use_settings_config(lifecycle, monkeypatch):
    monkeypatch.setattr(entrypoint.settings, "dedup_max_retries", 7)
    seen = []

    async def record_and_crash(self):
        seen.append((self._config.max_retries, self._store._config.max_retries))
        raise RuntimeError("stop")

    monkeypatch.setattr(DedupConsumer, "run", record_and_crash)

    await entrypoint.main()
    assert seen and all(pair == (7, 7) for pair in seen)
