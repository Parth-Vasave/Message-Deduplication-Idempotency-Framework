"""
Transactional Outbox Pattern helper.

The outbox pattern solves the dual-write problem: you want to update your
database AND publish a Kafka message atomically. Without it, a crash between
the two writes leaves the system in an inconsistent state.

How it works:
  1. In the same DB transaction that mutates your data, INSERT a row into the
     ``outbox`` table.
  2. A background relay reads unprocessed outbox rows and publishes them to
     Kafka, then marks the row PUBLISHED.
  3. The relay can be safely retried — Kafka dedup handles duplicate publishes.

This module provides:
  - ``OutboxEntry``  — Pydantic model for an outbox row
  - ``OutboxWriter`` — writes entries inside a ``asyncpg`` transaction
  - ``OutboxRelay``  — background task that publishes pending entries

Schema (see scripts/init_db.sql for DDL):
    outbox (
        id          BIGSERIAL PRIMARY KEY,
        topic       TEXT NOT NULL,
        message_id  TEXT NOT NULL UNIQUE,
        payload     JSONB NOT NULL,
        status      TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | PUBLISHED | FAILED
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        published_at TIMESTAMPTZ
    )
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


class OutboxEntry(BaseModel):
    topic: str
    message_id: str
    payload: dict[str, Any]
    created_at: datetime = None  # type: ignore[assignment]

    def model_post_init(self, __context: Any) -> None:
        if self.created_at is None:
            object.__setattr__(self, "created_at", datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Writer  (use inside your business-logic transaction)
# ---------------------------------------------------------------------------


class OutboxWriter:
    """
    Inserts outbox entries into the DB within an existing asyncpg connection.

    Usage::

        async with pool.acquire() as conn:
            async with conn.transaction():
                await writer.write(conn, OutboxEntry(
                    topic="orders",
                    message_id=f"order-{order_id}",
                    payload={"order_id": order_id, ...},
                ))
                # ... your other DB writes ...
    """

    _INSERT_SQL = """
        INSERT INTO outbox (topic, message_id, payload, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (message_id) DO NOTHING
    """

    async def write(self, conn: Any, entry: OutboxEntry) -> None:
        await conn.execute(
            self._INSERT_SQL,
            entry.topic,
            entry.message_id,
            json.dumps(entry.payload),
            entry.created_at,
        )
        logger.debug("Outbox entry written message_id=%s topic=%s", entry.message_id, entry.topic)


# ---------------------------------------------------------------------------
# Relay  (background task — polls the outbox and publishes to Kafka)
# ---------------------------------------------------------------------------


class OutboxRelay:
    """
    Background relay that reads PENDING outbox rows and publishes them to Kafka.

    Dependencies injected at construction time keep this easily testable.

    Args:
        pool:        asyncpg connection pool
        producer:    AIOKafkaProducer (or any object with
                     ``send_and_wait(topic, value, key)`` coroutine)
        poll_interval_s: how often to check for new rows (default 1 s)
        batch_size:      max rows to process per poll cycle (default 100)
    """

    _FETCH_SQL = """
        SELECT id, topic, message_id, payload
        FROM outbox
        WHERE status = 'PENDING'
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    """

    _MARK_PUBLISHED_SQL = """
        UPDATE outbox
        SET status = 'PUBLISHED', published_at = $1
        WHERE id = $2
    """

    _MARK_FAILED_SQL = """
        UPDATE outbox SET status = 'FAILED' WHERE id = $1
    """

    def __init__(
        self,
        pool: Any,
        producer: Any,
        poll_interval_s: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._pool = pool
        self._producer = producer
        self._poll_interval = poll_interval_s
        self._batch_size = batch_size
        self._running = False

    async def start(self) -> None:
        self._running = True
        logger.info("OutboxRelay started poll_interval=%.1fs", self._poll_interval)
        await self._loop()

    async def stop(self) -> None:
        self._running = False
        logger.info("OutboxRelay stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                published = await self._process_batch()
                if published == 0:
                    await asyncio.sleep(self._poll_interval)
            except Exception as exc:  # noqa: BLE001
                logger.exception("OutboxRelay error: %s", exc)
                await asyncio.sleep(self._poll_interval)

    async def _process_batch(self) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(self._FETCH_SQL, self._batch_size)
                published = 0
                for row in rows:
                    try:
                        payload_bytes = row["payload"].encode()
                        key_bytes = row["message_id"].encode()
                        await self._producer.send_and_wait(
                            row["topic"],
                            value=payload_bytes,
                            key=key_bytes,
                        )
                        await conn.execute(
                            self._MARK_PUBLISHED_SQL,
                            datetime.now(timezone.utc),
                            row["id"],
                        )
                        published += 1
                        logger.debug(
                            "Outbox published message_id=%s topic=%s",
                            row["message_id"], row["topic"],
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Outbox failed to publish message_id=%s: %s",
                            row["message_id"], exc,
                        )
                        await conn.execute(self._MARK_FAILED_SQL, row["id"])
                return published
