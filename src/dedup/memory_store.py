"""
In-memory DeduplicationStore — for local development and unit tests.

Thread-safe via asyncio.Lock (single event loop assumed).
No TTL enforcement — records live until process exit or explicit delete().
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from .models import DeduplicationConfig, ProcessingStatus, StatusValue
from .store import DeduplicationStore

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


class InMemoryDeduplicationStore(DeduplicationStore):
    def __init__(self, config: DeduplicationConfig | None = None) -> None:
        self._store: dict[str, ProcessingStatus] = {}
        self._lock = asyncio.Lock()
        self._config = config or DeduplicationConfig()

    async def claim(self, message_id: str) -> bool:
        async with self._lock:
            if message_id in self._store:
                existing = self._store[message_id]
                # Allow re-claim if previous attempt failed and retry_failed is on
                if (
                    existing.is_failed()
                    and self._config.retry_failed
                    and existing.attempts < self._config.max_retries
                ):
                    self._store[message_id] = ProcessingStatus(
                        message_id=message_id,
                        status=StatusValue.PROCESSING,
                        attempts=existing.attempts + 1,
                        created_at=existing.created_at,
                        updated_at=_now(),
                    )
                    logger.debug("Re-claimed for retry message_id=%s", message_id)
                    return True
                logger.debug("Duplicate detected message_id=%s", message_id)
                return False

            self._store[message_id] = ProcessingStatus(
                message_id=message_id,
                status=StatusValue.PROCESSING,
                created_at=_now(),
                updated_at=_now(),
            )
            logger.debug("Claimed message_id=%s", message_id)
            return True

    async def is_duplicate(self, message_id: str) -> bool:
        return message_id in self._store

    async def mark_completed(self, message_id: str, result: Any = None) -> None:
        async with self._lock:
            record = self._store.get(message_id)
            if record is None or record.status != StatusValue.PROCESSING:
                logger.warning(
                    "mark_completed called on non-PROCESSING record message_id=%s", message_id
                )
                return
            record.status = StatusValue.COMPLETED
            record.result = result
            record.updated_at = _now()

    async def mark_failed(self, message_id: str, error: str, attempts: int = 1) -> None:
        async with self._lock:
            record = self._store.get(message_id)
            if record is None:
                logger.warning("mark_failed called on missing record message_id=%s", message_id)
                return
            record.status = StatusValue.FAILED
            record.error = error
            record.attempts = attempts
            record.updated_at = _now()

    async def get_status(self, message_id: str) -> ProcessingStatus | None:
        return self._store.get(message_id)

    async def delete(self, message_id: str) -> None:
        async with self._lock:
            self._store.pop(message_id, None)

    def size(self) -> int:
        """Convenience for tests."""
        return len(self._store)

    def clear(self) -> None:
        """Wipe all records — use between tests."""
        self._store.clear()

    async def close(self) -> None:
        """No-op: in-memory store holds no external resources."""
