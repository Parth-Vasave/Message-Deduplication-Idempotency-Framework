"""
Idempotency utilities: @idempotent decorator and IdempotencyGuard context manager.

Both wrap arbitrary async business logic with the standard dedup lifecycle:
  1. claim() the message_id
  2. execute the handler
  3. mark_completed() or mark_failed()

The guard / decorator are independent of Kafka — they work anywhere you have
an idempotency_key and a DeduplicationStore.
"""

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from src.dedup.models import DeduplicationConfig, StatusValue
from src.dedup.store import DeduplicationStore

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class IdempotencyGuard:
    """
    Async context manager that wraps a block with claim / complete / fail.

    Usage::

        async with IdempotencyGuard(store, idempotency_key) as guard:
            if guard.already_processed:
                return guard.previous_result
            result = await do_work()
            guard.result = result          # stored on __aexit__

    Attributes:
        already_processed: True if the message was already COMPLETED.
        previous_result:   The stored result from the prior run (if any).
        result:            Set by the caller before exiting to persist it.
    """

    def __init__(
        self,
        store: DeduplicationStore,
        idempotency_key: str,
        config: DeduplicationConfig | None = None,
    ) -> None:
        self._store = store
        self._key = idempotency_key
        self._config = config or DeduplicationConfig()

        self.already_processed: bool = False
        self.previous_result: Any = None
        self.result: Any = None

    async def __aenter__(self) -> "IdempotencyGuard":
        existing = await self._store.get_status(self._key)

        if existing is not None and existing.status == StatusValue.COMPLETED:
            self.already_processed = True
            self.previous_result = existing.result
            logger.debug("IdempotencyGuard: already processed key=%s", self._key)
            return self

        claimed = await self._store.claim(self._key)
        if not claimed:
            # Someone else is currently PROCESSING — treat as already handled
            self.already_processed = True
            logger.debug("IdempotencyGuard: claim lost key=%s", self._key)

        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if self.already_processed:
            return False  # don't suppress exceptions from the caller's block

        if exc_type is not None:
            error_msg = f"{exc_type.__name__}: {exc_val}"
            await self._store.mark_failed(self._key, error=error_msg)
            logger.warning("IdempotencyGuard: marked failed key=%s error=%s", self._key, error_msg)
            return False  # propagate the exception

        await self._store.mark_completed(self._key, result=self.result)
        logger.debug("IdempotencyGuard: marked completed key=%s", self._key)
        return False


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def idempotent(
    store: DeduplicationStore,
    key_fn: Callable[..., str] | None = None,
    config: DeduplicationConfig | None = None,
) -> Callable[[F], F]:
    """
    Decorator that makes an async function idempotent.

    Args:
        store:  DeduplicationStore instance.
        key_fn: Callable that receives the same *args/**kwargs as the decorated
                function and returns the idempotency key string.
                Defaults to the first positional argument (message_id pattern).
        config: Optional DeduplicationConfig.

    Usage::

        @idempotent(store=redis_store, key_fn=lambda msg: msg["id"])
        async def process_order(msg: dict) -> dict:
            ...

        # Or with the default (first arg as key):
        @idempotent(store=redis_store)
        async def process_payment(message_id: str, payload: dict) -> dict:
            ...
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            if key_fn is not None:
                idempotency_key = key_fn(*args, **kwargs)
            elif args:
                idempotency_key = str(args[0])
            else:
                raise ValueError(
                    "@idempotent: cannot derive idempotency key — "
                    "supply key_fn or pass the key as the first positional argument"
                )

            existing = await store.get_status(idempotency_key)
            if existing is not None and existing.status == StatusValue.COMPLETED:
                logger.debug(
                    "@idempotent: returning cached result key=%s", idempotency_key
                )
                return existing.result

            claimed = await store.claim(idempotency_key)
            if not claimed:
                logger.debug(
                    "@idempotent: duplicate in-flight, skipping key=%s", idempotency_key
                )
                return None

            attempts = 1
            _config = config or DeduplicationConfig()
            last_exc: Exception | None = None

            while attempts <= _config.max_retries:
                try:
                    result = await func(*args, **kwargs)
                    await store.mark_completed(idempotency_key, result=result)
                    return result
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.warning(
                        "@idempotent: attempt %d/%d failed key=%s error=%s",
                        attempts, _config.max_retries, idempotency_key, exc,
                    )
                    await store.mark_failed(
                        idempotency_key, error=str(exc), attempts=attempts
                    )
                    if attempts < _config.max_retries:
                        backoff = _config.retry_backoff_ms / 1000 * attempts
                        await asyncio.sleep(backoff)
                        # Re-claim for the next attempt
                        reclaimed = await store.claim(idempotency_key)
                        if not reclaimed:
                            break
                    attempts += 1

            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator
