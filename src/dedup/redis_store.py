"""
Redis-backed DeduplicationStore.

Atomicity strategy
------------------
`claim()` uses a single Redis SET NX PX command which is atomic by design —
no Lua script needed for the initial claim.  `mark_completed` and
`mark_failed` use a Lua script that guards against overwriting a COMPLETED
record with a FAILED one (or vice-versa), preventing status corruption under
concurrent retries.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio import Redis

from .models import DeduplicationConfig, ProcessingStatus, StatusValue
from .store import DeduplicationStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

# Update status only if the key exists AND the current status is PROCESSING.
# This prevents a late retry from overwriting a completed record.
_LUA_UPDATE_STATUS = """
local key    = KEYS[1]
local raw    = redis.call('GET', key)
if not raw then
    return -1          -- key expired or never existed
end
local rec = cjson.decode(raw)
if rec['status'] ~= 'PROCESSING' then
    return 0           -- already in terminal/non-processing state
end
rec['status']     = ARGV[1]
rec['updated_at'] = ARGV[2]
if ARGV[3] ~= '' then rec['result'] = cjson.decode(ARGV[3]) end
if ARGV[4] ~= '' then rec['error']  = ARGV[4]               end
if ARGV[5] ~= '' then rec['attempts'] = tonumber(ARGV[5])   end
redis.call('SET', key, cjson.encode(rec), 'KEEPTTL')
return 1               -- success
"""


def _redis_key(message_id: str) -> str:
    return f"dedup:{message_id}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RedisDeduplicationStore(DeduplicationStore):
    """
    Production dedup store backed by Redis.

    Usage:
        store = RedisDeduplicationStore.from_url("redis://localhost:6379/0")
        await store.connect()
        ...
        await store.close()
    """

    def __init__(self, client: Redis, config: DeduplicationConfig | None = None) -> None:
        self._client = client
        self._config = config or DeduplicationConfig()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        config: DeduplicationConfig | None = None,
        **redis_kwargs: Any,
    ) -> "RedisDeduplicationStore":
        client: Redis = aioredis.from_url(url, decode_responses=True, **redis_kwargs)
        return cls(client, config)

    async def connect(self) -> None:
        await self._client.ping()
        logger.info("RedisDeduplicationStore connected")

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def claim(self, message_id: str) -> bool:
        """
        Atomically set dedup:{message_id} = PROCESSING if it doesn't exist.
        Redis SET NX is O(1) and guaranteed atomic.

        Returns True if this caller won the claim.
        """
        key = _redis_key(message_id)
        now = _now_iso()
        record = json.dumps({
            "message_id": message_id,
            "status":     StatusValue.PROCESSING,
            "attempts":   1,
            "result":     None,
            "error":      None,
            "created_at": now,
            "updated_at": now,
        })
        # SET key value NX PX milliseconds — atomic, no race condition
        ttl_ms = self._config.ttl_seconds * 1000
        claimed = await self._client.set(key, record, nx=True, px=ttl_ms)
        if claimed:
            logger.debug("Claimed message_id=%s", message_id)
        else:
            logger.debug("Duplicate detected message_id=%s", message_id)
        return bool(claimed)

    async def is_duplicate(self, message_id: str) -> bool:
        exists = await self._client.exists(_redis_key(message_id))
        return bool(exists)

    async def mark_completed(self, message_id: str, result: Any = None) -> None:
        result_json = json.dumps(result) if result is not None else ""
        ret = await self._run_update_script(
            message_id,
            status=StatusValue.COMPLETED,
            result_json=result_json,
            error="",
            attempts="",
        )
        self._check_script_return(message_id, ret, "mark_completed")

    async def mark_failed(
        self, message_id: str, error: str, attempts: int = 1
    ) -> None:
        ret = await self._run_update_script(
            message_id,
            status=StatusValue.FAILED,
            result_json="",
            error=error,
            attempts=str(attempts),
        )
        self._check_script_return(message_id, ret, "mark_failed")

    async def get_status(self, message_id: str) -> ProcessingStatus | None:
        raw = await self._client.get(_redis_key(message_id))
        if raw is None:
            return None
        data = json.loads(raw)
        return ProcessingStatus(**data)

    async def delete(self, message_id: str) -> None:
        await self._client.delete(_redis_key(message_id))

    async def close(self) -> None:
        await self._client.aclose()
        logger.info("RedisDeduplicationStore closed")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _run_update_script(
        self,
        message_id: str,
        *,
        status: StatusValue,
        result_json: str,
        error: str,
        attempts: str,
    ) -> Any:
        # Use EVAL directly — avoids EVALSHA/register_script which some
        # Redis-compatible clients (fakeredis, certain proxies) don't support.
        return await self._client.eval(
            _LUA_UPDATE_STATUS,
            1,  # number of keys
            _redis_key(message_id),
            status, _now_iso(), result_json, error, attempts,
        )

    @staticmethod
    def _check_script_return(message_id: str, ret: Any, operation: str) -> None:
        if ret == -1:
            logger.warning(
                "%s: key not found (expired?) message_id=%s", operation, message_id
            )
        elif ret == 0:
            logger.warning(
                "%s: status already terminal, skipped message_id=%s",
                operation, message_id,
            )
