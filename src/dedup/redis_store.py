"""
Redis-backed DeduplicationStore.

Atomicity strategy
------------------
`claim()` runs as a Lua script, so the read-check-write happens atomically on
the server.  It succeeds when the key is missing, when the record is FAILED and
still has retries left, or when the record is PROCESSING but its lease has
expired (the previous owner crashed).  Lease times come from Redis' own clock
(TIME), so clock skew between consumers doesn't matter.  `mark_completed` and
`mark_failed` use a Lua script that guards against overwriting a COMPLETED
record with a FAILED one (or vice-versa), preventing status corruption under
concurrent retries.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio import Redis

from .models import DeduplicationConfig, ProcessingStatus, StatusValue
from .store import DeduplicationStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua scripts
# ---------------------------------------------------------------------------

_LUA_CLAIM = """
local key          = KEYS[1]
local message_id   = ARGV[1]
local ttl_ms       = tonumber(ARGV[2])
local lease_ms     = tonumber(ARGV[3])
local max_retries  = tonumber(ARGV[4])
local retry_failed = ARGV[5] == '1'
local now_iso      = ARGV[6]
local t      = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local raw = redis.call('GET', key)
if not raw then
    local rec = {
        message_id = message_id, status = 'PROCESSING', attempts = 1,
        created_at = now_iso, updated_at = now_iso, lease_until_ms = now_ms + lease_ms,
    }
    redis.call('SET', key, cjson.encode(rec), 'PX', ttl_ms)
    return 1
end

local rec      = cjson.decode(raw)
local attempts = tonumber(rec['attempts']) or 1
local lease    = tonumber(rec['lease_until_ms'])
local retryable = rec['status'] == 'FAILED' and retry_failed and attempts < max_retries
local orphaned  = rec['status'] == 'PROCESSING' and lease ~= nil and lease <= now_ms
if not (retryable or orphaned) then
    return 0
end
rec['status']         = 'PROCESSING'
rec['attempts']       = attempts + 1
rec['error']          = nil
rec['updated_at']     = now_iso
rec['lease_until_ms'] = now_ms + lease_ms
redis.call('SET', key, cjson.encode(rec), 'KEEPTTL')
return 1
"""

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
rec['lease_until_ms'] = nil
redis.call('SET', key, cjson.encode(rec), 'KEEPTTL')
return 1               -- success
"""


def _redis_key(message_id: str) -> str:
    return f"dedup:{message_id}"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class RedisDeduplicationStore(DeduplicationStore):
    """
    Production dedup store backed by Redis.

    Usage:
        store = RedisDeduplicationStore.from_url("redis://localhost:6379/0")
        await store.connect()
        ...
        await store.close()
    """

    def __init__(self, client: "Redis[Any]", config: DeduplicationConfig | None = None) -> None:
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
        client: Redis[Any] = aioredis.from_url(url, decode_responses=True, **redis_kwargs)
        return cls(client, config)

    async def connect(self) -> None:
        await self._client.ping()
        logger.info("RedisDeduplicationStore connected")

    # ------------------------------------------------------------------
    # Core interface
    # ------------------------------------------------------------------

    async def claim(self, message_id: str) -> bool:
        """
        Atomically set dedup:{message_id} = PROCESSING if it is claimable:
        missing, FAILED with retries left, or PROCESSING with an expired lease.

        Returns True if this caller won the claim.
        """
        claimed = await self._client.eval(  # type: ignore[no-untyped-call]
            _LUA_CLAIM,
            1,
            _redis_key(message_id),
            message_id,
            self._config.ttl_seconds * 1000,
            self._config.processing_timeout_seconds * 1000,
            self._config.max_retries,
            "1" if self._config.retry_failed else "0",
            _now_iso(),
        )
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

    async def mark_failed(self, message_id: str, error: str, attempts: int = 1) -> None:
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
        lease_ms = data.pop("lease_until_ms", None)
        if lease_ms is not None:
            data["lease_expires_at"] = datetime.fromtimestamp(lease_ms / 1000, UTC)
        return ProcessingStatus(**data)

    async def delete(self, message_id: str) -> None:
        await self._client.delete(_redis_key(message_id))

    async def close(self) -> None:
        await self._client.close()
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
        return await self._client.eval(  # type: ignore[no-untyped-call]
            _LUA_UPDATE_STATUS,
            1,  # number of keys
            _redis_key(message_id),
            status,
            _now_iso(),
            result_json,
            error,
            attempts,
        )

    @staticmethod
    def _check_script_return(message_id: str, ret: Any, operation: str) -> None:
        if ret == -1:
            logger.warning("%s: key not found (expired?) message_id=%s", operation, message_id)
        elif ret == 0:
            logger.warning(
                "%s: status already terminal, skipped message_id=%s",
                operation,
                message_id,
            )
