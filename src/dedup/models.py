from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class StatusValue(StrEnum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ProcessingStatus(BaseModel):
    message_id: str
    status: StatusValue
    attempts: int = 1
    result: Any | None = None
    error: str | None = None
    # While PROCESSING, the claim is only exclusive until this time.  After it
    # passes, the owner is presumed dead and another consumer may re-claim.
    lease_expires_at: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def is_terminal(self) -> bool:
        return self.status in (StatusValue.COMPLETED, StatusValue.SKIPPED)

    def is_failed(self) -> bool:
        return self.status == StatusValue.FAILED


class DeduplicationConfig(BaseModel):
    ttl_seconds: int = 86400  # 24 hours
    max_retries: int = 3
    retry_backoff_ms: int = 500
    # How long a PROCESSING claim stays exclusive.  If the consumer crashes
    # between claim() and mark_completed()/mark_failed(), the message becomes
    # claimable again once this lease expires instead of being blocked for the
    # whole ttl_seconds.  Must be longer than your slowest handle() call.
    processing_timeout_seconds: int = 300
    # If True, a FAILED status from a previous attempt is treated as
    # retriable rather than a duplicate skip.
    retry_failed: bool = True
