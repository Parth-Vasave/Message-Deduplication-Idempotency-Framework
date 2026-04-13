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
    # If True, a FAILED status from a previous attempt is treated as
    # retriable rather than a duplicate skip.
    retry_failed: bool = True
