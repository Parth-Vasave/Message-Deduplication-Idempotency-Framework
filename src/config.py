from pydantic_settings import BaseSettings, SettingsConfigDict

from src.dedup.models import DeduplicationConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9094"
    kafka_group_id: str = "dedup-consumer-group"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Postgres
    postgres_dsn: str = "postgresql://dedup:dedup_secret@localhost:5432/kafkadedup"

    # Deduplication
    dedup_ttl_seconds: int = 86400  # 24 hours
    dedup_max_retries: int = 3
    dedup_retry_backoff_ms: int = 500
    dedup_processing_timeout_seconds: int = 300

    # Logging
    log_level: str = "INFO"

    def dedup_config(self) -> DeduplicationConfig:
        return DeduplicationConfig(
            ttl_seconds=self.dedup_ttl_seconds,
            max_retries=self.dedup_max_retries,
            retry_backoff_ms=self.dedup_retry_backoff_ms,
            processing_timeout_seconds=self.dedup_processing_timeout_seconds,
        )


settings = Settings()
