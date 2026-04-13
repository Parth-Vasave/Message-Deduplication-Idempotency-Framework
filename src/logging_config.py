"""
Structured logging setup using structlog.

Call ``setup_logging()`` once at application startup (before any loggers are
created) to configure both the standard library ``logging`` module and
``structlog`` to emit JSON-formatted log lines in production and colourful
console output in development.

Usage::

    from src.logging_config import setup_logging, get_logger

    setup_logging()                       # once at startup
    log = get_logger(__name__)

    log.info("message_processed", message_id="order-123", topic="orders")
    # → {"event": "message_processed", "message_id": "order-123",
    #    "topic": "orders", "level": "info", "timestamp": "2024-..."}
"""

import logging
import logging.config
import sys
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger

from src.config import settings


# ---------------------------------------------------------------------------
# Custom processors
# ---------------------------------------------------------------------------


def _drop_color_message_key(
    _logger: WrappedLogger, _method: str, event_dict: EventDict
) -> EventDict:
    """Remove the ``color_message`` key that uvicorn injects."""
    event_dict.pop("color_message", None)
    return event_dict


def _add_app_context(
    _logger: WrappedLogger, _method: str, event_dict: EventDict
) -> EventDict:
    """Inject static application context into every log record."""
    event_dict.setdefault("app", "kafkadedup")
    return event_dict


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def setup_logging(
    log_level: str | None = None,
    *,
    json_logs: bool | None = None,
) -> None:
    """
    Configure structlog + stdlib logging.

    Args:
        log_level:  Override the log level from settings (e.g. ``"DEBUG"``).
        json_logs:  ``True``  → JSON output (production default).
                    ``False`` → coloured console (development default).
                    ``None``  → auto-detect: JSON when not a TTY.
    """
    level_str = (log_level or settings.log_level).upper()
    level = getattr(logging, level_str, logging.INFO)

    if json_logs is None:
        json_logs = not sys.stderr.isatty()

    shared_processors: list[Any] = [
        _add_app_context,
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        _drop_color_message_key,
    ]

    if json_logs:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors
        + [
            # Bridge from stdlib logging into structlog when using getLogger()
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # These run on records that originate from stdlib logging (e.g. aiokafka)
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    # Quieten noisy third-party loggers
    for noisy in ("aiokafka", "kafka", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog-wrapped stdlib logger.

    Prefer this over ``logging.getLogger()`` for new code so you can pass
    structured key-value pairs::

        log = get_logger(__name__)
        log.info("claim_won", message_id=mid, topic=topic)
    """
    return structlog.get_logger(name)  # type: ignore[return-value]
