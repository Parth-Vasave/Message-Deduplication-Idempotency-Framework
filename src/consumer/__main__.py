"""
Consumer entrypoint: ``python -m src.consumer``.

Runs the order, payment and shipment handlers in one process, each with its
own Redis connection, configured from environment variables (see
``src.config.Settings``).  Serves Prometheus metrics on :9090.  SIGTERM /
SIGINT stop all consumers; if any consumer crashes, the process exits non-zero
so the orchestrator restarts it.
"""

import asyncio
import logging
import signal
import sys

from prometheus_client import start_http_server

from src.config import settings
from src.dedup import RedisDeduplicationStore
from src.logging_config import setup_logging

from .dedup_consumer import DedupConsumer
from .handlers import OrderHandler, PaymentHandler, ShipmentHandler

logger = logging.getLogger(__name__)

METRICS_PORT = 9090


async def main() -> int:
    config = settings.dedup_config()
    consumers: list[DedupConsumer] = [
        handler_cls(
            store=RedisDeduplicationStore.from_url(settings.redis_url, config=config),
            config=config,
        )
        for handler_cls in (OrderHandler, PaymentHandler, ShipmentHandler)
    ]

    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_requested.set)

    for consumer in consumers:
        await consumer.start()

    run_tasks = [asyncio.create_task(c.run()) for c in consumers]
    stop_task = asyncio.create_task(stop_requested.wait())
    done, _ = await asyncio.wait([*run_tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)

    exit_code = 0
    for task in run_tasks:
        if task in done and task.exception() is not None:
            logger.error("Consumer crashed", exc_info=task.exception())
            exit_code = 1

    # Cancel in-flight work: a message interrupted mid-handle() keeps its
    # offset uncommitted and becomes claimable again once its lease expires.
    stop_task.cancel()
    for task in run_tasks:
        task.cancel()
    await asyncio.gather(*run_tasks, stop_task, return_exceptions=True)
    for consumer in consumers:
        await consumer.stop()
    return exit_code


if __name__ == "__main__":
    setup_logging()
    start_http_server(METRICS_PORT)
    sys.exit(asyncio.run(main()))
