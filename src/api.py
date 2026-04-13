"""
FastAPI application exposing health and Prometheus metrics endpoints.

Endpoints:
    GET /health    — liveness probe (always 200 while process is alive)
    GET /ready     — readiness probe (checks Redis connectivity)
    GET /metrics   — Prometheus scrape endpoint

Run standalone::

    uvicorn src.api:app --host 0.0.0.0 --port 9090
"""

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.config import settings
from src.logging_config import setup_logging

setup_logging()

app = FastAPI(
    title="Kafka Dedup — Metrics & Health",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
)


@app.get("/health", summary="Liveness probe")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/ready", summary="Readiness probe — checks Redis")
async def ready() -> JSONResponse:
    import redis.asyncio as aioredis

    try:
        client = aioredis.from_url(settings.redis_url, socket_connect_timeout=2)
        await client.ping()
        await client.aclose()
        return JSONResponse({"status": "ready", "redis": "ok"})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(
            {"status": "not_ready", "redis": str(exc)},
            status_code=503,
        )


@app.get("/metrics", summary="Prometheus metrics scrape endpoint")
async def prometheus_metrics() -> Response:
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
