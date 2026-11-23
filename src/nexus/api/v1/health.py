import asyncio
import logging
from collections.abc import Awaitable

from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.infrastructure.cache.redis import get_redis
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.rabbitmq import get_rabbitmq_connection

router = APIRouter(tags=["health"])
logger = logging.getLogger("nexus.api.health")

_CHECK_TIMEOUT_SECONDS = 3.0


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. Never touches dependencies."""
    return {"status": "ok"}


async def _check(name: str, probe: Awaitable[object]) -> str:
    try:
        await asyncio.wait_for(probe, timeout=_CHECK_TIMEOUT_SECONDS)
    except Exception:
        # Details go to logs only: probe responses are unauthenticated and
        # driver errors can contain hostnames or connection strings.
        logger.warning("readiness check failed", extra={"dependency": name}, exc_info=True)
        return "error"
    return "ok"


async def _rabbitmq_probe(connection: AbstractRobustConnection) -> None:
    channel = await connection.channel()
    await channel.close()


@router.get("/ready", response_model=None)
async def ready(
    session: AsyncSession = Depends(get_db_session),
    redis: Redis | None = Depends(get_redis),
    rabbitmq_connection: AbstractRobustConnection | None = Depends(get_rabbitmq_connection),
) -> JSONResponse:
    """Readiness: every *configured* dependency answers. Returns 503 when
    degraded so load balancers and deploy checks stop routing to it."""
    checks: dict[str, str] = {
        "postgres": await _check("postgres", session.execute(text("SELECT 1")))
    }
    if redis is not None:
        checks["redis"] = await _check("redis", redis.ping())
    if rabbitmq_connection is not None:
        checks["rabbitmq"] = await _check("rabbitmq", _rabbitmq_probe(rabbitmq_connection))

    healthy = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", **checks},
    )
