from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, Depends
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.infrastructure.cache.redis import get_redis
from nexus.infrastructure.db.session import get_db_session
from nexus.infrastructure.messaging.rabbitmq import get_rabbitmq_connection

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(
    session: AsyncSession = Depends(get_db_session),
    redis: Redis = Depends(get_redis),
    rabbitmq_connection: AbstractRobustConnection = Depends(get_rabbitmq_connection),
) -> dict[str, str]:
    checks: dict[str, str] = {}

    try:
        await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = f"error: {exc}"

    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"

    try:
        channel = await rabbitmq_connection.channel()
        await channel.close()
        checks["rabbitmq"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["rabbitmq"] = f"error: {exc}"

    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": status, **checks}
