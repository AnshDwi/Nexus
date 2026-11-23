from fastapi import Request
from redis.asyncio import ConnectionPool, Redis


def create_redis_pool(redis_url: str) -> ConnectionPool:
    return ConnectionPool.from_url(redis_url)


def get_redis(request: Request) -> Redis | None:
    """None when Redis is not configured (it is optional: nothing in the task
    pipeline depends on it)."""
    pool: ConnectionPool | None = request.app.state.container.redis_pool
    if pool is None:
        return None
    return Redis(connection_pool=pool)
