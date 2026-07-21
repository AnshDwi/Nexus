from fastapi import Request
from redis.asyncio import ConnectionPool, Redis


def create_redis_pool(redis_url: str) -> ConnectionPool:
    return ConnectionPool.from_url(redis_url)


def get_redis(request: Request) -> Redis:
    pool: ConnectionPool = request.app.state.container.redis_pool
    return Redis(connection_pool=pool)
