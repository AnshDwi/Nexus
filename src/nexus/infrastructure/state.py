from dataclasses import dataclass

from aio_pika.abc import AbstractRobustConnection
from redis.asyncio import ConnectionPool
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


@dataclass
class AppState:
    db_engine: AsyncEngine
    db_sessionmaker: async_sessionmaker[AsyncSession]
    redis_pool: ConnectionPool
    rabbitmq_connection: AbstractRobustConnection
