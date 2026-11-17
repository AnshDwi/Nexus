from dataclasses import dataclass, field
from uuid import UUID, uuid4

from aio_pika.abc import AbstractRobustConnection
from redis.asyncio import ConnectionPool
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.interfaces.task_handler import TaskHandlerRegistry
from nexus.infrastructure.config import Settings


@dataclass
class AppState:
    """Per-process composition root, built once in the FastAPI lifespan.

    On Vercel one Fluid compute instance serves many concurrent invocations,
    so everything here (engine/pool, queue client) is shared across them and
    torn down when the instance shuts down."""

    db_engine: AsyncEngine
    db_sessionmaker: async_sessionmaker[AsyncSession]
    redis_pool: ConnectionPool | None
    rabbitmq_connection: AbstractRobustConnection | None
    settings: Settings | None = None
    publisher: EventPublisher | None = None
    task_handlers: TaskHandlerRegistry | None = None
    replica_id: UUID = field(default_factory=uuid4)
