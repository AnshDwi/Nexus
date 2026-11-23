import ssl
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

from fastapi import Request
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from nexus.infrastructure.config import DbPoolMode, Settings


def engine_options(settings: Settings) -> dict[str, Any]:
    """Engine/pool configuration for serverless (Vercel Fluid compute),
    daemons and local runs.

    - Fluid compute reuses one warm instance for many concurrent invocations
      and keeps it alive long enough to close idle connections, so a small
      per-instance QueuePool is safe and is Vercel's documented approach.
    - pool_pre_ping + pool_recycle drop connections a provider closed while
      the instance was suspended (Neon scales computes to zero).
    - Behind a transaction-mode PgBouncer, asyncpg's named prepared statements
      break across backend connections, so the statement cache is disabled
      and statement names are made unique.
    """
    connect_args: dict[str, Any] = {
        "server_settings": {
            "application_name": settings.app_name,
            "statement_timeout": str(settings.db_statement_timeout_ms),
        },
        "timeout": settings.db_pool_timeout_seconds,
    }
    if settings.database_requires_ssl:
        connect_args["ssl"] = ssl.create_default_context()
    if settings.database_behind_pgbouncer:
        connect_args["statement_cache_size"] = 0
        connect_args["prepared_statement_cache_size"] = 0
        connect_args["prepared_statement_name_func"] = lambda: f"__nexus_{uuid4()}__"
        # PgBouncer in transaction mode rejects startup parameters it does not
        # track; statement_timeout is applied per role/database instead.
        connect_args["server_settings"].pop("statement_timeout")

    options: dict[str, Any] = {"pool_pre_ping": True, "connect_args": connect_args}
    if settings.db_pool_mode is DbPoolMode.NULL:
        options["poolclass"] = NullPool
    else:
        options.update(
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout_seconds,
            pool_recycle=settings.db_pool_recycle_seconds,
        )
    return options


def create_engine(database_url: str, settings: Settings | None = None) -> AsyncEngine:
    if settings is None:
        return create_async_engine(database_url, pool_pre_ping=True)
    return create_async_engine(database_url, **engine_options(settings))


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False)


async def get_db_session(request: Request) -> AsyncGenerator[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.container.db_sessionmaker
    async with session_factory() as session:
        yield session
