import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.base import Base
from nexus.infrastructure.db.models import (  # noqa: F401
    inbox,
    job,
    leader_lease,
    outbox,
    task,
    worker_registration,
)
from nexus.infrastructure.db.session import engine_options

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=get_settings().migration_database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    # Migrations use the direct (non-pooled) URL when the provider offers one:
    # DDL and alembic's version bookkeeping need a session-level connection.
    settings = get_settings()
    options = engine_options(settings)
    options.pop("poolclass", None)
    for key in ("pool_size", "max_overflow", "pool_timeout", "pool_recycle"):
        options.pop(key, None)
    connectable: AsyncEngine = create_async_engine(
        settings.migration_database_url, poolclass=NullPool, **options
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
