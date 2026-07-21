from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from nexus.api.v1.health import router as health_router
from nexus.infrastructure.cache.redis import create_redis_pool
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.state import AppState


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    settings = get_settings()

    db_engine = create_engine(settings.database_url)
    redis_pool = create_redis_pool(settings.redis_url)
    rabbitmq_connection = await create_rabbitmq_connection(settings.rabbitmq_url)

    app.state.container = AppState(
        db_engine=db_engine,
        db_sessionmaker=create_session_factory(db_engine),
        redis_pool=redis_pool,
        rabbitmq_connection=rabbitmq_connection,
    )

    try:
        yield
    finally:
        await rabbitmq_connection.close()
        await redis_pool.disconnect()
        await db_engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)
    app.include_router(health_router, prefix="/api/v1")
    return app


app = create_app()
