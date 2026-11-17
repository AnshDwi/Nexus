import logging
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from nexus.api.v1.health import router as health_router
from nexus.api.v1.internal import router as internal_router
from nexus.api.v1.tasks import router as tasks_router
from nexus.infrastructure.cache.redis import create_redis_pool
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import build_registry
from nexus.infrastructure.messaging.factory import create_event_publisher
from nexus.infrastructure.observability.logging import configure_logging, request_id_var
from nexus.infrastructure.state import AppState

logger = logging.getLogger("nexus.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Composition root. On Vercel this runs once per Fluid compute instance
    (startup on first invocation, shutdown when the instance is retired), so
    the engine/pool built here is shared by every concurrent invocation the
    instance serves. AsyncExitStack guarantees that whatever was created is
    released even if a later step fails."""
    settings = get_settings()
    async with AsyncExitStack() as stack:
        db_engine = create_engine(settings.database_url, settings)
        stack.push_async_callback(db_engine.dispose)

        redis_pool = None
        if settings.redis_url:
            redis_pool = create_redis_pool(settings.redis_url)
            stack.push_async_callback(redis_pool.disconnect)

        publisher = await create_event_publisher(settings)
        stack.push_async_callback(publisher.close)

        app.state.container = AppState(
            db_engine=db_engine,
            db_sessionmaker=create_session_factory(db_engine),
            redis_pool=redis_pool,
            rabbitmq_connection=publisher.rabbitmq_connection,
            settings=settings,
            publisher=publisher.publisher,
            task_handlers=build_registry(
                enable_test_handlers=settings.task_enable_test_handlers,
                webhook_allowed_hosts=settings.webhook_allowed_host_set,
            ),
        )
        logger.info(
            "nexus api started",
            extra={"environment": settings.environment, "event_bus": settings.event_bus.value},
        )
        yield


async def _request_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    request_id = request.headers.get("x-vercel-id") or request.headers.get("x-request-id")
    token = request_id_var.set(request_id or uuid.uuid4().hex)
    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)
    return response


async def _unhandled_error(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled error", extra={"path": request.url.path})
    return JSONResponse(status_code=500, content={"detail": "internal server error"})


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging("DEBUG" if settings.debug else "INFO")
    app = FastAPI(
        title=settings.app_name,
        debug=settings.debug,
        lifespan=lifespan,
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.middleware("http")(_request_context)
    app.add_exception_handler(Exception, _unhandled_error)
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(tasks_router, prefix="/api/v1")
    app.include_router(internal_router, prefix="/api/v1")
    return app


app = create_app()
