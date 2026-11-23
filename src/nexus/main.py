import logging
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from nexus.api.v1.health import router as health_router
from nexus.api.v1.internal import router as internal_router
from nexus.api.v1.jobs import router as jobs_router
from nexus.api.v1.platform import router as platform_router
from nexus.api.v1.tasks import router as tasks_router
from nexus.infrastructure.cache.redis import create_redis_pool
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import build_registry
from nexus.infrastructure.messaging.factory import create_event_publisher
from nexus.infrastructure.observability.logging import (
    configure_logging,
    correlation_id_var,
    request_id_var,
)
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


_MAX_ID_LENGTH = 128


def _clean_id(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()[:_MAX_ID_LENGTH]
    return value if value.isprintable() else None


async def _request_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Request ID (Vercel's x-vercel-id or the client's X-Request-ID) and a
    correlation ID that follows the job through the outbox, RabbitMQ and the
    worker. Both are echoed back as response headers."""
    request_id = (
        _clean_id(request.headers.get("x-vercel-id"))
        or _clean_id(request.headers.get("x-request-id"))
        or uuid.uuid4().hex
    )
    correlation_id = _clean_id(request.headers.get("x-correlation-id")) or request_id
    request_token = request_id_var.set(request_id)
    correlation_token = correlation_id_var.set(correlation_id)
    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(request_token)
        correlation_id_var.reset(correlation_token)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Correlation-ID"] = correlation_id
    return response


async def _unhandled_error(request: Request, exc: Exception) -> JSONResponse:
    # Never leak internals: the client gets a request ID to quote, the
    # details go to the logs.
    logger.exception("unhandled error", extra={"path": request.url.path})
    return JSONResponse(
        status_code=500,
        content={
            "detail": {"code": "internal_error", "message": "internal server error"},
            "request_id": request_id_var.get(),
        },
    )


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
    if settings.cors_origin_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origin_list,
            allow_credentials=False,  # bearer API keys, never cookies
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "X-Correlation-ID",
                "X-Request-ID",
            ],
            expose_headers=["X-Request-ID", "X-Correlation-ID", "Retry-After"],
            max_age=600,
        )
    app.add_exception_handler(Exception, _unhandled_error)
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(jobs_router, prefix="/api/v1")
    app.include_router(platform_router, prefix="/api/v1")
    app.include_router(tasks_router, prefix="/api/v1")
    app.include_router(internal_router, prefix="/api/v1")
    return app


app = create_app()
