"""Lazily-built, process-wide runtime for entrypoints that are not the
FastAPI app (queue subscriber functions, daemons). On Vercel a queue consumer
is its own generated function, so the API's lifespan does not run there."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from nexus.infrastructure.config import Settings, get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.executor import TaskExecutor
from nexus.infrastructure.execution.handlers import build_registry
from nexus.infrastructure.execution.job_executor import JobExecutor
from nexus.infrastructure.messaging.factory import PublisherHandle, create_event_publisher
from nexus.infrastructure.observability.logging import configure_logging


@dataclass
class WorkerRuntime:
    settings: Settings
    executor: TaskExecutor
    publisher: PublisherHandle
    job_executor: JobExecutor | None = None

    async def aclose(self) -> None:
        await self.publisher.close()
        await self.executor.dispose()


_runtime: WorkerRuntime | None = None
_lock = asyncio.Lock()


async def build_worker_runtime(settings: Settings | None = None) -> WorkerRuntime:
    settings = settings or get_settings()
    configure_logging("DEBUG" if settings.debug else "INFO")
    engine = create_engine(settings.database_url, settings)
    session_factory = create_session_factory(engine)
    publisher = await create_event_publisher(settings)
    handlers = build_registry(
        enable_test_handlers=settings.task_enable_test_handlers,
        webhook_allowed_hosts=settings.webhook_allowed_host_set,
    )
    executor = TaskExecutor(session_factory, handlers, publisher.publisher, settings, engine=engine)
    job_executor = JobExecutor(
        session_factory,
        handlers,
        # One identity per function instance; the attempt + worker_id fence
        # still applies, the lease covers instance loss.
        worker_id=uuid4(),
        lease_grace=timedelta(seconds=settings.task_lease_grace_seconds),
    )
    return WorkerRuntime(
        settings=settings, executor=executor, publisher=publisher, job_executor=job_executor
    )


async def get_worker_runtime() -> WorkerRuntime:
    global _runtime
    if _runtime is None:
        async with _lock:
            if _runtime is None:
                _runtime = await build_worker_runtime()
    return _runtime


def set_worker_runtime(runtime: WorkerRuntime | None) -> None:
    """Tests and embedded runners: install a pre-built runtime."""
    global _runtime
    _runtime = runtime


async def reset_worker_runtime() -> None:
    """Tests: dispose and forget the cached runtime."""
    global _runtime
    if _runtime is not None:
        await _runtime.aclose()
        _runtime = None
