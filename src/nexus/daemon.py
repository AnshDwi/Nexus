"""Long-running NEXUS processes for Vercel Daemons (or any container host).

    python -m nexus.daemon scheduler   # default
    python -m nexus.daemon worker

scheduler  Every DAEMON_TICK_INTERVAL_SECONDS: acquire/renew the
           `nexus-maintenance` lease (PostgreSQL, fencing token) and, while
           leader, run the reconciler and drain the outbox. Multiple
           instances are safe; one leads, the rest stand by.

worker     With EVENT_BUS=rabbitmq (default): the RabbitMQ job worker
           (registration, heartbeats, prefetch = WORKER_CAPACITY, DLQ,
           graceful drain on SIGTERM). With EVENT_BUS=vercel_queues: a
           poll-mode Vercel Queues consumer that runs the same handler as the
           push-mode subscriber. Use it *instead of* the push subscriber
           (remove [[tool.vercel.subscribers]]) for tasks that need longer
           than a Function's maxDuration: a consumer group is either push or
           poll, and both register as group `nexus-executor`.

Both serve GET /health on DAEMON_HEALTH_PORT for the Daemon livenessProbe:
200 while the work loop is making progress, 503 once it has stalled.
Vercel sends SIGTERM before stopping an instance; the current tick finishes
and the process exits cleanly within terminationGracePeriodSeconds.
"""

import asyncio
import contextlib
import logging
import signal
import sys
import time
from datetime import timedelta
from uuid import uuid4

from nexus.infrastructure.config import EventBus, Settings, get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.maintenance import run_maintenance_pass
from nexus.infrastructure.messaging.factory import (
    create_event_publisher,
    create_vercel_queue_client,
)
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.logging import configure_logging

logger = logging.getLogger("nexus.daemon")


class Heartbeat:
    def __init__(self, stall_after_seconds: float) -> None:
        self._stall_after = stall_after_seconds
        self._last = time.monotonic()

    def beat(self) -> None:
        self._last = time.monotonic()

    @property
    def healthy(self) -> bool:
        return time.monotonic() - self._last < self._stall_after


async def serve_health(heartbeat: Heartbeat, port: int) -> asyncio.Server:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=2)
            path = request_line.split(b" ")[1] if b" " in request_line else b"/"
            ok = path == b"/health" and heartbeat.healthy
            status = b"200 OK" if ok else b"503 Service Unavailable"
            writer.write(
                b"HTTP/1.1 " + status + b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        except (TimeoutError, ConnectionError, IndexError):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(handle, host="0.0.0.0", port=port)  # noqa: S104


def can_publish_to_push_consumers(settings: Settings) -> bool:
    """Push consumers only receive messages from their own deployment's
    partition. A daemon that cannot name that partition must not publish
    dispatch messages, or they would never reach the consumer."""
    if settings.event_bus is not EventBus.VERCEL_QUEUES:
        return True
    if settings.vercel_queue_deployment != "auto":
        return True  # explicitly configured
    return settings.vercel_deployment_id is not None or settings.is_local


def _install_shutdown(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)


async def run_scheduler(settings: Settings, stop: asyncio.Event) -> None:
    engine = create_engine(settings.database_url, settings)
    session_factory = create_session_factory(engine)
    publisher = await create_event_publisher(settings)
    replica_id = uuid4()
    relay = can_publish_to_push_consumers(settings)
    if not relay:
        logger.warning(
            "VERCEL_DEPLOYMENT_ID unavailable: this daemon reconciles but leaves publishing "
            "to the Functions (eager relay and cron), whose messages reach the push consumer"
        )
    heartbeat = Heartbeat(stall_after_seconds=max(settings.daemon_tick_interval_seconds * 6, 60))
    server = await serve_health(heartbeat, settings.daemon_health_port)
    logger.info("scheduler daemon started", extra={"replica_id": str(replica_id)})

    async def relay_loop() -> None:
        # Every instance relays (SKIP LOCKED makes that safe); this is what
        # turns RETRYING jobs back into QUEUED soon after their backoff.
        while not stop.is_set():
            if relay:
                try:
                    await relay_pending_events(
                        session_factory, publisher.publisher, settings, max_events=50
                    )
                except Exception:
                    logger.exception("relay loop failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.outbox_relay_poll_interval_seconds
                )

    relay_task = asyncio.create_task(relay_loop())
    try:
        while not stop.is_set():
            try:
                await run_maintenance_pass(
                    session_factory,
                    publisher.publisher,
                    settings,
                    replica_id=replica_id,
                    trigger="daemon",
                    relay=relay,
                )
                heartbeat.beat()
            except Exception:
                # Database blips must not kill the loop; a persistent failure
                # stops heartbeats and the liveness probe restarts us.
                logger.exception("maintenance pass failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.daemon_tick_interval_seconds)
    finally:
        relay_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay_task
        server.close()
        await publisher.close()
        await engine.dispose()
        logger.info("scheduler daemon stopped")


async def run_rabbitmq_worker(settings: Settings, stop: asyncio.Event) -> None:
    from nexus.infrastructure.execution.executor import TaskExecutor
    from nexus.infrastructure.execution.handlers import build_registry
    from nexus.infrastructure.execution.job_executor import JobExecutor
    from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
    from nexus.infrastructure.messaging.rabbitmq_worker import RabbitMQJobWorker, WorkerConfig

    engine = create_engine(settings.database_url, settings)
    session_factory = create_session_factory(engine)
    connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    publisher = await create_event_publisher(settings)
    handlers = build_registry(
        enable_test_handlers=settings.task_enable_test_handlers,
        webhook_allowed_hosts=settings.webhook_allowed_host_set,
    )
    worker = RabbitMQJobWorker(
        connection=connection,
        session_factory=session_factory,
        executor=JobExecutor(
            session_factory,
            handlers,
            worker_id=uuid4(),
            lease_grace=timedelta(seconds=settings.task_lease_grace_seconds),
        ),
        task_executor=TaskExecutor(session_factory, handlers, publisher.publisher, settings),
        config=WorkerConfig(
            capacity=settings.worker_capacity,
            heartbeat_interval=settings.worker_heartbeat_interval_seconds,
            shutdown_grace=settings.worker_shutdown_grace_seconds,
        ),
    )
    heartbeat = Heartbeat(stall_after_seconds=settings.worker_heartbeat_ttl_seconds)
    server = await serve_health(heartbeat, settings.daemon_health_port)
    await worker.start()
    try:
        while not stop.is_set():
            if worker.healthy:
                heartbeat.beat()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.worker_heartbeat_interval_seconds
                )
    finally:
        await worker.stop()
        server.close()
        await connection.close()
        await publisher.close()
        await engine.dispose()


async def run_worker(settings: Settings, stop: asyncio.Event) -> None:
    if settings.event_bus is EventBus.RABBITMQ:
        await run_rabbitmq_worker(settings, stop)
        return
    await run_poll_worker(settings, stop)


async def run_poll_worker(settings: Settings, stop: asyncio.Event) -> None:
    from nexus.entrypoints.queue_subscriber import execute_dispatched_task
    from nexus.entrypoints.runtime import get_worker_runtime, reset_worker_runtime

    await get_worker_runtime()
    heartbeat = Heartbeat(stall_after_seconds=3600)  # handlers may legitimately run long
    server = await serve_health(heartbeat, settings.daemon_health_port)
    client = create_vercel_queue_client(settings)
    poller = asyncio.create_task(client.poll_and_handle(execute_dispatched_task, interval=1.0))
    logger.info("poll-mode worker daemon started")
    try:
        while not stop.is_set() and not poller.done():
            heartbeat.beat()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=5)
    finally:
        # Cancelling mid-task leaves the message unacknowledged: it is
        # redelivered, and the task's lease/attempt guard handles the rest.
        poller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poller
        server.close()
        await reset_worker_runtime()
        logger.info("poll-mode worker daemon stopped")


async def main_async(role: str) -> None:
    settings = get_settings()
    configure_logging("DEBUG" if settings.debug else "INFO")
    stop = asyncio.Event()
    _install_shutdown(stop)
    if role == "scheduler":
        await run_scheduler(settings, stop)
    elif role == "worker":
        await run_worker(settings, stop)
    else:
        raise SystemExit(f"unknown role '{role}' (expected 'scheduler' or 'worker')")


def main() -> None:
    role = sys.argv[1] if len(sys.argv) > 1 else "scheduler"
    asyncio.run(main_async(role))


if __name__ == "__main__":
    main()
