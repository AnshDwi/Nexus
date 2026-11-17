"""RabbitMQ job worker.

Lifecycle:
  start   register in the worker registry (capabilities, capacity) and
          start heartbeating; consume nexus.jobs (and nexus.tasks) with
          prefetch = capacity, so a worker never holds more messages than it
          can run.
  message parse -> JobExecutor.handle -> ack | reject to DLQ | requeue.
          Acknowledgement happens only after the outcome is committed.
  stop    (SIGTERM) mark draining, cancel consumers (no new messages), wait
          up to `shutdown_grace` for in-flight jobs, hand unfinished attempts
          back for retry, deregister, close. Unacked messages return to the
          queue; the inbox makes their redelivery a no-op.
If the process dies instead, its heartbeat stops and the elected scheduler
fails and retries its RUNNING jobs (dead-worker detection).
"""

import asyncio
import contextlib
import logging
import os
import platform
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.worker_capabilities import WorkerCapabilities
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.execution.executor import DispatchMessage, RetryLater, TaskExecutor
from nexus.infrastructure.execution.job_executor import Disposition, JobExecutor, JobMessage
from nexus.infrastructure.messaging.rabbitmq_topology import declare_topology
from nexus.infrastructure.observability.logging import correlation_id_var

logger = logging.getLogger("nexus.worker.rabbitmq")


def local_capabilities() -> WorkerCapabilities:
    memory_mb = 1024
    with contextlib.suppress(ValueError, OSError, AttributeError):
        memory_mb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // (1024 * 1024)
    return WorkerCapabilities(
        cpu_cores=float(os.cpu_count() or 1),
        memory_mb=max(int(memory_mb), 1),
        architecture=platform.machine() or "unknown",
        region=os.environ.get("VERCEL_REGION", "local"),
        runtime=f"python{platform.python_version()}",
    )


@dataclass
class WorkerConfig:
    capacity: int = 4
    heartbeat_interval: float = 5.0
    shutdown_grace: float = 25.0
    consume_tasks: bool = True


@dataclass
class _InFlight:
    job_id: UUID
    attempt: int


@dataclass
class RabbitMQJobWorker:
    connection: AbstractRobustConnection
    session_factory: async_sessionmaker[AsyncSession]
    executor: JobExecutor
    task_executor: TaskExecutor | None
    config: WorkerConfig = field(default_factory=WorkerConfig)

    def __post_init__(self) -> None:
        self._in_flight: dict[asyncio.Task[Any], _InFlight | None] = {}
        self._stopping = asyncio.Event()
        self._consumer_tags: list[tuple[Any, str]] = []
        self._heartbeat_task: asyncio.Task[None] | None = None
        self.healthy = False

    @property
    def worker_id(self) -> UUID:
        return self.executor.worker_id

    async def _register(self) -> None:
        async with self.session_factory() as session:
            await PostgresWorkerRegistry(session).register(
                self.worker_id, local_capabilities(), self.config.capacity
            )
            await session.commit()

    async def _heartbeat_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                async with self.session_factory() as session:
                    registration = await PostgresWorkerRegistry(session).heartbeat(self.worker_id)
                    await session.commit()
                if registration is None:
                    # Deregistered (e.g. declared dead after a long pause):
                    # come back as a fresh registration.
                    logger.warning("worker no longer registered; re-registering")
                    await self._register()
                self.healthy = True
            except Exception:
                self.healthy = False
                logger.exception("heartbeat failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), self.config.heartbeat_interval)

    async def start(self) -> None:
        # Fresh state, so a stopped worker object can be started again.
        self._stopping = asyncio.Event()
        self._consumer_tags = []
        await self._register()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=self.config.capacity)
        topology = await declare_topology(channel)
        tag = await topology.jobs.consume(self._on_job_message)
        self._consumer_tags.append((topology.jobs, tag))
        if self.config.consume_tasks and self.task_executor is not None:
            tag = await topology.tasks.consume(self._on_task_message)
            self._consumer_tags.append((topology.tasks, tag))
        self.healthy = True
        logger.info(
            "worker started",
            extra={"worker_id": str(self.worker_id), "capacity": self.config.capacity},
        )

    def _track(self) -> asyncio.Task[Any]:
        task = asyncio.current_task()
        assert task is not None
        self._in_flight[task] = None
        return task

    async def _on_job_message(self, message: AbstractIncomingMessage) -> None:
        if self._stopping.is_set():
            await message.nack(requeue=True)
            return
        task = self._track()
        token = correlation_id_var.set(message.correlation_id)
        try:
            try:
                parsed = JobMessage.parse(message.body, message.message_id)
            except (ValueError, KeyError, TypeError):
                logger.error(
                    "poison job message dead-lettered", extra={"message_id": message.message_id}
                )
                await message.reject(requeue=False)
                return
            self._in_flight[task] = _InFlight(parsed.job_id, parsed.attempt)
            disposition = await self.executor.handle(parsed)
            if disposition is Disposition.ACK:
                await message.ack()
            elif disposition is Disposition.DEAD_LETTER:
                await message.reject(requeue=False)
            else:
                await asyncio.sleep(1)  # back off before the broker redelivers
                await message.nack(requeue=True)
        finally:
            correlation_id_var.reset(token)
            self._in_flight.pop(task, None)

    async def _on_task_message(self, message: AbstractIncomingMessage) -> None:
        """Tasks (DAG pipeline) over RabbitMQ. A delivery that is too early
        is acknowledged: the outbox holds not-before dispatches back and the
        reconciler re-dispatches anything whose worker disappears."""
        assert self.task_executor is not None
        if self._stopping.is_set():
            await message.nack(requeue=True)
            return
        task = self._track()
        try:
            import json

            try:
                data = json.loads(message.body)
                dispatch = DispatchMessage.from_payload(data.get("payload", data))
            except (ValueError, KeyError, TypeError):
                await message.reject(requeue=False)
                return
            try:
                await self.task_executor.handle_dispatch(dispatch)
            except RetryLater:
                pass
            except Exception:
                logger.exception("task dispatch failed; requeueing")
                await asyncio.sleep(1)
                await message.nack(requeue=True)
                return
            await message.ack()
        finally:
            self._in_flight.pop(task, None)

    async def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        logger.info("worker draining", extra={"in_flight": len(self._in_flight)})
        with contextlib.suppress(Exception):
            async with self.session_factory() as session:
                await PostgresWorkerRegistry(session).start_draining(self.worker_id)
                await session.commit()
        for queue, tag in self._consumer_tags:
            with contextlib.suppress(Exception):
                await queue.cancel(tag)

        pending = list(self._in_flight)
        if pending:
            _, still_running = await asyncio.wait(pending, timeout=self.config.shutdown_grace)
            for task in still_running:
                info = self._in_flight.get(task)
                task.cancel()
                if info is not None:
                    with contextlib.suppress(Exception):
                        await self.executor.abandon(
                            info.job_id, info.attempt, "worker shut down before completion"
                        )
            with contextlib.suppress(Exception):
                await asyncio.gather(*still_running, return_exceptions=True)

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        with contextlib.suppress(Exception):
            async with self.session_factory() as session:
                await PostgresWorkerRegistry(session).deregister(self.worker_id)
                await session.commit()
        self.healthy = False
        logger.info("worker stopped", extra={"worker_id": str(self.worker_id)})
