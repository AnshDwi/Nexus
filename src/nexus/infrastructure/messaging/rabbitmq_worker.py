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

Self-fencing (WorkerConfig.heartbeat_ttl set; the daemon always sets it):
the worker tracks its ExecutionAuthority. If no heartbeat has succeeded for
`fence_after` seconds -- or the registry says it was deregistered -- it can
no longer rule out that the scheduler has given its jobs to another worker,
so it fences: cancels the handlers it is running, stops consuming, and hands
those attempts back once PostgreSQL accepts it. It resumes consuming after
its next successful heartbeat, under a new epoch.
"""

import asyncio
import contextlib
import logging
import os
import platform
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.worker_capabilities import WorkerCapabilities
from nexus.infrastructure.coordination.execution_authority import (
    ExecutionAuthority,
    SelfFencingThresholds,
    self_fencing_thresholds,
)
from nexus.infrastructure.coordination.fail_stop import FailStopWatchdog
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.execution.executor import DispatchMessage, RetryLater, TaskExecutor
from nexus.infrastructure.execution.job_executor import (
    SHUTDOWN_REASON,
    Disposition,
    JobExecutor,
    JobMessage,
)
from nexus.infrastructure.messaging.rabbitmq_topology import declare_topology
from nexus.infrastructure.observability.logging import correlation_id_var
from nexus.infrastructure.observability.metrics import worker_self_fences_total

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
    # Self-fencing: the scheduler's WORKER_HEARTBEAT_TTL_SECONDS. None turns
    # it off (callers that predate it); the daemon always passes it.
    heartbeat_ttl: float | None = None
    fence_after: float | None = None  # default derived from TTL and interval
    # Exit the process if handlers are still alive kill_after (TTL - interval)
    # after the last good heartbeat. Off unless asked for (the daemon turns
    # it on from WORKER_FAIL_STOP); requires self-fencing.
    fail_stop: bool = False

    def __post_init__(self) -> None:
        self.thresholds: SelfFencingThresholds | None = None
        if self.fail_stop and self.heartbeat_ttl is None:
            raise ValueError("fail_stop requires self-fencing (heartbeat_ttl)")
        if self.heartbeat_ttl is not None:
            # Raises ValueError for unsafe settings: refuse to start.
            self.thresholds = self_fencing_thresholds(
                ttl=self.heartbeat_ttl,
                heartbeat_interval=self.heartbeat_interval,
                fence_after=self.fence_after,
            )


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
    clock: Callable[[], float] = time.monotonic
    exit_fn: Callable[[int], None] = os._exit  # fail-stop only; tests inject their own

    def __post_init__(self) -> None:
        self._in_flight: dict[asyncio.Task[Any], _InFlight | None] = {}
        self._stopping = asyncio.Event()
        self._consumer_tags: list[tuple[Any, str]] = []
        self._consumers: list[tuple[Any, Callable[[AbstractIncomingMessage], Awaitable[None]]]] = []
        self._paused = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._fail_stop: FailStopWatchdog | None = None
        self.healthy = False
        self.authority: ExecutionAuthority | None = None
        thresholds = self.config.thresholds
        if thresholds is not None:
            self.authority = ExecutionAuthority(
                fence_after=thresholds.fence_after, clock=self.clock
            )
        self.executor.authority = self.authority

    @property
    def worker_id(self) -> UUID:
        return self.executor.worker_id

    async def _register(self) -> None:
        started = self.clock()
        async with self.session_factory() as session:
            await PostgresWorkerRegistry(session).register(
                self.worker_id, local_capabilities(), self.config.capacity
            )
            await session.commit()
        await self._confirm(started)

    async def _confirm(self, started: float) -> None:
        """A heartbeat or registration that started at `started` succeeded."""
        authority = self.authority
        if authority is None:
            return
        if authority.expired() and not authority.fenced:
            # Succeeded, but only after the deadline: fence first; this
            # success says nothing about the gap before it.
            await self._fence("heartbeat deadline passed before a heartbeat succeeded")
        authority.confirm(started)

    async def _heartbeat_loop(self) -> None:
        # Runs until stop() cancels it *after* draining: a draining worker
        # still holds running jobs and must keep its authority (and stay
        # alive in the scheduler's eyes) until they finish.
        while True:
            try:
                started = self.clock()
                async with self.session_factory() as session:
                    registration = await PostgresWorkerRegistry(session).heartbeat(self.worker_id)
                    await session.commit()
                if registration is None:
                    # Deregistered: the scheduler declared this worker dead
                    # and has (or will have) retried its jobs elsewhere.
                    logger.warning("worker no longer registered; fencing, then re-registering")
                    await self._fence("worker was deregistered (declared dead)")
                    await self._register()
                else:
                    await self._confirm(started)
                await self._after_heartbeat()
                self.healthy = self.authority is None or self.authority.is_authorized
            except Exception:
                self.healthy = False
                logger.exception("heartbeat failed")
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _after_heartbeat(self) -> None:
        if self.executor.pending_handbacks:
            await self.executor.flush_handbacks()
        if self._paused and self.authority is not None and self.authority.is_authorized:
            await self._resume_consuming()

    async def _authority_watchdog(self) -> None:
        """Fence as soon as the deadline passes, independent of how long a
        heartbeat attempt is taking."""
        authority = self.authority
        assert authority is not None
        while True:  # cancelled by stop() after draining, like the heartbeat
            if not authority.fenced and authority.expired():
                await self._fence(
                    f"no successful heartbeat within {self.config.thresholds.fence_after:g}s"
                    if self.config.thresholds
                    else "heartbeat deadline passed"
                )
            deadline = authority.deadline
            wait = self.config.heartbeat_interval
            if deadline is not None and not authority.fenced:
                wait = min(wait, max(deadline - self.clock(), 0.0) + 0.01)
            await asyncio.sleep(wait)

    async def _fence(self, reason: str) -> None:
        authority = self.authority
        if authority is None or not authority.fence(reason):
            return
        self.healthy = False
        # Most important first: stop the handlers.
        cancelled = self.executor.cancel_handlers(reason, stale_only=True)
        kind = "deregistered" if "deregistered" in reason else "deadline"
        worker_self_fences_total.labels(reason=kind).inc()
        logger.warning(
            "worker self-fenced: execution authority lost",
            extra={
                "worker_id": str(self.worker_id),
                "reason": reason,
                "epoch": authority.epoch,
                "cancelled_handlers": cancelled,
            },
        )
        await self._pause_consuming()

    async def _pause_consuming(self) -> None:
        if self._paused:
            return
        self._paused = True
        tags, self._consumer_tags = self._consumer_tags, []
        for queue, tag in tags:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(queue.cancel(tag), 2)

    async def _resume_consuming(self) -> None:
        if self._stopping.is_set():
            return
        try:
            for queue, callback in self._consumers:
                tag = await queue.consume(callback)
                self._consumer_tags.append((queue, tag))
        except Exception:
            logger.exception("could not resume consuming; will retry")
            return
        self._paused = False
        logger.info(
            "worker resumed consuming",
            extra={
                "worker_id": str(self.worker_id),
                "epoch": self.authority.epoch if self.authority else None,
            },
        )

    async def start(self) -> None:
        # Fresh state, so a stopped worker object can be started again.
        self._stopping = asyncio.Event()
        self._consumer_tags = []
        self._consumers = []
        self._paused = False
        await self._register()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self.authority is not None:
            self._watchdog_task = asyncio.create_task(self._authority_watchdog())
            thresholds = self.config.thresholds
            if self.config.fail_stop and thresholds is not None:
                self._fail_stop = FailStopWatchdog(
                    authority=self.authority,
                    kill_after=thresholds.kill_after,
                    live_handlers=lambda: self.executor.live_handlers,
                    exit_fn=self.exit_fn,
                    clock=self.clock,
                    poll_interval=min(max(self.config.heartbeat_interval / 5, 0.05), 1.0),
                )
                self._fail_stop.start()
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=self.config.capacity)
        topology = await declare_topology(channel)
        self._consumers.append((topology.jobs, self._on_job_message))
        if self.config.consume_tasks and self.task_executor is not None:
            self._consumers.append((topology.tasks, self._on_task_message))
        for queue, callback in self._consumers:
            tag = await queue.consume(callback)
            self._consumer_tags.append((queue, tag))
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
            if still_running:
                # Cancel the handlers only, so their outcome ("lost") is
                # reported through the normal path; a report already in
                # progress is not interrupted.
                self.executor.cancel_handlers(SHUTDOWN_REASON, stale_only=False)
                _, still_running = await asyncio.wait(
                    still_running, timeout=min(self.config.shutdown_grace, 5.0)
                )
            for task in still_running:  # last resort
                info = self._in_flight.get(task)
                task.cancel()
                if info is not None:
                    with contextlib.suppress(Exception):
                        await self.executor.abandon(info.job_id, info.attempt, SHUTDOWN_REASON)
            with contextlib.suppress(Exception):
                await asyncio.gather(*still_running, return_exceptions=True)

        if self._fail_stop is not None:
            # Before heartbeats stop, so a clean shutdown can never trip it.
            self._fail_stop.stop()
            self._fail_stop = None
        for background in (self._watchdog_task, self._heartbeat_task):
            if background is not None:
                background.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await background
        with contextlib.suppress(Exception):
            await self.executor.flush_handbacks()
        if self.executor.pending_handbacks:
            # Their attempts are still RUNNING under this worker. Staying
            # registered lets the scheduler's dead-worker sweep recover them
            # once the heartbeat ages out; deregistering would hide them
            # until their leases expire.
            logger.warning(
                "not deregistering: attempts could not be handed back",
                extra={"pending": self.executor.pending_handbacks},
            )
        else:
            with contextlib.suppress(Exception):
                async with self.session_factory() as session:
                    await PostgresWorkerRegistry(session).deregister(self.worker_id)
                    await session.commit()
        self.healthy = False
        logger.info("worker stopped", extra={"worker_id": str(self.worker_id)})
