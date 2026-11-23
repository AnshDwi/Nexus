"""Worker self-fencing against real PostgreSQL and RabbitMQ (fast, not opt-in).

A worker with self-fencing on (heartbeat 0.25 s, TTL 2 s -> fences after
1.5 s without a successful heartbeat). Where a worker must lose the
database, it connects through the in-process TCP gate from the C1 drill.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from nexus.api.security import hash_api_key
from nexus.application.interfaces.task_handler import TaskContext
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution import job_executor as job_executor_module
from nexus.infrastructure.execution.handlers import InMemoryTaskHandlerRegistry
from nexus.infrastructure.execution.job_executor import JobExecutor
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_topology import JOBS_QUEUE
from nexus.infrastructure.messaging.rabbitmq_worker import RabbitMQJobWorker, WorkerConfig
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.main import create_app
from tests.integration.test_c1_postgres_outage_overlap import PostgresGate, _reset, until

pytestmark = pytest.mark.integration

KEY = "fence-key"
AUTH = {"Authorization": f"Bearer {KEY}"}
HEARTBEAT, TTL = 0.25, 2.0


@dataclass
class Runs:
    """Handler executions: (attempt, outcome)."""

    log: list[list[Any]] = field(default_factory=list)
    on_return: Callable[[], Awaitable[None]] | None = None

    async def handler(self, payload: dict[str, Any], context: TaskContext) -> Any:
        entry = [context.attempt, "running"]
        self.log.append(entry)
        try:
            await asyncio.sleep(float(payload.get("first", 30)) if context.attempt == 1 else 0.05)
            if self.on_return is not None:
                hook, self.on_return = self.on_return, None
                await hook()
            entry[1] = "returned"
            return {"attempt": context.attempt}
        except asyncio.CancelledError:
            entry[1] = "cancelled"
            raise


@dataclass
class Env:
    client: AsyncClient
    sf: Any
    connection: Any
    gate: PostgresGate
    runs: Runs
    make_worker: Callable[..., Awaitable[RabbitMQJobWorker]]
    exits: list[tuple[int, float]]  # fail-stop calls: (code, monotonic time)

    async def submit(self, first: float) -> str:
        r = await self.client.post(
            "/api/v1/jobs",
            json={
                "name": "fence",
                "job_type": "test.sleep",
                "payload": {"first": first},
                "timeout_seconds": 120,
                "max_attempts": 3,
                "initial_backoff_seconds": 0,
                "max_backoff_seconds": 0,
            },
            headers={**AUTH, "Idempotency-Key": str(uuid4())},
        )
        assert r.status_code == 201, r.text
        job_id: str = r.json()["job_id"]
        return job_id

    async def sql(self, statement: str, **params: Any) -> list[Any]:
        async with self.sf() as session:
            result = await session.execute(text(statement), params)
            rows = list(result.all()) if result.returns_rows else []
            await session.commit()
            return rows

    async def job(self, job_id: str) -> tuple[str, int]:
        rows = await self.sql("SELECT state, attempt FROM jobs WHERE job_id = :j", j=job_id)
        return rows[0][0], rows[0][1]

    async def executions(self, job_id: str) -> list[tuple[int, str, str | None]]:
        rows = await self.sql(
            "SELECT attempt, status, error FROM job_executions WHERE job_id = :j ORDER BY id",
            j=job_id,
        )
        return [(r[0], r[1], r[2]) for r in rows]

    async def wait_state(self, job_id: str, *states: str, timeout: float = 20) -> bool:
        async def check() -> bool:
            return (await self.job(job_id))[0] in states

        return bool(await until(check, timeout, interval=0.05))

    async def ready_messages(self) -> int:
        channel = await self.connection.channel()
        try:
            queue = await channel.declare_queue(JOBS_QUEUE, passive=True)
            return int(queue.declaration_result.message_count or 0)
        finally:
            await channel.close()


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Env]:
    monkeypatch.setattr(job_executor_module, "_REPORT_INITIAL_DELAY", 0.05)
    monkeypatch.setattr(job_executor_module, "_REPORT_MAX_DELAY", 0.2)
    monkeypatch.setenv("API_KEYS", f"acme:{hash_api_key(KEY)}")
    monkeypatch.setenv("EVENT_BUS", "rabbitmq")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("TASK_ENABLE_TEST_HANDLERS", "true")
    monkeypatch.setenv("RATE_LIMIT_WRITES_PER_MINUTE", "1000")
    get_settings.cache_clear()
    settings = get_settings()
    engine = create_engine(settings.database_url, settings)
    sf = create_session_factory(engine)
    connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    await _reset(sf, connection)
    gate = PostgresGate("worker", settings.postgres_host, settings.postgres_port)
    await gate.open()
    gated = settings.model_copy(update={"postgres_host": "127.0.0.1", "postgres_port": gate.port})
    gated_engine = create_engine(gated.database_url, gated)
    gated_sf = create_session_factory(gated_engine)
    runs = Runs()
    workers: list[RabbitMQJobWorker] = []
    exits: list[tuple[int, float]] = []

    def record_exit(code: int) -> None:  # never a real os._exit in tests
        exits.append((code, time.monotonic()))

    async def make_worker(
        *, through_gate: bool = False, grace: float = 0.5, handler: Any = None
    ) -> RabbitMQJobWorker:
        factory = gated_sf if through_gate else sf
        worker = RabbitMQJobWorker(
            connection=connection,
            session_factory=factory,
            executor=JobExecutor(
                factory,
                InMemoryTaskHandlerRegistry({"test.sleep": handler or runs.handler}),
                worker_id=uuid4(),
                lease_grace=timedelta(seconds=5),
            ),
            task_executor=None,
            config=WorkerConfig(
                capacity=2,
                heartbeat_interval=HEARTBEAT,
                shutdown_grace=grace,
                consume_tasks=False,
                heartbeat_ttl=TTL,
                fail_stop=True,
            ),
            exit_fn=record_exit,
        )
        await worker.start()
        workers.append(worker)
        return worker

    app = create_app()
    async with app.router.lifespan_context(app):
        publisher = app.state.container.publisher

        async def relay_loop() -> None:  # what the scheduler daemon does (retries)
            while True:
                await relay_pending_events(sf, publisher, settings, max_events=50)
                await asyncio.sleep(0.1)

        relay = asyncio.create_task(relay_loop())
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://nexus.test") as client:
            yield Env(client, sf, connection, gate, runs, make_worker, exits)
        relay.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay
    if not gate.is_open:
        await gate.open()
    for worker in workers:
        await worker.stop()
    await gate.cut()
    await connection.close()
    await gated_engine.dispose()
    await engine.dispose()
    get_settings.cache_clear()


async def test_deregistered_worker_fences_and_hands_its_attempt_back(env: Env):
    """R8: the registry no longer knows the worker (the scheduler declared it
    dead). Its next heartbeat must fence it before it re-registers: attempt 1
    is cancelled and handed back (lost), and the job is retried."""
    worker = await env.make_worker()
    job_id = await env.submit(first=30)
    assert await env.wait_state(job_id, "running")
    await env.sql("DELETE FROM worker_registrations WHERE worker_id = :w", w=worker.worker_id)

    assert await env.wait_state(job_id, "completed", timeout=20)
    assert [tuple(e) for e in env.runs.log] == [(1, "cancelled"), (2, "returned")]
    executions = await env.executions(job_id)
    assert [(a, s) for a, s, _ in executions] == [(1, "lost"), (2, "completed")]
    assert "self-fenced" in (executions[0][2] or "")
    assert worker.authority is not None and worker.authority.epoch == 1
    rows = await env.sql(
        "SELECT 1 FROM worker_registrations WHERE worker_id = :w", w=worker.worker_id
    )
    assert rows, "the worker should have re-registered after fencing"
    assert env.exits == []  # cooperative handler: fencing sufficed


async def test_fenced_worker_claims_nothing_until_its_authority_returns(env: Env):
    """R7: while fenced the worker stops consuming; a new job's message waits
    in the broker, unclaimed, and runs once a heartbeat succeeds again."""
    worker = await env.make_worker(through_gate=True)
    authority = worker.authority
    assert authority is not None
    await env.gate.cut()

    async def fenced() -> bool:
        return authority.fenced

    started = time.monotonic()
    assert await until(fenced, 10, interval=0.05)
    assert time.monotonic() - started < TTL, "fenced later than the TTL"

    job_id = await env.submit(first=0.05)

    async def waiting_in_broker() -> bool:
        return await env.ready_messages() == 1

    assert await until(waiting_in_broker, 10, interval=0.1)
    assert await env.job(job_id) == ("queued", 1)
    assert env.runs.log == []

    await env.gate.open()
    assert await env.wait_state(job_id, "completed", timeout=20)
    assert not authority.fenced
    assert [tuple(e) for e in env.runs.log] == [(1, "returned")]
    assert env.exits == []


async def test_shutdown_does_not_interrupt_a_report_in_progress(env: Env):
    """R10: the handler has returned and its result is being recorded while
    PostgreSQL is briefly unreachable when the worker is asked to stop. The
    report must be allowed to finish (completed), not be cancelled and
    abandoned as lost."""
    worker = await env.make_worker(through_gate=True, grace=0.5)

    async def drop_database_briefly() -> None:
        await env.gate.cut()

        async def restore() -> None:
            await asyncio.sleep(0.7)
            await env.gate.open()

        asyncio.get_running_loop().create_task(restore())

    env.runs.on_return = drop_database_briefly
    job_id = await env.submit(first=0.2)

    async def returned() -> bool:
        return bool(env.runs.log) and env.runs.log[0][1] == "returned"

    assert await until(returned, 10, interval=0.02)
    await worker.stop()

    assert await env.job(job_id) == ("completed", 1)
    assert [(a, s) for a, s, _ in await env.executions(job_id)] == [(1, "completed")]
    assert env.exits == []  # a clean shutdown never trips fail-stop


async def test_fail_stop_exits_when_a_handler_ignores_cancellation(env: Env):
    """A handler that swallows the fence's cancellation keeps running; the
    fail-stop watchdog must exit the process (here: the injected function)
    at kill_after = TTL - interval after the last good heartbeat."""
    release = asyncio.Event()
    cancellations = 0

    async def stubborn(payload: dict[str, Any], context: TaskContext) -> Any:
        nonlocal cancellations
        while not release.is_set():
            try:
                await asyncio.sleep(0.02)
            except asyncio.CancelledError:
                cancellations += 1  # swallowed: the anti-pattern under test
        return {}

    worker = await env.make_worker(through_gate=True, handler=stubborn)
    authority = worker.authority
    assert authority is not None
    job_id = await env.submit(first=0)
    assert await env.wait_state(job_id, "running")
    await env.gate.cut()

    async def exited() -> bool:
        return bool(env.exits)

    assert await until(exited, 10, interval=0.02)
    code, at = env.exits[0]
    assert code == 70
    assert cancellations >= 1  # the fence did try to cancel it
    assert authority.last_ok is not None
    assert at - authority.last_ok >= TTL - HEARTBEAT - 0.05  # not before kill_after
    release.set()
    await env.gate.open()


async def test_fail_stop_exits_even_while_the_event_loop_is_blocked(env: Env):
    """A handler that blocks the event loop stops heartbeats *and* the
    asyncio fencing watchdog; only the fail-stop thread can still act."""

    async def blocking(payload: dict[str, Any], context: TaskContext) -> Any:
        time.sleep(TTL + 1.0)  # blocks the whole event loop (the anti-pattern)
        return {}

    await env.make_worker(handler=blocking)
    job_id = await env.submit(first=0)
    assert await env.wait_state(job_id, "running")

    async def exited() -> bool:
        return bool(env.exits)

    assert await until(exited, 15, interval=0.05)
    assert env.exits[0][0] == 70


async def test_graceful_drain_longer_than_fence_after_still_reports_the_result(env: Env):
    """The worker keeps heartbeating while it drains, so a job that finishes
    during a drain longer than fence_after (1.5 s) is reported as completed
    -- not handed back -- and fail-stop (1.75 s) never fires."""
    worker = await env.make_worker(grace=4.0)
    job_id = await env.submit(first=2.5)
    assert await env.wait_state(job_id, "running")
    await worker.stop()  # waits for the 2.5 s handler: longer than fence_after

    assert await env.job(job_id) == ("completed", 1)
    assert [(a, s) for a, s, _ in await env.executions(job_id)] == [(1, "completed")]
    assert [tuple(e) for e in env.runs.log] == [(1, "returned")]
    assert env.exits == []
