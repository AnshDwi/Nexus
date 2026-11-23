"""C1 reproduction: can a PostgreSQL outage make the scheduler start attempt
N+1 of a job while attempt N's handler is still running?

Opt-in: runs only with NEXUS_C1_REPRO=1, so the normal integration suite
(and CI) is unaffected.

Safety: no container is stopped, restarted or killed. PostgreSQL stays up
the whole time. An in-process TCP gate sits between PostgreSQL and the
components under test (two workers and the scheduler daemon). "Cutting" a
gate aborts their open connections and closes its port, so they see what
they would see during a PostgreSQL restart: dropped connections and
ECONNREFUSED. The test's observer and the API connect to PostgreSQL directly,
so the outage can be watched from the database itself. Only the isolated
`nexus_test` database and `nexus-test` RabbitMQ vhost are touched (see
scripts/test-integration.sh).

Proof of overlap comes from the handler itself: every execution of the job
is recorded in process memory with monotonic start and end times, so
"attempt 2 started while attempt 1 was still inside its handler" is measured
directly. Database rows (job_executions, job_transitions, heartbeats, the
leader lease) are recorded alongside as corroborating evidence.

Outcomes:
  passed                        PASS: no two attempts of the job overlapped
  failed  "FAIL (C1 ...)"       attempt N+1 began while attempt N was running
  skipped "INCONCLUSIVE: ..."   the outage/recovery scenario was not achieved

Two restore orders:
  scheduler_reconnects_first  the scheduler regains PostgreSQL and finishes a
                              maintenance pass before the workers reconnect
                              (deterministic worst case: can it happen?)
  simultaneous_restore        everyone regains PostgreSQL at the same instant
                              and the real loops race (does it happen?)

Timings default to a 1/5 scale of production (TTL 6 s instead of 30 s) so a
run takes about two minutes; NEXUS_C1_TIMINGS=real uses the production
defaults (TTL 30 s, heartbeat 5 s, tick 5 s, leader lease 15 s).

    NEXUS_C1_REPRO=1 scripts/test-integration.sh \\
        tests/integration/test_c1_postgres_outage_overlap.py -v -s -rs
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from nexus.api.security import hash_api_key
from nexus.application.interfaces.task_handler import TaskContext, TaskHandler
from nexus.daemon import run_scheduler
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import InMemoryTaskHandlerRegistry, sleep_handler
from nexus.infrastructure.execution.job_executor import JobExecutor
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_topology import (
    DEAD_LETTER_QUEUE,
    JOBS_QUEUE,
    TASKS_QUEUE,
    declare_topology,
)
from nexus.infrastructure.messaging.rabbitmq_worker import RabbitMQJobWorker, WorkerConfig
from nexus.main import create_app

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("NEXUS_C1_REPRO") != "1",
        reason="C1 reproduction is opt-in: set NEXUS_C1_REPRO=1",
    ),
]

KEY = "c1-key"
AUTH = {"Authorization": f"Bearer {KEY}"}


@dataclass(frozen=True)
class Timings:
    ttl: float  # WORKER_HEARTBEAT_TTL_SECONDS
    heartbeat: float  # WORKER_HEARTBEAT_INTERVAL_SECONDS
    tick: float  # DAEMON_TICK_INTERVAL_SECONDS
    leader_lease: float  # SCHEDULER_LEASE_DURATION_SECONDS
    handler: float  # how long the probe job's handler runs
    job_timeout: int  # far above `handler`, so lease expiry cannot be the trigger
    grace: int  # TASK_LEASE_GRACE_SECONDS


TIMINGS = {
    "fast": Timings(
        ttl=6, heartbeat=1, tick=1, leader_lease=3, handler=45, job_timeout=300, grace=5
    ),
    "real": Timings(
        ttl=30, heartbeat=5, tick=5, leader_lease=15, handler=150, job_timeout=600, grace=30
    ),
}[os.environ.get("NEXUS_C1_TIMINGS", "fast")]


# --- the outage: an in-process TCP gate ------------------------------------------


class PostgresGate:
    """TCP forwarder to PostgreSQL that can be cut and restored."""

    def __init__(self, name: str, upstream_host: str, upstream_port: int) -> None:
        self.name = name
        self._upstream = (upstream_host, upstream_port)
        self._server: asyncio.Server | None = None
        self._live: set[tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = set()
        self.port = 0
        self.is_open = False
        self.aborted_connections = 0

    async def open(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, "127.0.0.1", self.port, reuse_address=True
        )
        self.port = self._server.sockets[0].getsockname()[1]
        self.is_open = True

    async def cut(self) -> None:
        """Abort every live connection (RST) and stop listening (ECONNREFUSED)."""
        self.is_open = False
        for client, upstream in list(self._live):
            for writer in (client, upstream):
                writer.transport.abort()
            self.aborted_connections += 1
        self._live.clear()
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._server.wait_closed(), 5)
            self._server = None

    async def _handle(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        if not self.is_open:
            client_w.transport.abort()
            return
        try:
            up_r, up_w = await asyncio.open_connection(*self._upstream)
        except OSError:
            client_w.transport.abort()
            return
        pair = (client_w, up_w)
        self._live.add(pair)

        async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while data := await reader.read(65536):
                    writer.write(data)
                    await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.transport.abort()

        try:
            await asyncio.gather(pump(client_r, up_w), pump(up_r, client_w))
        finally:
            self._live.discard(pair)


# --- proof of execution: the handler records itself -------------------------------


@dataclass
class HandlerRun:
    worker: str
    job_id: str
    attempt: int
    started: float  # monotonic seconds
    ended: float | None = None
    outcome: str = "running"  # running | returned | cancelled | raised


class Probe:
    """Wraps the real `test.sleep` handler and records every execution."""

    def __init__(self) -> None:
        self.runs: list[HandlerRun] = []

    def handler_for(self, worker: str) -> TaskHandler:
        async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
            run = HandlerRun(worker, str(context.task_id), context.attempt, time.monotonic())
            self.runs.append(run)
            try:
                result = await sleep_handler(payload, context)
                run.outcome = "returned"
                return result
            except asyncio.CancelledError:
                run.outcome = "cancelled"
                raise
            except Exception:
                run.outcome = "raised"
                raise
            finally:
                run.ended = time.monotonic()

        return handler

    def of(self, job_id: str) -> list[HandlerRun]:
        return sorted((r for r in self.runs if r.job_id == job_id), key=lambda r: r.started)


def find_overlap(runs: list[HandlerRun]) -> tuple[HandlerRun, HandlerRun] | None:
    """Two different attempts whose handler intervals intersect."""
    for i, first in enumerate(runs):
        for second in runs[i + 1 :]:
            if first.attempt == second.attempt:
                continue
            first_end = first.ended if first.ended is not None else float("inf")
            second_end = second.ended if second.ended is not None else float("inf")
            if second.started < first_end and first.started < second_end:
                return first, second
    return None


# --- observation straight from PostgreSQL ------------------------------------------


async def until[T](
    check: Callable[[], Awaitable[T | None]], timeout: float, interval: float = 0.2
) -> T | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        value = await check()
        if value:
            return value
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(interval)


@dataclass
class Evidence:
    mode: str
    timings: dict[str, Any]
    t0_monotonic: float
    db_t0: datetime | None = None
    job_id: str = ""
    events: list[dict[str, Any]] = field(default_factory=list)
    handler_runs: list[dict[str, Any]] = field(default_factory=list)
    executions: list[dict[str, Any]] = field(default_factory=list)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    lease_renewals_after_restore: list[str] = field(default_factory=list)

    def rel(self, monotonic: float | None) -> float | None:
        return None if monotonic is None else round(monotonic - self.t0_monotonic, 3)

    def db_rel(self, ts: datetime | None) -> float | None:
        if ts is None or self.db_t0 is None:
            return None
        return round((ts - self.db_t0).total_seconds(), 3)

    def mark(self, what: str, **details: Any) -> None:
        self.events.append({"t": self.rel(time.monotonic()), "event": what, **details})

    def dump(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


class Observer:
    def __init__(self, session_factory: Any, evidence: Evidence) -> None:
        self._sf = session_factory
        self._ev = evidence

    async def _rows(self, sql: str, **params: Any) -> list[Any]:
        async with self._sf() as session:
            return list((await session.execute(text(sql), params)).all())

    async def db_now(self) -> datetime:
        value: datetime = (await self._rows("SELECT statement_timestamp()"))[0][0]
        return value

    async def job(self, job_id: str) -> Any:
        rows = await self._rows(
            "SELECT state, attempt, worker_id FROM jobs WHERE job_id = :j", j=job_id
        )
        return rows[0] if rows else None

    async def executions(self, job_id: str) -> list[dict[str, Any]]:
        rows = await self._rows(
            "SELECT id, attempt, worker_id, status, started_at, finished_at FROM job_executions "
            "WHERE job_id = :j ORDER BY id",
            j=job_id,
        )
        return [
            {
                "execution_id": r[0],
                "attempt": r[1],
                "worker_id": str(r[2]),
                "status": r[3],
                "started_at": r[4].isoformat(),
                "started_rel": self._ev.db_rel(r[4]),
                "finished_rel": self._ev.db_rel(r[5]),
            }
            for r in rows
        ]

    async def transitions(self, job_id: str) -> list[dict[str, Any]]:
        rows = await self._rows(
            "SELECT id, attempt, from_state, to_state, occurred_at, reason FROM job_transitions "
            "WHERE job_id = :j ORDER BY id",
            j=job_id,
        )
        return [
            {
                "id": r[0],
                "attempt": r[1],
                "transition": f"{r[2]} -> {r[3]}",
                "t_rel": self._ev.db_rel(r[4]),
                "reason": r[5],
            }
            for r in rows
        ]

    async def heartbeat_ages(self) -> dict[str, float]:
        rows = await self._rows(
            "SELECT worker_id, EXTRACT(EPOCH FROM statement_timestamp() - last_heartbeat_at) "
            "FROM worker_registrations"
        )
        return {str(w): float(age) for w, age in rows}

    async def lease_expiry(self) -> datetime | None:
        rows = await self._rows("SELECT expires_at FROM scheduler_leader_lease")
        return rows[0][0] if rows else None


# --- the stack under test ----------------------------------------------------------


@dataclass
class C1Stack:
    client: AsyncClient
    observer: Observer
    probe: Probe
    worker_gate: PostgresGate
    scheduler_gate: PostgresGate
    worker_ids: dict[str, str]
    evidence: Evidence


async def _reset(session_factory: Any, connection: Any) -> None:
    async with session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE job_executions, job_transitions, jobs, inbox_records, outbox_records, "
                "worker_registrations, scheduler_leader_lease, task_transitions, tasks "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()
    channel = await connection.channel()
    await declare_topology(channel)
    for name in (JOBS_QUEUE, TASKS_QUEUE, DEAD_LETTER_QUEUE):
        await (await channel.declare_queue(name, passive=True)).purge()
    await channel.close()


@pytest.fixture(params=["scheduler_reconnects_first", "simultaneous_restore"])
async def c1(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[C1Stack]:
    t = TIMINGS
    for name, value in {
        "API_KEYS": f"c1:{hash_api_key(KEY)}",
        "EVENT_BUS": "rabbitmq",
        "REDIS_URL": "",
        "TASK_ENABLE_TEST_HANDLERS": "true",
        "RATE_LIMIT_WRITES_PER_MINUTE": "1000",
        "WORKER_HEARTBEAT_TTL_SECONDS": str(t.ttl),
        "WORKER_HEARTBEAT_INTERVAL_SECONDS": str(t.heartbeat),
        "DAEMON_TICK_INTERVAL_SECONDS": str(t.tick),
        "SCHEDULER_LEASE_DURATION_SECONDS": str(t.leader_lease),
        "TASK_LEASE_GRACE_SECONDS": str(t.grace),
        "OUTBOX_RELAY_POLL_INTERVAL_SECONDS": "0.2",
        "DB_POOL_TIMEOUT_SECONDS": "2",
        "DAEMON_HEALTH_PORT": "0",
        "DEAD_LETTER_PRUNE_RESOLVED": "false",
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    settings = get_settings()
    if settings.database_url_override or settings.postgres_url:
        pytest.skip("INCONCLUSIVE: the gate needs POSTGRES_HOST/PORT, not DATABASE_URL")

    evidence = Evidence(mode=request.param, timings=asdict(t), t0_monotonic=time.monotonic())
    direct_engine = create_engine(settings.database_url, settings)
    direct_sf = create_session_factory(direct_engine)
    observer = Observer(direct_sf, evidence)
    evidence.db_t0 = await observer.db_now()

    setup_connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    await _reset(direct_sf, setup_connection)

    worker_gate = PostgresGate("workers", settings.postgres_host, settings.postgres_port)
    scheduler_gate = PostgresGate("scheduler", settings.postgres_host, settings.postgres_port)
    await worker_gate.open()
    await scheduler_gate.open()

    def through(gate: PostgresGate) -> Any:
        return settings.model_copy(
            update={"postgres_host": "127.0.0.1", "postgres_port": gate.port}
        )

    probe = Probe()
    workers: list[RabbitMQJobWorker] = []
    worker_ids: dict[str, str] = {}
    resources: list[Callable[[], Awaitable[None]]] = []
    for label in ("A", "B"):  # two separate "processes": own engine, own broker connection
        gated = through(worker_gate)
        engine = create_engine(gated.database_url, gated)
        sf = create_session_factory(engine)
        connection = await create_rabbitmq_connection(settings.rabbitmq_url)
        worker_id = uuid4()
        worker_ids[str(worker_id)] = label
        workers.append(
            RabbitMQJobWorker(
                connection=connection,
                session_factory=sf,
                executor=JobExecutor(
                    sf,
                    InMemoryTaskHandlerRegistry({"test.sleep": probe.handler_for(label)}),
                    worker_id=worker_id,
                    lease_grace=timedelta(seconds=t.grace),
                ),
                task_executor=None,
                config=WorkerConfig(
                    capacity=2,
                    heartbeat_interval=t.heartbeat,
                    shutdown_grace=1,
                    consume_tasks=False,
                ),
            )
        )
        resources += [connection.close, engine.dispose]

    stop_scheduler = asyncio.Event()
    scheduler = asyncio.create_task(run_scheduler(through(scheduler_gate), stop_scheduler))
    for worker in workers:
        await worker.start()

    app = create_app()  # API: direct connection, used only to submit the job
    try:
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://nexus.test") as client:
                yield C1Stack(
                    client, observer, probe, worker_gate, scheduler_gate, worker_ids, evidence
                )
    finally:
        for gate in (worker_gate, scheduler_gate):
            if not gate.is_open:
                await gate.open()
        stop_scheduler.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(scheduler, 30)
        for worker in workers:
            with contextlib.suppress(Exception):
                await worker.stop()
        for close in resources:
            with contextlib.suppress(Exception):
                await close()
        for gate in (worker_gate, scheduler_gate):
            await gate.cut()
        await setup_connection.close()
        await direct_engine.dispose()
        get_settings.cache_clear()


# --- the scenario ------------------------------------------------------------------


async def test_c1_no_overlapping_attempts_after_postgres_outage(c1: C1Stack) -> None:
    t, ev, obs, probe = TIMINGS, c1.evidence, c1.observer, c1.probe

    def inconclusive(reason: str) -> None:
        print(ev.dump())
        pytest.skip(f"INCONCLUSIVE: {reason}")

    # 1. A long-running job whose first attempt is inside its handler.
    r = await c1.client.post(
        "/api/v1/jobs",
        json={
            "name": "c1-probe",
            "job_type": "test.sleep",
            "payload": {"seconds": t.handler},
            "timeout_seconds": t.job_timeout,
            "max_attempts": 3,
            "initial_backoff_seconds": 0,
            "max_backoff_seconds": 0,
        },
        headers={**AUTH, "Idempotency-Key": str(uuid4())},
    )
    assert r.status_code == 201, r.text
    job_id = ev.job_id = r.json()["job_id"]
    ev.mark("job submitted", job_id=job_id)

    async def first_attempt_running() -> HandlerRun | None:
        runs = probe.of(job_id)
        row = await obs.job(job_id)
        if runs and runs[0].outcome == "running" and row and row[0] == "running":
            return runs[0]
        return None

    first = await until(first_attempt_running, 30)
    if first is None:
        inconclusive("attempt 1 never started running")
    assert first is not None
    ev.mark(
        "attempt 1 handler running",
        worker=first.worker,
        attempt=first.attempt,
        executions=await obs.executions(job_id),
        heartbeat_ages=await obs.heartbeat_ages(),
    )

    # 2. The outage: workers and scheduler lose PostgreSQL.
    await c1.worker_gate.cut()
    await c1.scheduler_gate.cut()
    ev.mark(
        "postgres cut for workers and scheduler",
        aborted_connections=c1.worker_gate.aborted_connections
        + c1.scheduler_gate.aborted_connections,
    )

    stale_after = t.ttl + 2 * t.tick  # comfortably past the TTL

    async def every_worker_stale() -> dict[str, float] | None:
        ages = await obs.heartbeat_ages()
        return ages if ages and min(ages.values()) > stale_after else None

    stale = await until(every_worker_stale, stale_after * 3 + 30, interval=0.5)
    if stale is None:
        inconclusive("worker heartbeats never went stale; the outage did not take effect")
    if first.outcome != "running":
        inconclusive("attempt 1 finished during the outage; raise the handler duration")
    ev.mark("heartbeats stale beyond the TTL; attempt 1 still in its handler", ages=stale)

    # 3. PostgreSQL comes back.
    restore_db = await obs.db_now()
    if ev.mode == "scheduler_reconnects_first":
        await c1.scheduler_gate.open()
        ev.mark("postgres restored for the scheduler only")
        seen: list[datetime] = []

        async def one_full_pass_or_recovery() -> bool:
            expiry = await obs.lease_expiry()
            if expiry and expiry > restore_db and expiry not in seen:
                seen.append(expiry)
            recovered = any(
                tr["transition"] == "running -> failed" for tr in await obs.transitions(job_id)
            )
            return recovered or len(seen) >= 2  # 2 renewals => the first pass completed

        if not await until(one_full_pass_or_recovery, t.tick * 10 + t.leader_lease * 2 + 15):
            inconclusive("the scheduler did not complete a maintenance pass after restore")
        ev.lease_renewals_after_restore = [s.isoformat() for s in seen]
        ev.mark("scheduler pass done; restoring postgres for the workers")
        await c1.worker_gate.open()
    else:
        await c1.scheduler_gate.open()
        await c1.worker_gate.open()
        ev.mark("postgres restored for everyone at once")

    # 4. Watch until attempt 1's handler has ended and the job has settled.
    async def settled() -> bool:
        row = await obs.job(job_id)
        handlers_done = all(run.outcome != "running" for run in probe.of(job_id))
        return handlers_done and row is not None and row[0] in ("completed", "failed", "cancelled")

    finished = await until(settled, t.handler + t.job_timeout / 10 + 60, interval=0.5)
    restore_rel = ev.db_rel(restore_db)
    ev.mark("observation ended", settled=bool(finished), restore_t_rel_db=restore_rel)

    ev.handler_runs = [
        {**asdict(run), "started": ev.rel(run.started), "ended": ev.rel(run.ended)}
        for run in probe.of(job_id)
    ]
    ev.executions = await obs.executions(job_id)
    for execution in ev.executions:
        execution["worker"] = c1.worker_ids.get(execution["worker_id"], "?")
    ev.transitions = await obs.transitions(job_id)

    # 5. Verdict.
    overlap = find_overlap(probe.of(job_id))
    print(ev.dump())
    if overlap:
        a, b = overlap
        pytest.fail(
            "FAIL (C1 reproduced): attempt "
            f"{b.attempt} started on worker {b.worker} at t={ev.rel(b.started)}s while attempt "
            f"{a.attempt} was still in its handler on worker {a.worker} "
            f"(started t={ev.rel(a.started)}s, ended t={ev.rel(a.ended)}s, {a.outcome}). "
            "Evidence above.",
            pytrace=False,
        )
    if not finished:
        inconclusive("the job did not settle before the deadline")
    # PASS: every handler run of this job was strictly sequential.
