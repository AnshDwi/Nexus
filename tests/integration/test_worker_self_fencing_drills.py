"""Outage drills for worker self-fencing (Fix 2) and result reporting (C2).

Opt-in like the C1 drill (NEXUS_C1_REPRO=1) and built on its harness: an
in-process TCP gate per worker sits between PostgreSQL and that worker, so
one worker can lose the database while the scheduler and the other worker
keep it. No container is stopped; only `nexus_test` and the `nexus-test`
vhost are touched.

    NEXUS_C1_REPRO=1 scripts/test-integration.sh \\
        tests/integration/test_worker_self_fencing_drills.py -v -s -rs

Outcomes: passed = PASS; failed = FAIL with evidence; skipped
"INCONCLUSIVE: ..." = the scenario was not achieved.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from nexus.api.security import hash_api_key
from nexus.application.interfaces.task_handler import TaskContext, TaskHandler
from nexus.daemon import run_scheduler
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import InMemoryTaskHandlerRegistry, sleep_handler
from nexus.infrastructure.execution.job_executor import JobExecutor
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_worker import RabbitMQJobWorker, WorkerConfig
from nexus.main import create_app
from tests.integration.test_c1_postgres_outage_overlap import (
    Evidence,
    HandlerRun,
    Observer,
    PostgresGate,
    _reset,
    find_overlap,
    until,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("NEXUS_C1_REPRO") != "1",
        reason="outage drills are opt-in: set NEXUS_C1_REPRO=1",
    ),
]

KEY = "drill-key"
AUTH = {"Authorization": f"Bearer {KEY}"}


@dataclass(frozen=True)
class Timings:
    ttl: float = 6
    heartbeat: float = 1
    tick: float = 1
    leader_lease: float = 3
    grace: int = 3


T = Timings()


def worker_config() -> WorkerConfig:
    """How these drills build a worker's config: the same values the daemon
    derives from settings."""
    return WorkerConfig(
        capacity=2,
        heartbeat_interval=T.heartbeat,
        shutdown_grace=1,
        consume_tasks=False,
        heartbeat_ttl=T.ttl,  # self-fencing on, as run_rabbitmq_worker configures it
    )


class HookedProbe:
    """The real `test.sleep` handler, recorded per run, with an optional
    hook awaited just before a given attempt's handler returns."""

    def __init__(self) -> None:
        self.runs: list[HandlerRun] = []
        self.before_return: dict[int, Callable[[str], Awaitable[None]]] = {}

    def handler_for(self, worker: str) -> TaskHandler:
        async def handler(payload: dict[str, Any], context: TaskContext) -> Any:
            run = HandlerRun(worker, str(context.task_id), context.attempt, time.monotonic())
            self.runs.append(run)
            try:
                result = await sleep_handler(payload, context)
                hook = self.before_return.pop(context.attempt, None)
                if hook is not None:
                    await hook(worker)
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


@dataclass
class Drill:
    client: AsyncClient
    observer: Observer
    probe: HookedProbe
    gates: dict[str, PostgresGate]
    evidence: Evidence
    lease_seconds: float

    async def submit(self, seconds: float, timeout: int) -> str:
        r = await self.client.post(
            "/api/v1/jobs",
            json={
                "name": "drill",
                "job_type": "test.sleep",
                "payload": {"seconds": seconds},
                "timeout_seconds": timeout,
                "max_attempts": 3,
                "initial_backoff_seconds": 0,
                "max_backoff_seconds": 0,
            },
            headers={**AUTH, "Idempotency-Key": str(uuid4())},
        )
        assert r.status_code == 201, r.text
        job_id: str = r.json()["job_id"]
        self.evidence.job_id = job_id
        self.evidence.mark("job submitted", job_id=job_id)
        return job_id

    async def collect(self, job_id: str) -> None:
        ev = self.evidence
        ev.handler_runs = [
            {**asdict(run), "started": ev.rel(run.started), "ended": ev.rel(run.ended)}
            for run in self.probe.of(job_id)
        ]
        ev.executions = await self.observer.executions(job_id)
        ev.transitions = await self.observer.transitions(job_id)

    def inconclusive(self, reason: str) -> None:
        print(self.evidence.dump())
        pytest.skip(f"INCONCLUSIVE: {reason}")


@pytest.fixture
async def drill(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Drill]:
    for name, value in {
        "API_KEYS": f"drill:{hash_api_key(KEY)}",
        "EVENT_BUS": "rabbitmq",
        "REDIS_URL": "",
        "TASK_ENABLE_TEST_HANDLERS": "true",
        "RATE_LIMIT_WRITES_PER_MINUTE": "1000",
        "WORKER_HEARTBEAT_TTL_SECONDS": str(T.ttl),
        "WORKER_HEARTBEAT_INTERVAL_SECONDS": str(T.heartbeat),
        "DAEMON_TICK_INTERVAL_SECONDS": str(T.tick),
        "SCHEDULER_LEASE_DURATION_SECONDS": str(T.leader_lease),
        "TASK_LEASE_GRACE_SECONDS": str(T.grace),
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

    evidence = Evidence(mode=request.node.name, timings=asdict(T), t0_monotonic=time.monotonic())
    direct_engine = create_engine(settings.database_url, settings)
    direct_sf = create_session_factory(direct_engine)
    observer = Observer(direct_sf, evidence)
    evidence.db_t0 = await observer.db_now()
    setup_connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    await _reset(direct_sf, setup_connection)

    probe = HookedProbe()
    gates: dict[str, PostgresGate] = {}
    workers: list[RabbitMQJobWorker] = []
    closers: list[Callable[[], Awaitable[None]]] = []
    for label in ("A", "B"):
        gate = PostgresGate(label, settings.postgres_host, settings.postgres_port)
        await gate.open()
        gates[label] = gate
        gated = settings.model_copy(
            update={"postgres_host": "127.0.0.1", "postgres_port": gate.port}
        )
        engine = create_engine(gated.database_url, gated)
        sf = create_session_factory(engine)
        connection = await create_rabbitmq_connection(settings.rabbitmq_url)
        workers.append(
            RabbitMQJobWorker(
                connection=connection,
                session_factory=sf,
                executor=JobExecutor(
                    sf,
                    InMemoryTaskHandlerRegistry({"test.sleep": probe.handler_for(label)}),
                    worker_id=uuid4(),
                    lease_grace=timedelta(seconds=T.grace),
                ),
                task_executor=None,
                config=worker_config(),
            )
        )
        closers += [connection.close, engine.dispose]

    stop_scheduler = asyncio.Event()  # the scheduler keeps a direct connection
    scheduler = asyncio.create_task(run_scheduler(settings, stop_scheduler))
    for worker in workers:
        await worker.start()

    app = create_app()
    try:
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://nexus.test") as client:
                yield Drill(client, observer, probe, gates, evidence, T.leader_lease)
    finally:
        for gate in gates.values():
            if not gate.is_open:
                await gate.open()
        stop_scheduler.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(scheduler, 30)
        for worker in workers:
            with contextlib.suppress(Exception):
                await worker.stop()
        for close in closers:
            with contextlib.suppress(Exception):
                await close()
        for gate in gates.values():
            await gate.cut()
        await setup_connection.close()
        await direct_engine.dispose()
        get_settings.cache_clear()


async def _scheduler_window_open(d: Drill) -> bool:
    """Fix 1: wait until the leader has watched heartbeats for a full TTL, so
    its dead-worker sweep is live (a worker-only partition is then detected)."""
    acquired = await until(d.observer.lease_expiry, 30)
    if acquired is None:
        return False
    since = acquired - timedelta(seconds=d.lease_seconds)

    async def open_() -> bool:
        return (await d.observer.db_now() - since).total_seconds() > T.ttl + 3 * T.tick

    return bool(await until(open_, T.ttl * 4 + 30))


async def test_worker_only_partition_does_not_overlap_attempts(drill: Drill) -> None:
    """Design test 1: one worker loses PostgreSQL for longer than the TTL while
    the scheduler and the other worker keep it. The scheduler (window open)
    rightly declares that worker dead and retries its job on the other one;
    the isolated worker must not still be running attempt 1 by then."""
    d, ev, obs = drill, drill.evidence, drill.observer
    if not await _scheduler_window_open(d):
        d.inconclusive("the scheduler's observation window never opened")
    ev.mark("scheduler window open")

    job_id = await d.submit(seconds=30, timeout=300)

    async def first_running() -> HandlerRun | None:
        runs = d.probe.of(job_id)
        row = await obs.job(job_id)
        ok = runs and runs[0].outcome == "running" and row and row[0] == "running"
        return runs[0] if ok else None

    first = await until(first_running, 30)
    if first is None:
        d.inconclusive("attempt 1 never started")
    assert first is not None
    isolated = d.gates[first.worker]
    await isolated.cut()
    ev.mark("postgres cut for one worker only", worker=first.worker)

    async def swept() -> bool:
        return any(
            "stopped heartbeating" in (t["reason"] or "") for t in await obs.transitions(job_id)
        )

    if not await until(swept, T.ttl * 4 + 30):
        d.inconclusive("the scheduler never declared the isolated worker dead")
    ev.mark("scheduler declared the isolated worker dead", attempt1_outcome=first.outcome)

    async def second_started() -> bool:
        return len(d.probe.of(job_id)) >= 2

    await until(second_started, 30)
    await isolated.open()
    ev.mark("postgres restored for the isolated worker")

    async def settled() -> bool:
        row = await obs.job(job_id)
        done = all(r.outcome != "running" for r in d.probe.of(job_id))
        return done and row is not None and row[0] in ("completed", "failed", "cancelled")

    finished = await until(settled, 120, interval=0.5)
    await d.collect(job_id)
    print(ev.dump())
    overlap = find_overlap(d.probe.of(job_id))
    if overlap:
        a, b = overlap
        pytest.fail(
            f"FAIL (worker-only partition): attempt {b.attempt} started on worker {b.worker} "
            f"at t={ev.rel(b.started)}s while attempt {a.attempt} was still in its handler on "
            f"isolated worker {a.worker} (started t={ev.rel(a.started)}s, ended "
            f"t={ev.rel(a.ended)}s, {a.outcome}). Evidence above.",
            pytrace=False,
        )
    if not finished:
        d.inconclusive("the job did not settle before the deadline")


async def test_short_outage_while_reporting_does_not_rerun_the_job(drill: Drill) -> None:
    """Design test 4 (C2): the worker loses PostgreSQL just as the handler
    returns, for 2.5 s -- well inside its authority (no fence, no sweep). The
    result must be recorded once the database is back; the handler must not
    run again."""
    d, ev, obs = drill, drill.evidence, drill.observer
    outage = 2.5

    async def cut_while_returning(worker: str) -> None:
        gate = d.gates[worker]
        await gate.cut()
        ev.mark("postgres cut for the reporting worker", worker=worker, outage_s=outage)

        async def restore() -> None:
            await asyncio.sleep(outage)
            await gate.open()
            ev.mark("postgres restored for the reporting worker", worker=worker)

        asyncio.get_running_loop().create_task(restore())

    d.probe.before_return[1] = cut_while_returning
    job_id = await d.submit(seconds=3, timeout=10)  # lease = 10 s + 3 s grace

    async def settled() -> bool:
        row = await obs.job(job_id)
        done = d.probe.of(job_id) and all(r.outcome != "running" for r in d.probe.of(job_id))
        return bool(done) and row is not None and row[0] in ("completed", "failed")

    finished = await until(settled, 90, interval=0.5)
    # Give a lease-expiry recovery (if one is coming) time to show up.
    await asyncio.sleep(T.tick * 2)
    await until(settled, 60, interval=0.5)
    await d.collect(job_id)
    print(ev.dump())
    runs = d.probe.of(job_id)
    if len(runs) > 1:
        pytest.fail(
            f"FAIL (C2): the job's handler ran {len(runs)} times "
            f"({[(r.attempt, r.worker, r.outcome) for r in runs]}); attempt 1 returned a result "
            f"but it was not recorded, so the job was re-run. Executions: "
            f"{[(e['attempt'], e['status']) for e in ev.executions]}. Evidence above.",
            pytrace=False,
        )
    if not finished or not runs:
        d.inconclusive("the job did not settle before the deadline")
    statuses = [(e["attempt"], e["status"]) for e in ev.executions]
    assert statuses == [(1, "completed")], statuses


async def _first_attempt_running(d: Drill, job_id: str) -> HandlerRun | None:
    async def check() -> HandlerRun | None:
        runs = d.probe.of(job_id)
        row = await d.observer.job(job_id)
        ok = runs and runs[0].outcome == "running" and row and row[0] == "running"
        return runs[0] if ok else None

    return await until(check, 30)


async def _settled(d: Drill, job_id: str, timeout: float) -> bool:
    async def check() -> bool:
        row = await d.observer.job(job_id)
        runs = d.probe.of(job_id)
        done = bool(runs) and all(r.outcome != "running" for r in runs)
        return done and row is not None and row[0] in ("completed", "failed", "cancelled")

    return bool(await until(check, timeout, interval=0.5))


async def test_fenced_attempt_is_handed_back_when_the_database_returns(drill: Drill) -> None:
    """Design test 2: a worker loses PostgreSQL long enough to fence (4 s) and
    gets it back before the scheduler would declare it dead (6 s). It must
    cancel attempt 1, hand it back once it can (lost -> retried), and the job
    must not sit RUNNING until its lease expires."""
    d, ev = drill, drill.evidence
    job_id = await d.submit(seconds=30, timeout=300)
    first = await _first_attempt_running(d, job_id)
    if first is None:
        d.inconclusive("attempt 1 never started")
    assert first is not None
    gate = d.gates[first.worker]
    await gate.cut()
    ev.mark("postgres cut for one worker", worker=first.worker)

    async def cancelled() -> bool:
        return first.outcome == "cancelled"

    if not await until(cancelled, T.ttl * 3):
        d.inconclusive("the worker never fenced")
    ev.mark("attempt 1 handler cancelled by self-fencing")
    await gate.open()
    ev.mark("postgres restored for that worker")

    finished = await _settled(d, job_id, 90)
    await d.collect(job_id)
    print(ev.dump())
    overlap = find_overlap(d.probe.of(job_id))
    assert overlap is None, f"overlap: {overlap}"
    if not finished:
        d.inconclusive("the job did not settle before the deadline")
    statuses = [(e["attempt"], e["status"]) for e in ev.executions]
    assert statuses[0] == (1, "lost") and statuses[-1][1] == "completed", statuses
    reasons = [t["reason"] or "" for t in ev.transitions if t["transition"] == "running -> failed"]
    assert reasons and ("self-fenced" in reasons[0] or "stopped heartbeating" in reasons[0])
    ev.mark("attempt 1 recovered by", how="hand-back" if "self-fenced" in reasons[0] else "sweep")


async def test_outage_shorter_than_the_fence_deadline_changes_nothing(drill: Drill) -> None:
    """Design test 3: a 2 s outage (fence after 4 s) must not fence: attempt 1
    keeps running and completes; no retry."""
    d, ev = drill, drill.evidence
    job_id = await d.submit(seconds=8, timeout=300)
    first = await _first_attempt_running(d, job_id)
    if first is None:
        d.inconclusive("attempt 1 never started")
    assert first is not None
    gate = d.gates[first.worker]
    await gate.cut()
    ev.mark("postgres cut for one worker", worker=first.worker)
    await asyncio.sleep(2)  # the outage itself: deliberately shorter than fence_after
    await gate.open()
    ev.mark("postgres restored")

    finished = await _settled(d, job_id, 60)
    await d.collect(job_id)
    print(ev.dump())
    if not finished:
        d.inconclusive("the job did not settle before the deadline")
    runs = d.probe.of(job_id)
    assert [(r.attempt, r.outcome) for r in runs] == [(1, "returned")]
    assert [(e["attempt"], e["status"]) for e in ev.executions] == [(1, "completed")]
