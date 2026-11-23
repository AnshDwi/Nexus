"""The leader's dead-worker sweep waits until it has watched heartbeats, with
working database access, for a full TTL (C1: no false "dead" verdicts right
after a PostgreSQL outage, a failed pass or a leadership change).

Runs the real maintenance pass against PostgreSQL. The observation window's
clock is a fake so a 30 s TTL is stepped through instantly; staleness of a
worker is set in the database (last_heartbeat_at in the past), and the
leader lease is expired in the database to force leadership changes.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from nexus.application.use_cases.jobs import claim_job
from nexus.domain.job import Job, JobPriority, JobState
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.coordination.observation_window import HeartbeatObservationWindow
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.maintenance import MAINTENANCE_LEASE, run_maintenance_pass
from nexus.infrastructure.messaging.rabbitmq_worker import local_capabilities

pytestmark = pytest.mark.integration

TTL = 30.0
LEASE = 15.0
TICK = 5.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 10_000.0

    def __call__(self) -> float:
        return self.now


@dataclass
class Env:
    settings: Any
    sf: Any
    down_sf: Any  # a session factory whose database is unreachable
    clock: FakeClock

    def window(self) -> HeartbeatObservationWindow:
        return HeartbeatObservationWindow(ttl_seconds=TTL, lease_seconds=LEASE, clock=self.clock)

    async def run_pass(
        self, replica: UUID, window: HeartbeatObservationWindow | None, *, down: bool = False
    ) -> Any:
        return await run_maintenance_pass(
            self.down_sf if down else self.sf,
            None,
            self.settings,
            replica_id=replica,
            trigger="test",
            observation=window,
        )

    async def steady_passes(
        self, replica: UUID, window: HeartbeatObservationWindow, seconds: float
    ) -> list[Any]:
        """Leader passes every TICK (fake) seconds; returns their reports."""
        reports = []
        elapsed = 0.0
        while elapsed < seconds:
            self.clock.now += TICK
            elapsed += TICK
            reports.append(await self.run_pass(replica, window))
        return reports

    async def sql(self, statement: str, **params: Any) -> list[Any]:
        async with self.sf() as session:
            result = await session.execute(text(statement), params)
            rows = list(result.all()) if result.returns_rows else []
            await session.commit()
            return rows

    async def worker(self, *, stale: bool) -> UUID:
        worker_id = uuid4()
        async with self.sf() as session:
            await PostgresWorkerRegistry(session).register(worker_id, local_capabilities(), 2)
            await session.commit()
        if stale:
            await self.make_stale(worker_id)
        return worker_id

    async def make_stale(self, worker_id: UUID) -> None:
        await self.sql(
            "UPDATE worker_registrations SET last_heartbeat_at = now() - interval '5 minutes' "
            "WHERE worker_id = :w",
            w=worker_id,
        )

    async def running_job(self, worker_id: UUID) -> UUID:
        now = datetime.now(UTC)
        job = Job(
            job_id=uuid4(),
            tenant_id="acme",
            name="observed",
            job_type="echo",
            payload={},
            priority=JobPriority.NORMAL,
            state=JobState.QUEUED,
            attempt=1,
            max_attempts=3,
            retry_limit=3,
            initial_backoff_seconds=0,
            max_backoff_seconds=0,
            timeout_seconds=600,  # lease expiry stays out of the way
            created_at=now,
            updated_at=now,
        )
        async with self.sf() as session:
            store = PostgresJobStore(session)
            await store.insert_or_get_existing(job)
            decision, _ = await claim_job(
                store,
                PostgresInboxRepository(session),
                job_id=job.job_id,
                attempt=1,
                message_id=f"test-{uuid4()}",
                worker_id=worker_id,
                lease_grace=timedelta(seconds=30),
            )
            await session.commit()
        assert decision.value == "run"
        return job.job_id

    async def job(self, job_id: UUID) -> tuple[str, int]:
        rows = await self.sql("SELECT state, attempt FROM jobs WHERE job_id = :j", j=job_id)
        return rows[0][0], rows[0][1]

    async def registered(self, worker_id: UUID) -> bool:
        rows = await self.sql(
            "SELECT 1 FROM worker_registrations WHERE worker_id = :w", w=worker_id
        )
        return bool(rows)

    async def expire_leader_lease(self) -> None:
        await self.sql(
            "UPDATE scheduler_leader_lease SET expires_at = now() - interval '1 second' "
            "WHERE lease_name = :n",
            n=MAINTENANCE_LEASE,
        )


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Env]:
    monkeypatch.setenv("WORKER_HEARTBEAT_TTL_SECONDS", str(TTL))
    monkeypatch.setenv("SCHEDULER_LEASE_DURATION_SECONDS", str(LEASE))
    monkeypatch.setenv("JOB_REDISPATCH_AFTER_SECONDS", "900")
    monkeypatch.setenv("EVENT_BUS", "none")
    get_settings.cache_clear()
    settings = get_settings()
    engine = create_engine(settings.database_url, settings)
    sf = create_session_factory(engine)
    down_engine = create_engine(
        "postgresql+asyncpg://nexus:nexus@127.0.0.1:1/unreachable",
        settings.model_copy(update={"db_pool_timeout_seconds": 2.0}),
    )
    e = Env(settings, sf, create_session_factory(down_engine), FakeClock())
    await e.sql(
        "TRUNCATE job_executions, job_transitions, jobs, inbox_records, outbox_records, "
        "worker_registrations, scheduler_leader_lease RESTART IDENTITY CASCADE"
    )
    yield e
    await engine.dispose()
    await down_engine.dispose()
    get_settings.cache_clear()


async def test_dead_worker_recovery_waits_for_a_full_ttl_then_recovers(env: Env):
    """Delayed sweep, then normal dead-worker recovery once the window spans the TTL."""
    replica, window = uuid4(), env.window()
    dead = await env.worker(stale=True)
    job_id = await env.running_job(dead)
    await env.make_stale(dead)  # claim does not heartbeat, but be explicit

    first = await env.run_pass(replica, window)
    assert first.leader and first.dead_worker_sweep_deferred
    assert first.dead_workers == 0 and first.jobs.lost_on_dead_workers == 0
    for report in await env.steady_passes(replica, window, TTL - TICK):
        assert report.dead_worker_sweep_deferred, "swept before a full TTL was observed"
    assert await env.job(job_id) == ("running", 1)
    assert await env.registered(dead)

    [report] = await env.steady_passes(replica, window, TICK)
    assert not report.dead_worker_sweep_deferred
    assert report.dead_workers == 1 and report.jobs.lost_on_dead_workers == 1
    assert await env.job(job_id) == ("retrying", 2)
    rows = await env.sql(
        "SELECT attempt, status FROM job_executions WHERE job_id = :j ORDER BY attempt", j=job_id
    )
    assert [tuple(r) for r in rows] == [(1, "lost")]


async def test_scheduler_reconnecting_after_an_outage_defers_the_sweep(env: Env):
    """The window is open, PostgreSQL goes away long enough for every
    heartbeat to go stale, then comes back: the first passes must not treat
    the stale workers as dead; a worker that stays silent is recovered a TTL
    later."""
    replica, window = uuid4(), env.window()
    worker = await env.worker(stale=False)
    job_id = await env.running_job(worker)
    await env.run_pass(replica, window)
    reports = await env.steady_passes(replica, window, TTL)
    assert not reports[-1].dead_worker_sweep_deferred  # steady state: sweeping

    # Outage: passes fail; meanwhile the worker's heartbeat ages past the TTL.
    for _ in range(3):
        env.clock.now += TICK
        with pytest.raises(OSError):
            await env.run_pass(replica, window, down=True)
    assert window.last_reset_reason == "maintenance pass failed"
    await env.make_stale(worker)
    await env.expire_leader_lease()  # the lease ran out during the outage too

    env.clock.now += TICK
    back = await env.run_pass(replica, window)
    assert back.leader and back.dead_worker_sweep_deferred
    assert back.jobs.lost_on_dead_workers == 0
    assert await env.job(job_id) == ("running", 1)

    # A live worker would have heartbeated by now. This one stays silent:
    # it is declared dead once the window spans the TTL, not before.
    reports = await env.steady_passes(replica, window, TTL)
    assert all(r.dead_worker_sweep_deferred for r in reports[:-1])
    assert reports[-1].jobs.lost_on_dead_workers == 1
    assert await env.job(job_id) == ("retrying", 2)


async def test_a_live_worker_that_heartbeats_again_is_not_recovered(env: Env):
    """The point of the window: a worker whose heartbeats were only blocked by
    the outage refreshes them within the TTL and keeps its job."""
    replica, window = uuid4(), env.window()
    worker = await env.worker(stale=True)  # stale: PostgreSQL just came back
    job_id = await env.running_job(worker)
    await env.make_stale(worker)

    await env.run_pass(replica, window)
    await env.steady_passes(replica, window, TICK)
    async with env.sf() as session:  # the worker's next heartbeat succeeds
        assert await PostgresWorkerRegistry(session).heartbeat(worker) is not None
        await session.commit()
    reports = await env.steady_passes(replica, window, TTL)
    assert not reports[-1].dead_worker_sweep_deferred
    assert all(r.jobs.lost_on_dead_workers == 0 for r in reports)
    assert await env.job(job_id) == ("running", 1)


async def test_another_failure_restarts_the_window(env: Env):
    replica, window = uuid4(), env.window()
    dead = await env.worker(stale=True)
    job_id = await env.running_job(dead)
    await env.make_stale(dead)

    await env.run_pass(replica, window)
    await env.steady_passes(replica, window, TTL - 2 * TICK)  # almost open
    env.clock.now += TICK
    with pytest.raises(OSError):
        await env.run_pass(replica, window, down=True)

    # Without the reset, the next pass would be past the TTL and sweep.
    env.clock.now += TICK
    after = await env.run_pass(replica, window)
    assert after.dead_worker_sweep_deferred
    reports = await env.steady_passes(replica, window, TTL - TICK)
    assert all(r.dead_worker_sweep_deferred for r in reports)
    assert await env.job(job_id) == ("running", 1)
    [report] = await env.steady_passes(replica, window, TICK)
    assert report.jobs.lost_on_dead_workers == 1


async def test_reacquiring_leadership_restarts_the_window(env: Env):
    """R leads (window open), loses the lease to X, then takes it back: R must
    observe a full TTL again before sweeping."""
    r, x = uuid4(), uuid4()
    window_r = env.window()
    await env.run_pass(r, window_r)
    reports = await env.steady_passes(r, window_r, TTL)
    assert not reports[-1].dead_worker_sweep_deferred

    await env.expire_leader_lease()
    taken = await env.run_pass(x, None)  # X takes over
    assert taken.leader
    env.clock.now += TICK
    lost = await env.run_pass(r, window_r)
    assert not lost.leader
    assert window_r.last_reset_reason == "not the leader"

    dead = await env.worker(stale=True)
    job_id = await env.running_job(dead)
    await env.make_stale(dead)
    await env.expire_leader_lease()
    env.clock.now += TICK
    regained = await env.run_pass(r, window_r)
    assert regained.leader and regained.dead_worker_sweep_deferred
    assert window_r.last_reset_reason == "leadership acquired"
    assert await env.job(job_id) == ("running", 1)
    reports = await env.steady_passes(r, window_r, TTL)
    assert reports[-1].jobs.lost_on_dead_workers == 1


async def test_renewing_an_own_lapsed_lease_restarts_the_window(env: Env):
    """No other replica took over, but R's lease lapsed (e.g. R could not
    reach PostgreSQL to renew it): renewing it counts as re-acquiring."""
    replica, window = uuid4(), env.window()
    await env.run_pass(replica, window)
    await env.steady_passes(replica, window, TTL)
    assert window.allows_dead_worker_sweep

    await env.expire_leader_lease()
    env.clock.now += TICK
    report = await env.run_pass(replica, window)
    assert report.leader and report.dead_worker_sweep_deferred
    assert window.last_reset_reason == "leadership acquired"


async def test_expired_leases_are_still_recovered_while_the_sweep_is_deferred(env: Env):
    replica, window = uuid4(), env.window()
    live = await env.worker(stale=False)
    job_id = await env.running_job(live)
    await env.sql(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE job_id = :j",
        j=job_id,
    )
    report = await env.run_pass(replica, window)
    assert report.dead_worker_sweep_deferred
    assert report.jobs.expired_leases == 1
    assert await env.job(job_id) == ("retrying", 2)
    rows = await env.sql("SELECT status FROM job_executions WHERE job_id = :j", j=job_id)
    assert [r[0] for r in rows] == ["timed_out"]


async def test_without_a_window_the_sweep_is_immediate_as_before(env: Env):
    """The stateless cron endpoint passes no window: behaviour unchanged."""
    dead = await env.worker(stale=True)
    job_id = await env.running_job(dead)
    await env.make_stale(dead)
    report = await env.run_pass(uuid4(), None)
    assert not report.dead_worker_sweep_deferred
    assert report.jobs.lost_on_dead_workers == 1
    assert await env.job(job_id) == ("retrying", 2)
