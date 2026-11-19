"""Job pipeline end to end against real infrastructure:

    FastAPI (real lifespan) -> PostgreSQL (jobs + outbox) -> relay with
    publisher confirms -> RabbitMQ (nexus.jobs, DLX) -> RabbitMQJobWorker
    (registration, heartbeat, inbox, executor) -> PostgreSQL

plus the scheduler's fenced recovery (dead workers, expired leases).
Requires PostgreSQL and RabbitMQ (docker compose up postgres rabbitmq).
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text, update

from nexus.api.security import hash_api_key
from nexus.application.use_cases.jobs import claim_job, report_success
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.coordination.leader_election import PostgresLeaderElection
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.db.models.worker_registration import WorkerRegistrationModel
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.handlers import build_registry
from nexus.infrastructure.execution.job_executor import JobExecutor
from nexus.infrastructure.execution.maintenance import (
    MAINTENANCE_LEASE,
    _recover_jobs_fenced,
    run_maintenance_pass,
)
from nexus.infrastructure.messaging.rabbitmq import create_rabbitmq_connection
from nexus.infrastructure.messaging.rabbitmq_topology import (
    DEAD_LETTER_QUEUE,
    EVENTS_EXCHANGE,
    JOB_DISPATCH_ROUTING_KEY,
    JOBS_QUEUE,
    TASKS_QUEUE,
    declare_topology,
)
from nexus.infrastructure.messaging.rabbitmq_worker import RabbitMQJobWorker, WorkerConfig
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.main import create_app

pytestmark = pytest.mark.integration

KEY_A, KEY_B, CRON = "job-key-a", "job-key-b", "cron-jobs"
AUTH_A = {"Authorization": f"Bearer {KEY_A}"}
AUTH_B = {"Authorization": f"Bearer {KEY_B}"}
FAST_RETRY = {"max_attempts": 3, "initial_backoff_seconds": 0.2, "max_backoff_seconds": 1}


@dataclass
class Stack:
    client: AsyncClient
    app: Any
    settings: Any
    session_factory: Any
    connection: aio_pika.abc.AbstractRobustConnection
    worker: RabbitMQJobWorker

    async def create(self, body: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        return await self.client.post("/api/v1/jobs", json=body, headers=headers or AUTH_A)

    async def detail(self, job_id: str) -> dict[str, Any]:
        r = await self.client.get(f"/api/v1/jobs/{job_id}", headers=AUTH_A)
        assert r.status_code == 200, r.text
        body: dict[str, Any] = r.json()
        return body

    async def wait(self, job_id: str, *states: str, timeout: float = 20) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            d = await self.detail(job_id)
            if d["job"]["state"] in states:
                return d
            if loop.time() > deadline:
                raise AssertionError(f"job stuck in {d['job']['state']}: {d}")
            await asyncio.sleep(0.1)

    async def queue_count(self, name: str) -> int:
        channel = await self.connection.channel()
        try:
            q = await channel.declare_queue(name, passive=True)
            return int(q.declaration_result.message_count or 0)
        finally:
            await channel.close()


async def _reset_infrastructure(session_factory: Any, connection: Any) -> None:
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
        queue = await channel.declare_queue(name, passive=True)
        await queue.purge()
    await channel.close()


@pytest.fixture
async def stack(monkeypatch) -> AsyncIterator[Stack]:
    monkeypatch.setenv("API_KEYS", f"acme:{hash_api_key(KEY_A)},globex:{hash_api_key(KEY_B)}")
    monkeypatch.setenv("CRON_SECRET", CRON)
    monkeypatch.setenv("EVENT_BUS", "rabbitmq")
    monkeypatch.setenv("REDIS_URL", "")  # in-process limiter unless a test opts in
    monkeypatch.setenv("TASK_ENABLE_TEST_HANDLERS", "true")
    monkeypatch.setenv("TASK_LEASE_GRACE_SECONDS", "1")
    monkeypatch.setenv("WORKER_HEARTBEAT_TTL_SECONDS", "2")
    monkeypatch.setenv("RATE_LIMIT_WRITES_PER_MINUTE", "1000")
    get_settings.cache_clear()
    settings = get_settings()

    engine = create_engine(settings.database_url, settings)
    session_factory = create_session_factory(engine)
    connection = await create_rabbitmq_connection(settings.rabbitmq_url)
    await _reset_infrastructure(session_factory, connection)

    worker = RabbitMQJobWorker(
        connection=connection,
        session_factory=session_factory,
        executor=JobExecutor(
            session_factory,
            build_registry(enable_test_handlers=True, webhook_allowed_hosts=frozenset()),
            worker_id=uuid4(),
            lease_grace=timedelta(seconds=1),
        ),
        task_executor=None,
        config=WorkerConfig(capacity=4, heartbeat_interval=0.5, shutdown_grace=0.5),
    )
    app = create_app()
    async with app.router.lifespan_context(app):
        publisher = app.state.container.publisher

        async def relay_loop() -> None:  # what the scheduler daemon does every second
            while True:
                await relay_pending_events(session_factory, publisher, settings, max_events=50)
                await asyncio.sleep(0.1)

        relay = asyncio.create_task(relay_loop())
        await worker.start()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://nexus.test") as client:
            yield Stack(client, app, settings, session_factory, connection, worker)
        await worker.stop()
        relay.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay
    await connection.close()
    await engine.dispose()
    get_settings.cache_clear()


def _states(detail: dict[str, Any]) -> list[tuple[str, str]]:
    return [(t["from_state"], t["to_state"]) for t in detail["transitions"]]


async def test_job_is_persisted_dispatched_executed_and_reported(stack: Stack):
    r = await stack.create(
        {"name": "hello", "job_type": "echo", "payload": {"x": 1}, "priority": "high"},
        headers={**AUTH_A, "X-Correlation-ID": "corr-123"},
    )
    assert r.status_code == 201, r.text
    assert r.headers["X-Correlation-ID"] == "corr-123"
    job_id = r.json()["job_id"]

    d = await stack.wait(job_id, "completed")
    assert d["job"]["result"] == {"echo": {"x": 1}, "attempt": 1}
    assert d["job"]["correlation_id"] == "corr-123"
    assert _states(d) == [("pending", "queued"), ("queued", "running"), ("running", "completed")]
    assert d["transitions"][0]["reason"] == "dispatch confirmed by broker"
    (execution,) = d["executions"]
    assert execution["status"] == "completed"
    assert execution["worker_id"] == str(stack.worker.worker_id)
    assert execution["duration_ms"] is not None


async def test_failed_attempts_retry_with_backoff_then_complete(stack: Stack):
    r = await stack.create(
        {
            "name": "flaky",
            "job_type": "test.flaky",
            "payload": {"succeed_on_attempt": 3},
            **FAST_RETRY,
        }
    )
    d = await stack.wait(r.json()["job_id"], "completed")
    assert d["job"]["attempt"] == 3
    assert _states(d) == [
        ("pending", "queued"),
        ("queued", "running"),
        ("running", "failed"),
        ("failed", "retrying"),
        ("retrying", "queued"),
        ("queued", "running"),
        ("running", "failed"),
        ("failed", "retrying"),
        ("retrying", "queued"),
        ("queued", "running"),
        ("running", "completed"),
    ]
    assert [e["status"] for e in sorted(d["executions"], key=lambda e: e["attempt"])] == [
        "failed",
        "failed",
        "completed",
    ]


async def test_exhausted_job_is_dead_lettered_and_can_be_retried_manually(stack: Stack):
    r = await stack.create(
        {
            "name": "needs-a-retry",
            "job_type": "test.flaky",
            "payload": {"succeed_on_attempt": 3},
            "max_attempts": 2,
            "initial_backoff_seconds": 0.1,
            "max_backoff_seconds": 0.2,
        }
    )
    job_id = r.json()["job_id"]
    failed = await stack.wait(job_id, "failed")
    assert failed["job"]["attempt"] == 2
    assert failed["job"]["dead_lettered_at"] is not None
    assert failed["job"]["can_retry"] is True
    await asyncio.sleep(0.3)
    assert await stack.queue_count(DEAD_LETTER_QUEUE) == 1  # broker-side DLQ

    retried = await stack.client.post(f"/api/v1/jobs/{job_id}/retry", headers=AUTH_A)
    assert retried.status_code == 200, retried.text
    done = await stack.wait(job_id, "completed")
    assert done["job"]["attempt"] == 3
    assert ("failed", "retrying") in _states(done)

    again = await stack.client.post(f"/api/v1/jobs/{job_id}/retry", headers=AUTH_A)
    assert again.status_code == 409


async def test_timeouts_fail_the_attempt(stack: Stack):
    r = await stack.create(
        {
            "name": "slow",
            "job_type": "test.sleep",
            "payload": {"seconds": 5},
            "timeout_seconds": 1,
            "max_attempts": 1,
        }
    )
    d = await stack.wait(r.json()["job_id"], "failed")
    assert "timed out after 1s" in d["job"]["last_error"]
    assert d["executions"][0]["status"] == "timed_out"


async def test_idempotent_submission(stack: Stack):
    headers = {**AUTH_A, "Idempotency-Key": "deploy-42"}
    body = {"name": "deploy", "job_type": "echo"}
    first = await stack.create(body, headers)
    second = await stack.create(body, headers)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["job_id"] == second.json()["job_id"]
    d = await stack.wait(first.json()["job_id"], "completed")
    assert len(d["executions"]) == 1


async def test_job_is_not_queued_until_the_broker_accepts_it(stack: Stack):
    # Warm up: the publisher declares (and so re-binds) the topology on its
    # first publish; unbind only after that.
    warm = (await stack.create({"name": "warm-up", "job_type": "echo"})).json()["job_id"]
    await stack.wait(warm, "completed")
    channel = await stack.connection.channel()
    exchange = await channel.get_exchange(EVENTS_EXCHANGE)
    queue = await channel.declare_queue(JOBS_QUEUE, passive=True)
    await queue.unbind(exchange, routing_key=JOB_DISPATCH_ROUTING_KEY)
    try:
        r = await stack.create({"name": "unroutable", "job_type": "echo"})
        job_id = r.json()["job_id"]
        await asyncio.sleep(0.5)
        d = await stack.detail(job_id)
        # Mandatory publish was returned by the broker: never marked QUEUED.
        assert d["job"]["state"] == "pending"
        async with stack.session_factory() as session:
            attempts = (
                await session.execute(
                    text("SELECT attempts, last_error FROM outbox_records WHERE aggregate_id=:id"),
                    {"id": job_id},
                )
            ).one()
        assert attempts[0] >= 1
    finally:
        await queue.bind(exchange, routing_key=JOB_DISPATCH_ROUTING_KEY)
        await channel.close()
    # Relay backoff then republishes once the route exists again.
    async with stack.session_factory() as session:
        await session.execute(text("UPDATE outbox_records SET available_at = now()"))
        await session.commit()
    await stack.wait(job_id, "completed")


async def test_cancel_queued_job_is_never_executed(stack: Stack):
    await stack.worker.stop()  # nothing consumes, so the job waits in QUEUED
    r = await stack.create({"name": "to-cancel", "job_type": "echo"})
    job_id = r.json()["job_id"]
    await stack.wait(job_id, "queued")
    c = await stack.client.post(
        f"/api/v1/jobs/{job_id}/cancel", headers=AUTH_A, json={"reason": "not needed"}
    )
    assert c.status_code == 200 and c.json()["state"] == "cancelled"
    assert (
        await stack.client.post(f"/api/v1/jobs/{job_id}/cancel", headers=AUTH_A)
    ).status_code == 409


async def test_dead_worker_jobs_are_recovered_and_stale_worker_is_fenced(stack: Stack):
    await stack.worker.stop()  # keep the real worker out of the way
    r = await stack.create(
        {"name": "orphan", "job_type": "echo", "timeout_seconds": 60, **FAST_RETRY}
    )
    job_id = UUID(r.json()["job_id"])
    await stack.wait(str(job_id), "queued")

    zombie = uuid4()
    async with stack.session_factory() as session:
        registry = PostgresWorkerRegistry(session)
        from nexus.infrastructure.messaging.rabbitmq_worker import local_capabilities

        await registry.register(zombie, local_capabilities(), 1)
        await claim_job(
            PostgresJobStore(session),
            PostgresInboxRepository(session),
            job_id=job_id,
            attempt=1,
            message_id=f"manual-{uuid4()}",
            worker_id=zombie,
            lease_grace=timedelta(seconds=1),
        )
        # The zombie stops heartbeating.
        await session.execute(
            update(WorkerRegistrationModel)
            .where(WorkerRegistrationModel.worker_id == zombie)
            .values(last_heartbeat_at=text("now() - interval '1 minute'"))
        )
        await session.commit()

    report = await run_maintenance_pass(
        stack.session_factory,
        stack.app.state.container.publisher,
        stack.settings,
        replica_id=uuid4(),
        trigger="test",
    )
    assert report.leader and report.jobs.lost_on_dead_workers == 1 and report.dead_workers == 1
    d = await stack.detail(str(job_id))
    assert d["job"]["state"] in ("retrying", "queued") and d["job"]["attempt"] == 2
    assert d["executions"][0]["status"] == "lost"

    # The zombie wakes up and reports success for attempt 1: rejected.
    async with stack.session_factory() as session:
        stale = await report_success(
            PostgresJobStore(session), job_id=job_id, attempt=1, worker_id=zombie, result="late"
        )
        await session.commit()
    assert stale is None

    await stack.worker.start()
    done = await stack.wait(str(job_id), "completed")
    assert done["job"]["attempt"] == 2 and done["job"]["result"] != "late"


async def test_superseded_scheduler_is_fenced_out(stack: Stack):
    old, new = uuid4(), uuid4()
    async with stack.session_factory() as session:
        status = await PostgresLeaderElection(
            session,
            replica_id=old,
            lease_name=MAINTENANCE_LEASE,
            lease_duration=timedelta(milliseconds=50),
        ).try_acquire_or_renew()
        await session.commit()
    await asyncio.sleep(0.2)
    async with stack.session_factory() as session:
        taken = await PostgresLeaderElection(
            session, replica_id=new, lease_name=MAINTENANCE_LEASE, lease_duration=timedelta(60)
        ).try_acquire_or_renew()
        await session.commit()
    assert taken.is_leader and taken.fencing_token > status.fencing_token

    _, _, fenced_out = await _recover_jobs_fenced(
        stack.session_factory, stack.settings, replica_id=old, fencing_token=status.fencing_token
    )
    assert fenced_out is True


async def test_graceful_shutdown_hands_unfinished_job_back(stack: Stack):
    r = await stack.create(
        {"name": "long", "job_type": "test.sleep", "payload": {"seconds": 10}, **FAST_RETRY}
    )
    job_id = r.json()["job_id"]
    await stack.wait(job_id, "running")
    await stack.worker.stop()  # grace 0.5s < 10s job
    d = await stack.detail(job_id)
    assert d["job"]["state"] in ("retrying", "queued")
    assert d["executions"][0]["status"] == "lost"
    async with stack.session_factory() as session:
        workers = await PostgresWorkerRegistry(session).list_workers()
    assert stack.worker.worker_id not in {w.worker_id for w in workers}


async def test_poison_message_goes_to_dead_letter_queue(stack: Stack):
    channel = await stack.connection.channel()
    exchange = await channel.get_exchange(EVENTS_EXCHANGE)
    await exchange.publish(
        aio_pika.Message(body=b"not json", message_id="poison-1"),
        routing_key=JOB_DISPATCH_ROUTING_KEY,
    )
    await channel.close()
    for _ in range(30):
        if await stack.queue_count(DEAD_LETTER_QUEUE) == 1:
            break
        await asyncio.sleep(0.1)
    assert await stack.queue_count(DEAD_LETTER_QUEUE) == 1


async def test_validation_auth_and_tenant_isolation(stack: Stack):
    assert (await stack.client.get("/api/v1/jobs")).status_code == 401
    bad = await stack.create({"name": "", "job_type": "echo"})
    assert bad.status_code == 422
    unknown = await stack.create({"name": "x", "job_type": "rm-rf"})
    assert unknown.status_code == 422
    assert unknown.json()["detail"]["code"] == "job_rejected"

    job_id = (await stack.create({"name": "mine", "job_type": "echo"})).json()["job_id"]
    assert (await stack.client.get(f"/api/v1/jobs/{job_id}", headers=AUTH_B)).status_code == 404
    assert (
        await stack.client.post(f"/api/v1/jobs/{job_id}/cancel", headers=AUTH_B)
    ).status_code == 404
    listing = await stack.client.get("/api/v1/jobs", headers=AUTH_B)
    assert listing.json()["total"] == 0


async def test_listing_search_filter_and_pagination(stack: Stack):
    for i in range(5):
        await stack.create(
            {"name": f"report-{i}", "job_type": "echo", "priority": "critical" if i % 2 else "low"}
        )
    await stack.create({"name": "backup", "job_type": "echo"})
    page = (await stack.client.get("/api/v1/jobs?page_size=2&q=report", headers=AUTH_A)).json()
    assert (page["total"], page["pages"], len(page["items"])) == (5, 3, 2)
    critical = (await stack.client.get("/api/v1/jobs?priority=critical", headers=AUTH_A)).json()
    assert critical["total"] == 2
    await asyncio.sleep(1)
    done = (
        await stack.client.get("/api/v1/jobs?status=completed&status=failed", headers=AUTH_A)
    ).json()
    assert all(j["state"] in ("completed", "failed") for j in done["items"])


async def test_dashboard_read_models(stack: Stack):
    job_id = (await stack.create({"name": "metrics", "job_type": "echo"})).json()["job_id"]
    await stack.wait(job_id, "completed")

    workers = (await stack.client.get("/api/v1/workers", headers=AUTH_A)).json()
    me = next(w for w in workers if w["worker_id"] == str(stack.worker.worker_id))
    assert me["status"] == "healthy" and me["capacity_slots"] == 4

    summary = (await stack.client.get("/api/v1/metrics/summary", headers=AUTH_A)).json()
    assert summary["total_jobs"] >= 1 and summary["completed"] >= 1
    assert summary["workers"]["healthy"] >= 1
    assert summary["broker"]["available"] is True
    assert summary["recent_activity"][0]["job_name"] == "metrics"
    assert sum(p["completed"] for p in summary["trend"]) >= 1

    queue = (await stack.client.get("/api/v1/queue", headers=AUTH_A)).json()
    assert queue["broker"]["available"] is True and "items" in queue

    executions = (await stack.client.get("/api/v1/executions", headers=AUTH_A)).json()
    assert executions["total"] >= 1

    system = (await stack.client.get("/api/v1/system", headers=AUTH_A)).json()
    assert system["checks"]["postgres"] == "ok" and system["checks"]["rabbitmq"] == "ok"
    assert "echo" in system["job_types"]


async def test_rate_limiting_in_process(stack: Stack, monkeypatch):
    import nexus.api.rate_limit as rl

    monkeypatch.setattr(rl, "_local", rl.InMemoryRateLimiter())
    stack.settings.rate_limit_writes_per_minute = 3
    codes = [
        (await stack.create({"name": f"r{i}", "job_type": "echo"}, AUTH_B)).status_code
        for i in range(5)
    ]
    assert codes.count(201) == 3 and codes[-1] == 429


async def test_rate_limiting_shared_through_redis(stack: Stack):
    from redis.asyncio import ConnectionPool, Redis

    pool = ConnectionPool.from_url("redis://localhost:6379/15")
    await Redis(connection_pool=pool).flushdb()
    stack.app.state.container.redis_pool = pool
    stack.settings.rate_limit_reads_per_minute = 2
    try:
        codes = [
            (await stack.client.get("/api/v1/jobs", headers=AUTH_A)).status_code for _ in range(3)
        ]
        assert codes == [200, 200, 429]
        keys = await Redis(connection_pool=pool).keys("rl:read:acme:*")
        assert len(keys) == 1
    finally:
        stack.app.state.container.redis_pool = None
        await pool.disconnect()


async def test_fleet_metrics_come_from_durable_state(stack: Stack):
    from prometheus_client.parser import text_string_to_metric_families
    from pydantic import SecretStr

    stack.settings.metrics_token = SecretStr("metrics-secret")
    ok = (await stack.create({"name": "m-ok", "job_type": "echo"})).json()["job_id"]
    dead = (
        await stack.create(
            {
                "name": "m-dead",
                "job_type": "test.fail",
                "payload": {"message": "boom"},
                "max_attempts": 2,
                "initial_backoff_seconds": 0.1,
                "max_backoff_seconds": 0.1,
            }
        )
    ).json()["job_id"]
    await stack.wait(ok, "completed")
    await stack.wait(dead, "failed")
    await asyncio.sleep(0.3)

    assert (await stack.client.get("/api/v1/internal/metrics")).status_code == 401
    r = await stack.client.get(
        "/api/v1/internal/metrics", headers={"Authorization": "Bearer metrics-secret"}
    )
    assert r.status_code == 200
    samples = {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for family in text_string_to_metric_families(r.text)
        for s in family.samples
    }

    def value(name: str, **labels: str) -> float:
        return samples[(name, tuple(sorted(labels.items())))]

    assert value("nexus_jobs", tenant="acme", state="completed") == 1
    assert value("nexus_jobs", tenant="acme", state="failed") == 1
    assert value("nexus_jobs", tenant="acme", state="running") == 0
    assert value("nexus_jobs_dead_lettered", tenant="acme") == 1
    assert value("nexus_job_executions_total", job_type="echo", status="completed") == 1
    assert value("nexus_job_executions_total", job_type="test.fail", status="failed") == 2
    assert value("nexus_job_execution_duration_seconds_count", job_type="test.fail") == 2
    assert value("nexus_outbox_due") == 0 and value("nexus_outbox_lag_seconds") == 0
    assert value("nexus_workers", status="healthy") == 1
    assert value("nexus_worker_capacity_slots") >= 1
    assert value("nexus_state_database_up") == 1
    assert value("nexus_broker_up") == 1
    assert value("nexus_broker_dead_letter_messages") == 1
    assert value("nexus_broker_consumers") >= 1


async def _dead(
    stack: Stack, name: str, message: str, headers: dict[str, str] | None = None
) -> str:
    r = await stack.create(
        {
            "name": name,
            "job_type": "test.fail",
            "payload": {"message": message},
            "max_attempts": 1,
        },
        headers,
    )
    job_id: str = r.json()["job_id"]
    for _ in range(200):
        d = await stack.client.get(f"/api/v1/jobs/{job_id}", headers=headers or AUTH_A)
        if d.json()["job"]["state"] == "failed":
            return job_id
        await asyncio.sleep(0.1)
    raise AssertionError(f"{name} did not fail")


async def test_error_filter_and_bulk_retry(stack: Stack):
    disk = [await _dead(stack, f"disk-{i}", "disk full on /var") for i in range(3)]
    quota = await _dead(stack, "quota", "quota 100% used")
    other_tenant = await _dead(stack, "b", "disk full on /var", AUTH_B)
    done = (await stack.create({"name": "ok", "job_type": "echo"})).json()["job_id"]
    await stack.wait(done, "completed")

    listed = (
        await stack.client.get(
            "/api/v1/jobs", params={"dead_lettered": "true", "error": "DISK FULL"}, headers=AUTH_A
        )
    ).json()
    assert {j["job_id"] for j in listed["items"]} == set(disk)
    # LIKE wildcards in the filter are matched literally.
    pct = (await stack.client.get("/api/v1/jobs", params={"error": "100%"}, headers=AUTH_A)).json()
    assert [j["job_id"] for j in pct["items"]] == [quota]
    assert (await stack.client.get("/api/v1/jobs", params={"error": "1_0"}, headers=AUTH_A)).json()[
        "total"
    ] == 0

    # Explicit ids: one retried, the rest reported, nothing else touched.
    r = await stack.client.post(
        "/api/v1/jobs/bulk-retry",
        json={"job_ids": [quota, done, other_tenant, str(uuid4()), quota]},
        headers=AUTH_A,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert [j["job_id"] for j in body["retried"]] == [quota]
    assert {(s["job_id"], s["code"]) for s in body["skipped"]} >= {
        (done, "not_retryable"),
        (other_tenant, "not_found"),
    }
    assert body["requested"] == 5

    # By filter: every dead-lettered "disk full" job of this tenant only.
    r = await stack.client.post(
        "/api/v1/jobs/bulk-retry",
        json={"dead_lettered": True, "error": "disk full"},
        headers=AUTH_A,
    )
    assert r.status_code == 200, r.text
    assert {j["job_id"] for j in r.json()["retried"]} == set(disk)
    for job_id in disk:
        detail = await stack.wait(job_id, "failed")  # test.fail fails again ...
        assert detail["job"]["attempt"] == 2  # ... but it really ran a second time
    untouched = await stack.client.get(f"/api/v1/jobs/{other_tenant}", headers=AUTH_B)
    assert untouched.json()["job"]["attempt"] == 1  # the other tenant's job is not retried

    bad = await stack.client.post(
        "/api/v1/jobs/bulk-retry", json={"job_ids": [quota], "dead_lettered": True}, headers=AUTH_A
    )
    assert bad.status_code == 422
    assert (
        await stack.client.post("/api/v1/jobs/bulk-retry", json={}, headers=AUTH_A)
    ).status_code == 422


async def test_prune_removes_only_resolved_dead_letters(stack: Stack):
    from nexus.infrastructure.messaging.dead_letter import prune_resolved_dead_letters

    resolved = (
        await stack.create(
            {
                "name": "recovers",
                "job_type": "test.flaky",
                "payload": {"succeed_on_attempt": 2},
                "max_attempts": 1,
            }
        )
    ).json()["job_id"]
    await stack.wait(resolved, "failed")
    still_dead = await _dead(stack, "stays-dead", "permanent")
    channel = await stack.connection.channel()
    exchange = await channel.get_exchange(EVENTS_EXCHANGE)
    await exchange.publish(  # poison: cannot be attributed to a job, must be kept
        aio_pika.Message(body=b"not json", message_id="poison-2"),
        routing_key=JOB_DISPATCH_ROUTING_KEY,
    )
    await channel.close()
    for _ in range(50):
        if await stack.queue_count(DEAD_LETTER_QUEUE) == 3:
            break
        await asyncio.sleep(0.1)
    assert await stack.queue_count(DEAD_LETTER_QUEUE) == 3

    retried = await stack.client.post(f"/api/v1/jobs/{resolved}/retry", headers=AUTH_A)
    assert retried.status_code == 200
    await stack.wait(resolved, "completed")

    report = await prune_resolved_dead_letters(stack.connection, stack.session_factory)
    assert (report.inspected, report.pruned, report.kept) == (3, 1, 2)
    assert await stack.queue_count(DEAD_LETTER_QUEUE) == 2
    again = await prune_resolved_dead_letters(stack.connection, stack.session_factory)
    assert (again.pruned, again.kept) == (0, 2)  # idempotent; kept messages are still there

    # The operator endpoint does the same, behind CRON_SECRET.
    assert (await stack.client.post("/api/v1/internal/dead-letter/prune")).status_code == 401
    r = await stack.client.post(
        "/api/v1/internal/dead-letter/prune", headers={"Authorization": f"Bearer {CRON}"}
    )
    assert r.status_code == 200 and r.json()["pruned"] == 0
    assert (await stack.detail(still_dead))["job"]["dead_lettered_at"] is not None
