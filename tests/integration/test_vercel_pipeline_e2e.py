"""End-to-end: FastAPI app (real lifespan) -> PostgreSQL -> outbox -> Vercel
Queues -> push subscriber -> executor -> PostgreSQL.

Vercel Queues is provided by the official SDK's embedded queue service
(`vercel.queue.embedded`), which runs the same client, lease, push-dispatch,
acknowledge and retry paths as the hosted service, in-process. The
subscriber exercised is the real `nexus.entrypoints.queue_subscriber`
function that Vercel deploys as the queue consumer.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from vercel.queue.embedded import embedded_queue_service

import nexus.entrypoints.queue_subscriber  # noqa: F401 - registers the @subscribe consumer
from nexus.api.security import hash_api_key
from nexus.application.use_cases.task_lifecycle import claim_task
from nexus.entrypoints.runtime import WorkerRuntime, reset_worker_runtime, set_worker_runtime
from nexus.infrastructure.config import get_settings
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.db.session import create_engine, create_session_factory
from nexus.infrastructure.execution.executor import TaskExecutor
from nexus.infrastructure.execution.handlers import build_registry
from nexus.infrastructure.messaging.factory import PublisherHandle
from nexus.infrastructure.messaging.vercel_queues import VercelQueuesEventPublisher
from nexus.main import create_app

pytestmark = pytest.mark.integration

KEY_A, KEY_B, CRON = "key-tenant-a", "key-tenant-b", "cron-secret"
AUTH_A = {"Authorization": f"Bearer {KEY_A}"}
AUTH_B = {"Authorization": f"Bearer {KEY_B}"}
RETRY = {"max_attempts": 3, "initial_backoff_seconds": 0.2, "max_backoff_seconds": 1}


async def _noop() -> None:
    return None


@dataclass
class Harness:
    client: AsyncClient
    app: Any
    publisher: VercelQueuesEventPublisher
    queue_client: Any

    def disconnect_queue(self) -> None:
        """Simulate the queue being unreachable from the API (or the API
        process dying between commit and publish)."""
        self.app.state.container.publisher = None

    def reconnect_queue(self) -> None:
        self.app.state.container.publisher = self.publisher

    async def submit(self, body: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        return await self.client.post("/api/v1/tasks", json=body, headers=headers or AUTH_A)

    async def task(self, task_id: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        response = await self.client.get(f"/api/v1/tasks/{task_id}", headers=headers or AUTH_A)
        assert response.status_code == 200, response.text
        body: dict[str, Any] = response.json()
        return body

    async def wait_for(self, task_id: str, *states: str, timeout: float = 15) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            task = await self.task(task_id)
            if task["state"] in states:
                return task
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"task stuck in {task['state']}: {task}")
            await asyncio.sleep(0.1)

    async def transitions(self, task_id: str) -> list[tuple[int, str, str]]:
        response = await self.client.get(f"/api/v1/tasks/{task_id}/transitions", headers=AUTH_A)
        return [(t["attempt"], t["from_state"], t["to_state"]) for t in response.json()]

    async def maintenance(self) -> dict[str, Any]:
        response = await self.client.get(
            "/api/v1/internal/maintenance", headers={"Authorization": f"Bearer {CRON}"}
        )
        assert response.status_code == 200, response.text
        body: dict[str, Any] = response.json()
        return body


@asynccontextmanager
async def _queue_service_in_own_task() -> AsyncIterator[Any]:
    """The SDK's embedded service owns an anyio task group, which must be
    exited by the task that entered it; pytest-asyncio tears fixtures down
    from a different task, so the service lives in a dedicated task."""
    ready: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    stop = asyncio.Event()

    async def _host() -> None:
        try:
            async with embedded_queue_service() as service:
                ready.set_result(service)
                await stop.wait()
        except BaseException as exc:
            if not ready.done():
                ready.set_exception(exc)
            raise

    host = asyncio.create_task(_host())
    try:
        yield await ready
    finally:
        stop.set()
        await host


@pytest.fixture
async def nexus(monkeypatch, task_session_factory) -> AsyncIterator[Harness]:
    monkeypatch.setenv("API_KEYS", f"tenant-a:{hash_api_key(KEY_A)},tenant-b:{hash_api_key(KEY_B)}")
    monkeypatch.setenv("CRON_SECRET", CRON)
    monkeypatch.setenv("TASK_ENABLE_TEST_HANDLERS", "true")
    monkeypatch.setenv("TASK_LEASE_GRACE_SECONDS", "0")
    monkeypatch.setenv("REDIS_URL", "")  # independent of a developer .env
    monkeypatch.setenv("EVENT_BUS", "none")  # replaced by the embedded-queue publisher below
    get_settings.cache_clear()
    settings = get_settings()

    async with _queue_service_in_own_task() as service:
        queue_client = service.get_async_client()
        publisher = VercelQueuesEventPublisher(
            queue_client,
            dispatch_topic=settings.vercel_queue_dispatch_topic,
            events_topic=settings.vercel_queue_events_topic,
        )
        engine = create_engine(settings.database_url, settings)
        set_worker_runtime(
            WorkerRuntime(
                settings=settings,
                executor=TaskExecutor(
                    create_session_factory(engine),
                    build_registry(enable_test_handlers=True, webhook_allowed_hosts=frozenset()),
                    publisher,
                    settings,
                    engine=engine,
                ),
                publisher=PublisherHandle(publisher=publisher, close=_noop),
            )
        )
        app = create_app()
        async with app.router.lifespan_context(app):
            app.state.container.publisher = publisher
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://nexus.test") as client:
                yield Harness(
                    client=client, app=app, publisher=publisher, queue_client=queue_client
                )
        await reset_worker_runtime()
    get_settings.cache_clear()


async def test_submitted_task_is_executed_and_result_persisted(nexus: Harness):
    response = await nexus.submit(
        {"task_type": "echo", "payload": {"hello": "vercel"}, "retry_policy": RETRY}
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["task"]["task_id"]

    task = await nexus.wait_for(task_id, "succeeded")
    assert task["result"] == {"echo": {"hello": "vercel"}, "attempt": 1}
    assert [t[1:] for t in await nexus.transitions(task_id)] == [
        ("awaiting_admission", "pending"),
        ("pending", "ready"),
        ("ready", "dispatched"),
        ("dispatched", "running"),
        ("running", "succeeded"),
    ]


async def test_failing_task_is_retried_with_backoff_until_it_succeeds(nexus: Harness):
    response = await nexus.submit(
        {"task_type": "test.flaky", "payload": {"succeed_on_attempt": 3}, "retry_policy": RETRY}
    )
    task_id = response.json()["task"]["task_id"]

    task = await nexus.wait_for(task_id, "succeeded")
    assert task["attempt"] == 3
    assert task["result"] == {"succeeded_on_attempt": 3}
    failures = [t for t in await nexus.transitions(task_id) if t[2] == "failed"]
    assert [f[0] for f in failures] == [1, 2]


async def test_exhausted_retries_fail_permanently_and_cancel_dependents(nexus: Harness):
    root = await nexus.submit(
        {"task_type": "test.fail", "payload": {"message": "nope"}, "retry_policy": RETRY}
    )
    root_id = root.json()["task"]["task_id"]
    child = await nexus.submit(
        {"task_type": "echo", "depends_on": [root_id], "retry_policy": RETRY}
    )
    child_id = child.json()["task"]["task_id"]
    assert child.json()["task"]["state"] == "pending"

    failed = await nexus.wait_for(root_id, "permanently_failed")
    assert failed["attempt"] == 3
    assert "nope" in failed["last_error"]
    cancelled = await nexus.wait_for(child_id, "cancelled")
    assert root_id in cancelled["last_error"]


async def test_permanent_errors_skip_remaining_retries(nexus: Harness):
    response = await nexus.submit(
        {"task_type": "test.fail", "payload": {"permanent": True}, "retry_policy": RETRY}
    )
    task = await nexus.wait_for(response.json()["task"]["task_id"], "permanently_failed")
    assert task["attempt"] == 1


async def test_timeouts_are_enforced(nexus: Harness):
    response = await nexus.submit(
        {
            "task_type": "test.sleep",
            "payload": {"seconds": 5},
            "timeout_seconds": 1,
            "retry_policy": {
                "max_attempts": 1,
                "initial_backoff_seconds": 0,
                "max_backoff_seconds": 0,
            },
        }
    )
    task = await nexus.wait_for(response.json()["task"]["task_id"], "permanently_failed")
    assert "timed out after 1s" in task["last_error"]


async def test_dependencies_run_in_order(nexus: Harness):
    a = (
        await nexus.submit(
            {"task_type": "test.sleep", "payload": {"seconds": 0.3}, "retry_policy": RETRY}
        )
    ).json()
    b = (
        await nexus.submit(
            {"task_type": "echo", "depends_on": [a["task"]["task_id"]], "retry_policy": RETRY}
        )
    ).json()
    c = (
        await nexus.submit(
            {
                "task_type": "echo",
                "depends_on": [a["task"]["task_id"], b["task"]["task_id"]],
                "retry_policy": RETRY,
            }
        )
    ).json()

    done_a = await nexus.wait_for(a["task"]["task_id"], "succeeded")
    done_b = await nexus.wait_for(b["task"]["task_id"], "succeeded")
    done_c = await nexus.wait_for(c["task"]["task_id"], "succeeded")
    assert done_b["started_at"] >= done_a["finished_at"]
    assert done_c["started_at"] >= done_b["finished_at"]


async def test_not_before_delays_execution(nexus: Harness):
    start = datetime.now(UTC)
    response = await nexus.submit(
        {
            "task_type": "echo",
            "not_before": (start + timedelta(seconds=2)).isoformat(),
            "retry_policy": RETRY,
        }
    )
    assert response.json()["task"]["state"] == "pending"
    task = await nexus.wait_for(response.json()["task"]["task_id"], "succeeded")
    assert datetime.fromisoformat(task["started_at"]) >= start + timedelta(seconds=2)


async def test_idempotent_submission_returns_the_original_task(nexus: Harness):
    headers = {**AUTH_A, "Idempotency-Key": "order-42"}
    body = {"task_type": "echo", "payload": {"n": 1}, "retry_policy": RETRY}
    first = await nexus.submit(body, headers)
    second = await nexus.submit(body, headers)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["task"]["task_id"] == second.json()["task"]["task_id"]
    assert second.json()["created"] is False


async def test_admission_rejections_are_persisted_and_explained(nexus: Harness):
    missing_policy = await nexus.submit({"task_type": "echo"})
    assert missing_policy.status_code == 422
    body = missing_policy.json()
    assert body["task"]["state"] == "rejected"
    assert body["admission"]["outcome"] == "rejected"
    assert "retry_policy" in body["admission"]["reason"]

    unknown = await nexus.submit({"task_type": "rm -rf", "retry_policy": RETRY})
    assert unknown.status_code == 422
    assert unknown.json()["admission"]["validation_results"][0]["rule_name"] == "task_type"

    bad_dep = await nexus.submit(
        {"task_type": "echo", "depends_on": [str(uuid4())], "retry_policy": RETRY}
    )
    assert bad_dep.status_code == 422
    assert "unknown task id" in bad_dep.json()["admission"]["reason"]


async def test_authentication_and_tenant_isolation(nexus: Harness):
    assert (await nexus.client.get("/api/v1/tasks")).status_code == 401
    assert (
        await nexus.client.get("/api/v1/tasks", headers={"Authorization": "Bearer x"})
    ).status_code == 401

    task_id = (await nexus.submit({"task_type": "echo", "retry_policy": RETRY})).json()["task"][
        "task_id"
    ]
    other = await nexus.client.get(f"/api/v1/tasks/{task_id}", headers=AUTH_B)
    assert other.status_code == 404
    cancel = await nexus.client.post(f"/api/v1/tasks/{task_id}/cancel", headers=AUTH_B)
    assert cancel.status_code == 404
    # Tenant B cannot depend on tenant A's task either.
    dep = await nexus.submit(
        {"task_type": "echo", "depends_on": [task_id], "retry_policy": RETRY}, AUTH_B
    )
    assert dep.status_code == 422

    listing_b = await nexus.client.get("/api/v1/tasks", headers=AUTH_B)
    assert all(t["tenant_id"] == "tenant-b" for t in listing_b.json()["items"])
    listing_a = await nexus.client.get("/api/v1/tasks", headers=AUTH_A)
    assert task_id in {t["task_id"] for t in listing_a.json()["items"]}


async def test_cancel_pending_task_and_conflict_on_terminal(nexus: Harness):
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    task_id = (
        await nexus.submit({"task_type": "echo", "not_before": later, "retry_policy": RETRY})
    ).json()["task"]["task_id"]

    cancelled = await nexus.client.post(
        f"/api/v1/tasks/{task_id}/cancel", headers=AUTH_A, json={"reason": "changed my mind"}
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "cancelled"
    again = await nexus.client.post(f"/api/v1/tasks/{task_id}/cancel", headers=AUTH_A)
    assert again.status_code == 409


async def test_task_committed_while_queue_unreachable_is_recovered_by_maintenance(nexus: Harness):
    nexus.disconnect_queue()
    task_id = (await nexus.submit({"task_type": "echo", "retry_policy": RETRY})).json()["task"][
        "task_id"
    ]
    await asyncio.sleep(0.5)
    assert (await nexus.task(task_id))["state"] == "ready"  # durable, not lost, not run

    nexus.reconnect_queue()
    report = await nexus.maintenance()
    assert report["leader"] is True
    assert report["relayed"] >= 1
    await nexus.wait_for(task_id, "succeeded")


async def test_worker_crash_mid_execution_is_retried_after_lease_expiry(
    nexus: Harness, task_session_factory
):
    nexus.disconnect_queue()
    task_id = (
        await nexus.submit({"task_type": "echo", "timeout_seconds": 1, "retry_policy": RETRY})
    ).json()["task"]["task_id"]

    # A worker claims attempt 1 and then dies before reporting anything.
    async with task_session_factory() as session:
        from uuid import UUID

        claimed = await claim_task(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            task_id=UUID(task_id),
            attempt=1,
            lease_grace=timedelta(0),
        )
        await session.commit()
    assert claimed.task is not None and claimed.task.state.value == "running"

    await asyncio.sleep(1.2)
    nexus.reconnect_queue()
    report = await nexus.maintenance()
    assert report["expired_leases"] == 1

    task = await nexus.wait_for(task_id, "succeeded")
    assert task["attempt"] == 2
    assert (1, "running", "failed") in await nexus.transitions(task_id)


async def test_duplicate_deliveries_execute_the_task_once(nexus: Harness):
    nexus.disconnect_queue()
    task_id = (await nexus.submit({"task_type": "echo", "retry_policy": RETRY})).json()["task"][
        "task_id"
    ]

    # Three distinct messages for the same attempt (e.g. relay re-publish
    # after a crash plus a reconciler re-dispatch) all hit the subscriber.
    for _ in range(3):
        await nexus.queue_client.send(
            "nexus-task-dispatch", {"task_id": task_id, "attempt": 1, "event_id": str(uuid4())}
        )
    await nexus.wait_for(task_id, "succeeded")
    await asyncio.sleep(1.0)  # let the duplicates be delivered and acknowledged

    running = [t for t in await nexus.transitions(task_id) if t[2] == "running"]
    assert len(running) == 1


async def test_readiness_and_metrics_endpoints(nexus: Harness, monkeypatch):
    ready = await nexus.client.get("/api/v1/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok", "postgres": "ok"}  # Redis/RabbitMQ not configured
    # Metrics are disabled unless METRICS_TOKEN is set.
    assert (await nexus.client.get("/api/v1/internal/metrics")).status_code == 404
