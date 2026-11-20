"""Failure drills against a running production stack (compose.production.yml).

These restart, stop and SIGKILL real containers while real jobs run, then
check that every job still finishes exactly once. They need the test job
types, so set TASK_ENABLE_TEST_HANDLERS=true in .env.production for the run
(and back to false afterwards), then:

    NEXUS_E2E_API_URL=https://nexus.example.com \\
    NEXUS_E2E_API_KEY=nx_... \\
    uv run pytest -m resilience -v        # from the repository root on the server

Destructive: run against a staging stack or before going live. Takes ~6 min.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.resilience

API = os.environ.get("NEXUS_E2E_API_URL", "http://localhost").rstrip("/") + "/api/v1"
KEY = os.environ.get("NEXUS_E2E_API_KEY", "")
# The operations script pins the deployed version, so restarts never switch images.
COMPOSE = shlex.split(os.environ.get("NEXUS_COMPOSE", "deploy/production/nexus.sh compose"))


def compose(*args: str, timeout: float = 300) -> str:
    out = subprocess.run(
        [*COMPOSE, *args], capture_output=True, text=True, timeout=timeout, check=True
    )
    return out.stdout


def client() -> httpx.Client:
    return httpx.Client(base_url=API, headers={"Authorization": f"Bearer {KEY}"}, timeout=15)


def wait_for(predicate: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except httpx.HTTPError:
            pass  # the API itself may be restarting
        time.sleep(1)
    raise AssertionError(f"timed out after {timeout:.0f}s waiting for {what}")


def ready() -> bool:
    with httpx.Client(timeout=5) as c:
        return c.get(f"{API}/ready").status_code == 200


def submit(c: httpx.Client, job_type: str, payload: dict[str, Any], **extra: Any) -> str:
    body = {"name": f"drill-{job_type}", "job_type": job_type, "payload": payload, **extra}
    r = c.post("/jobs", json=body, headers={"Idempotency-Key": str(uuid.uuid4())})
    assert r.status_code == 201, r.text
    return str(r.json()["job_id"])


def detail(c: httpx.Client, job_id: str) -> dict[str, Any]:
    r = c.get(f"/jobs/{job_id}")
    r.raise_for_status()
    body: dict[str, Any] = r.json()
    return body


def wait_states(c: httpx.Client, ids: list[str], states: set[str], timeout: float) -> None:
    wait_for(
        lambda: all(detail(c, i)["job"]["state"] in states for i in ids),
        timeout,
        f"{len(ids)} job(s) to reach {sorted(states)}",
    )


def assert_completed_exactly_once(c: httpx.Client, ids: list[str]) -> None:
    for job_id in ids:
        d = detail(c, job_id)
        assert d["job"]["state"] == "completed", d["job"]
        completed = [e for e in d["executions"] if e["status"] == "completed"]
        assert len(completed) == 1, f"{job_id} completed {len(completed)} times"


@pytest.fixture(scope="module", autouse=True)
def stack_is_up() -> None:
    if not KEY:
        pytest.skip("set NEXUS_E2E_API_KEY")
    wait_for(ready, 180, "the API to be ready")
    with client() as c:
        types = c.get("/system").json()["job_types"]
    if "test.sleep" not in types:
        pytest.skip("set TASK_ENABLE_TEST_HANDLERS=true for the drill")


def test_health_reports_every_dependency_and_every_container_is_healthy() -> None:
    with httpx.Client(timeout=5) as c:
        body = c.get(f"{API}/ready").json()
    assert body == {"status": "ok", "postgres": "ok", "redis": "ok", "rabbitmq": "ok"}
    rows = [json.loads(line) for line in compose("ps", "--format", "json").splitlines() if line]
    health = {(r["Service"], r["Name"]): r.get("Health", "") for r in rows}
    for service in ("api", "scheduler", "worker", "postgres", "rabbitmq", "redis"):
        states = [h for (s, _), h in health.items() if s == service]
        assert states and all(h == "healthy" for h in states), (service, states)


def test_jobs_survive_an_api_restart() -> None:
    with client() as c:
        ids = [submit(c, "test.sleep", {"seconds": 2}) for _ in range(10)]
        compose("restart", "api")
        wait_for(ready, 120, "the API to come back")
        wait_states(c, ids, {"completed"}, 120)
        assert_completed_exactly_once(c, ids)


def test_jobs_submitted_while_the_broker_is_down_run_when_it_returns() -> None:
    compose("stop", "rabbitmq")
    try:
        with client() as c:
            # The API still accepts work: the job and its dispatch message are
            # committed to PostgreSQL (outbox); nothing is marked QUEUED.
            ids = [submit(c, "echo", {"n": n}) for n in range(3)]
            assert all(detail(c, i)["job"]["state"] == "pending" for i in ids)
    finally:
        compose("start", "rabbitmq")
    with client() as c:
        wait_states(c, ids, {"completed"}, 180)
        assert_completed_exactly_once(c, ids)


def test_graceful_worker_restart_hands_back_unfinished_work() -> None:
    with client() as c:
        # Longer than WORKER_SHUTDOWN_GRACE_SECONDS (25 s): cannot finish in time.
        ids = [submit(c, "test.sleep", {"seconds": 35}, timeout_seconds=120) for _ in range(2)]
        wait_states(c, ids, {"running"}, 60)
        compose("restart", "worker", timeout=180)
        wait_states(c, ids, {"completed"}, 240)
        assert_completed_exactly_once(c, ids)
        for job_id in ids:
            attempts = {e["attempt"]: e["status"] for e in detail(c, job_id)["executions"]}
            assert attempts[1] in {"lost", "failed"} and attempts[2] == "completed", attempts


def test_killed_workers_are_detected_and_their_jobs_retried() -> None:
    with client() as c:
        ids = [submit(c, "test.sleep", {"seconds": 20}, timeout_seconds=120) for _ in range(2)]
        wait_states(c, ids, {"running"}, 60)
        compose("kill", "-s", "SIGKILL", "worker")  # no drain, no deregistration
        compose("up", "-d", "--wait", "worker", timeout=180)
        # Heartbeat TTL (30 s) expires, the scheduler fails and retries the jobs.
        wait_states(c, ids, {"completed"}, 240)
        assert_completed_exactly_once(c, ids)
        workers = c.get("/workers").json()
        assert sum(w["status"] == "healthy" for w in workers) >= 2


def test_database_and_redis_restarts_are_survived() -> None:
    compose("restart", "postgres")
    compose("restart", "redis")
    wait_for(ready, 180, "readiness after PostgreSQL and Redis restarts")
    with client() as c:
        ids = [submit(c, "echo", {"after": "restart"}) for _ in range(3)]
        wait_states(c, ids, {"completed"}, 120)
        assert_completed_exactly_once(c, ids)


def test_retries_dead_letters_and_recovery() -> None:
    with client() as c:
        flaky = submit(
            c, "test.flaky", {"succeed_on_attempt": 2}, max_attempts=3, initial_backoff_seconds=1
        )
        dead = submit(
            c, "test.flaky", {"succeed_on_attempt": 3}, max_attempts=2, initial_backoff_seconds=1
        )
        wait_states(c, [flaky], {"completed"}, 60)
        assert detail(c, flaky)["job"]["attempt"] == 2
        wait_states(c, [dead], {"failed"}, 60)
        job = detail(c, dead)["job"]
        assert job["dead_lettered_at"] and "attempts exhausted (2)" in job["last_error"]
        listed = c.get("/jobs", params={"dead_lettered": "true", "error": "flaky"}).json()
        assert dead in {j["job_id"] for j in listed["items"]}
        r = c.post("/jobs/bulk-retry", json={"job_ids": [dead]})
        assert [j["job_id"] for j in r.json()["retried"]] == [dead]
        wait_states(c, [dead], {"completed"}, 60)
        assert detail(c, dead)["job"]["attempt"] == 3


def test_full_stop_and_start_keeps_every_job() -> None:
    with client() as c:
        total_before = c.get("/jobs", params={"page_size": 1}).json()["total"]
        ids = [submit(c, "test.sleep", {"seconds": 40}, timeout_seconds=120) for _ in range(2)]
        ids += [submit(c, "test.sleep", {"seconds": 1}) for _ in range(6)]
        wait_states(c, ids[:2], {"running"}, 60)
    compose("stop", timeout=240)  # graceful: workers drain, data stays in volumes
    compose("up", "-d", "--wait", timeout=300)
    wait_for(ready, 180, "the stack to come back")
    with client() as c:
        assert c.get("/jobs", params={"page_size": 1}).json()["total"] == total_before + 8
        wait_states(c, ids, {"completed"}, 300)
        assert_completed_exactly_once(c, ids)


def test_migrations_are_idempotent() -> None:
    compose("run", "--rm", "migrate")  # second run on an up-to-date schema
    current = compose("run", "--rm", "migrate", "alembic", "current")
    heads = compose("run", "--rm", "migrate", "alembic", "heads")
    assert heads.split()[0] in current


def test_logs_are_structured_and_traceable() -> None:
    lines = compose("logs", "--no-color", "--no-log-prefix", "--tail", "400", "worker").splitlines()
    records = [json.loads(line) for line in lines if line.startswith("{")]
    finished = [r for r in records if r.get("msg") == "job finished"]
    assert finished, "no 'job finished' records in worker logs"
    assert {"job_id", "attempt", "outcome", "duration_ms", "correlation_id"} <= finished[-1].keys()
