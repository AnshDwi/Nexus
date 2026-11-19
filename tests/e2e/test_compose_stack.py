"""End-to-end tests against a running stack (`docker compose up -d --build --wait`).

Black-box: they only use HTTP, exactly like the dashboard. Run with
    uv run pytest -m e2e
Configuration: NEXUS_E2E_API_URL (default http://localhost:8000),
NEXUS_E2E_DASHBOARD_URL (default http://localhost:5173),
NEXUS_E2E_API_KEY (default dev-key).
"""

import os
import time
import uuid
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

API = os.environ.get("NEXUS_E2E_API_URL", "http://localhost:8000").rstrip("/") + "/api/v1"
DASHBOARD = os.environ.get("NEXUS_E2E_DASHBOARD_URL", "http://localhost:5173").rstrip("/")
KEY = os.environ.get("NEXUS_E2E_API_KEY", "dev-key")
AUTH = {"Authorization": f"Bearer {KEY}"}


@pytest.fixture(scope="module")
def http() -> httpx.Client:
    with httpx.Client(timeout=15) as client:
        yield client


def _create(http: httpx.Client, **body: Any) -> dict[str, Any]:
    body.setdefault("name", f"e2e-{uuid.uuid4().hex[:8]}")
    r = http.post(f"{API}/jobs", json=body, headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())})
    assert r.status_code == 201, r.text
    return r.json()


def _wait(http: httpx.Client, job_id: str, *states: str, timeout: float = 60) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        d = http.get(f"{API}/jobs/{job_id}", headers=AUTH).json()
        if d["job"]["state"] in states:
            return d
        assert time.monotonic() < deadline, f"stuck in {d['job']['state']}"
        time.sleep(0.5)


def test_every_dependency_is_ready(http: httpx.Client) -> None:
    ready = http.get(f"{API}/ready").json()
    assert ready == {"status": "ok", "postgres": "ok", "redis": "ok", "rabbitmq": "ok"}


def test_two_workers_with_different_capacities_are_healthy(http: httpx.Client) -> None:
    workers = [
        w for w in http.get(f"{API}/workers", headers=AUTH).json() if w["status"] == "healthy"
    ]
    assert sorted(w["capacity_slots"] for w in workers)[-2:] == [2, 4]


def test_dashboard_is_served_and_proxies_the_api(http: httpx.Client) -> None:
    page = http.get(f"{DASHBOARD}/jobs")  # deep link -> SPA
    assert page.status_code == 200 and '<div id="root">' in page.text
    proxied = http.get(f"{DASHBOARD}/api/v1/system", headers=AUTH)
    assert proxied.status_code == 200 and proxied.json()["event_bus"] == "rabbitmq"


def test_job_lifecycle_through_rabbitmq_and_a_worker(http: httpx.Client) -> None:
    job = _create(http, job_type="echo", payload={"hello": "compose"}, priority="high")
    assert job["state"] in ("queued", "running", "completed")  # broker confirmed on submit
    d = _wait(http, job["job_id"], "completed")
    assert d["job"]["result"]["echo"] == {"hello": "compose"}
    assert [(t["from_state"], t["to_state"]) for t in d["transitions"]] == [
        ("pending", "queued"),
        ("queued", "running"),
        ("running", "completed"),
    ]
    assert d["executions"][0]["status"] == "completed"


def test_retryable_failure_is_retried_until_success(http: httpx.Client) -> None:
    job = _create(
        http,
        job_type="test.flaky",
        payload={"succeed_on_attempt": 2},
        initial_backoff_seconds=1,
        max_backoff_seconds=2,
    )
    d = _wait(http, job["job_id"], "completed")
    assert d["job"]["attempt"] == 2
    assert ("failed", "retrying") in [(t["from_state"], t["to_state"]) for t in d["transitions"]]


def test_exhausted_job_is_dead_lettered_then_retried_manually(http: httpx.Client) -> None:
    dlq_before = http.get(f"{API}/queue", headers=AUTH).json()["broker"]["dead_letter_messages"]
    job = _create(
        http,
        job_type="test.flaky",
        payload={"succeed_on_attempt": 3},
        max_attempts=2,
        initial_backoff_seconds=1,
        max_backoff_seconds=1,
    )
    d = _wait(http, job["job_id"], "failed")
    assert d["job"]["dead_lettered_at"] is not None
    assert d["job"]["next_attempt_at"] is None  # regression: no stale "next attempt"
    listed = http.get(f"{API}/jobs", params={"dead_lettered": "true"}, headers=AUTH).json()
    assert job["job_id"] in {j["job_id"] for j in listed["items"]}
    time.sleep(1)
    dlq_after = http.get(f"{API}/queue", headers=AUTH).json()["broker"]["dead_letter_messages"]
    assert dlq_after == dlq_before + 1

    retried = http.post(f"{API}/jobs/{job['job_id']}/retry", headers=AUTH)
    assert retried.status_code == 200
    done = _wait(http, job["job_id"], "completed")
    assert done["job"]["attempt"] == 3 and done["job"]["dead_lettered_at"] is None


def test_permanent_failure_is_not_retried(http: httpx.Client) -> None:
    job = _create(http, job_type="test.fail", payload={"permanent": True, "message": "bad input"})
    d = _wait(http, job["job_id"], "failed")
    assert d["job"]["attempt"] == 1 and len(d["executions"]) == 1


def test_summary_counts_come_from_persisted_data(http: httpx.Client) -> None:
    summary = http.get(f"{API}/metrics/summary", headers=AUTH).json()
    listed = http.get(f"{API}/jobs", params={"page_size": 1}, headers=AUTH).json()
    assert summary["total_jobs"] == listed["total"]
    failed = http.get(
        f"{API}/jobs", params={"status": "failed", "page_size": 1}, headers=AUTH
    ).json()
    assert summary["failed"] == failed["total"]
    dead = http.get(f"{API}/jobs", params={"dead_lettered": "true", "page_size": 1}, headers=AUTH)
    assert summary["dead_lettered"] == dead.json()["total"]
