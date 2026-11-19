"""Optional, explicit demo scenario for NEXUS.

    python -m nexus.demo seed --api-url http://localhost:8000 --api-key dev-key --wait
    docker compose --profile demo run --rm seed

The seed is an ordinary API client. It submits jobs through the public API,
so every job is persisted, dispatched through RabbitMQ and executed by the
real workers; nothing is written to the database directly and no dashboard
value is faked. Each run is tagged with a run id and uses idempotency keys,
so re-running creates a new, clearly labelled batch rather than duplicates.

Safety:
* refuses to run unless the API reports a local/test/development
  environment (override with --allow-non-local);
* needs the `test.*` job types, which only exist when
  TASK_ENABLE_TEST_HANDLERS=true.

Scenario (all names prefixed `demo-<run>/`):
  build-api               echo                      -> completed
  rotate-certificates     fails once, then succeeds -> retried, completed
  scan-images             non-retryable error       -> failed after 1 attempt
  backup-orders           always fails, 3 attempts  -> retries exhausted,
                                                       dead-lettered
  load-1 .. load-8        3-6 s sleeps              -> spread over the
                                                       workers by capacity
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

_LOCAL = {"local", "test", "development"}
_TERMINAL = {"completed", "failed", "cancelled"}


@dataclass(frozen=True)
class DemoJob:
    key: str
    name: str
    job_type: str
    payload: dict[str, Any]
    priority: str = "normal"
    max_attempts: int = 3
    expected_state: str = "completed"
    expected_attempts: int | None = None
    expect_dead_lettered: bool = False


def scenario() -> list[DemoJob]:
    jobs = [
        DemoJob(
            "build-api", "build-api", "echo", {"repo": "platform/api"}, "high", 3, "completed", 1
        ),
        DemoJob(
            "rotate-certificates",
            "rotate-certificates",
            "test.flaky",
            {"succeed_on_attempt": 2},
            "critical",
            3,
            "completed",
            2,
        ),
        DemoJob(
            "scan-images",
            "scan-images",
            "test.fail",
            {"message": "registry returned 403", "permanent": True},
            "normal",
            3,
            "failed",
            1,
            True,
        ),
        DemoJob(
            "backup-orders",
            "backup-orders",
            "test.fail",
            {"message": "object storage unreachable"},
            "high",
            3,
            "failed",
            3,
            True,
        ),
    ]
    for i in range(1, 9):
        jobs.append(
            DemoJob(
                f"load-{i}",
                f"load-{i}",
                "test.sleep",
                {"seconds": 3 + i % 4},
                "low",
                3,
                "completed",
                1,
            )
        )
    return jobs


class Client:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base = base_url.rstrip("/") + "/api/v1"
        self.key = api_key

    def request(
        self, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
                **(headers or {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:300]
            raise SystemExit(f"{method} {path} failed: HTTP {exc.code} {detail}") from None
        except urllib.error.URLError as exc:
            raise SystemExit(f"cannot reach the NEXUS API at {self.base}: {exc.reason}") from None


def seed(client: Client, *, allow_non_local: bool, wait: bool, timeout: float) -> int:
    system = client.request("GET", "/system")
    if system["environment"] not in _LOCAL and not allow_non_local:
        print(
            f"Refusing to seed demo data into environment '{system['environment']}'. "
            "Use --allow-non-local if you really mean it.",
            file=sys.stderr,
        )
        return 2
    missing = {"test.flaky", "test.fail", "test.sleep"} - set(system["job_types"])
    if missing:
        print(
            f"The API does not offer {sorted(missing)}; start it with "
            "TASK_ENABLE_TEST_HANDLERS=true to run the demo.",
            file=sys.stderr,
        )
        return 2

    run = datetime.now(UTC).strftime("%m%d-%H%M%S")
    print(f"Seeding demo run {run} into tenant '{system['tenant_id']}' ...")
    submitted: dict[str, tuple[DemoJob, str]] = {}
    for job in scenario():
        created = client.request(
            "POST",
            "/jobs",
            {
                "name": f"demo-{run}/{job.name}",
                "job_type": job.job_type,
                "payload": job.payload,
                "priority": job.priority,
                "max_attempts": job.max_attempts,
                "initial_backoff_seconds": 1,
                "max_backoff_seconds": 4,
            },
            headers={"Idempotency-Key": f"demo-{run}-{job.key}"},
        )
        submitted[job.key] = (job, created["job_id"])
        print(f"  submitted {job.name:<22} {created['state']}")
    if not wait:
        print("Submitted. Watch progress in the dashboard.")
        return 0

    deadline = time.monotonic() + timeout
    results: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline and len(results) < len(submitted):
        for key, (_, job_id) in submitted.items():
            if key in results:
                continue
            detail = client.request("GET", f"/jobs/{job_id}")
            if detail["job"]["state"] in _TERMINAL:
                results[key] = detail
        time.sleep(1)

    failures = 0
    workers_used: dict[str, int] = {}
    print("\nResults (read back from the API):")
    for key, (job, _) in submitted.items():
        detail = results.get(key)
        if detail is None:
            print(f"  [FAIL] {job.name:<22} did not finish within {timeout:.0f}s")
            failures += 1
            continue
        j = detail["job"]
        for execution in detail["executions"]:
            wid = execution["worker_id"] or "?"
            workers_used[wid] = workers_used.get(wid, 0) + 1
        ok = j["state"] == job.expected_state
        if job.expected_attempts is not None:
            ok = ok and j["attempt"] == job.expected_attempts
        if job.expect_dead_lettered:
            ok = ok and j["dead_lettered_at"] is not None
        failures += 0 if ok else 1
        note = " dead-lettered" if j["dead_lettered_at"] else ""
        print(
            f"  [{'ok' if ok else 'FAIL'}] {job.name:<22} {j['state']:<10} "
            f"attempts={j['attempt']}{note}"
        )

    queue_now = client.request("GET", "/queue")
    after = queue_now["broker"]
    workers = client.request("GET", "/workers")
    print("\nWorkers (registry):")
    for w in workers:
        print(
            f"  {w['worker_id'][:8]} {w['status']:<9} capacity={w['capacity_slots']} "
            f"executions in this run={workers_used.get(w['worker_id'], 0)}"
        )
    if after.get("available"):
        print(
            f"\nDead-lettered: {queue_now['dead_lettered_jobs']} job(s) in the database, "
            f"{after['dead_letter_messages']} message(s) in RabbitMQ nexus.dead-letter "
            "(the scheduler prunes messages whose job was retried)"
        )
    print("\nDemo " + ("verified." if failures == 0 else f"had {failures} unexpected result(s)."))
    return 0 if failures == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nexus.demo")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("seed", help="submit the demo scenario through the API")
    p.add_argument("--api-url", default=os.environ.get("NEXUS_API_URL", "http://localhost:8000"))
    p.add_argument("--api-key", default=os.environ.get("NEXUS_API_KEY", ""))
    p.add_argument("--wait", action="store_true", help="wait and verify outcomes")
    p.add_argument("--timeout", type=float, default=120)
    p.add_argument("--allow-non-local", action="store_true")
    args = parser.parse_args(argv)
    if not args.api_key:
        parser.error("--api-key (or NEXUS_API_KEY) is required")
    return seed(
        Client(args.api_url, args.api_key),
        allow_non_local=args.allow_non_local,
        wait=args.wait,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    sys.exit(main())
