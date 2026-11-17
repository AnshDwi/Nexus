"""Operator CLI.

python -m nexus.cli hash-api-key <secret>
    Prints the "<sha256>" to put after "tenant_id:" in API_KEYS.

python -m nexus.cli new-api-key <tenant_id>
    Generates a random key; prints the key (give it to the client) and
    the API_KEYS entry (store it as a Sensitive env var on Vercel).

python -m nexus.cli verify-deployment <base_url> --api-key KEY [--timeout 90]
    Post-deploy check used by CI: liveness, readiness, then submits an
    `echo` task and waits for it to be executed by the queue consumer.
    Exits non-zero on any failure.
"""

import argparse
import secrets
import sys
import time
from typing import Any

import httpx

from nexus.api.security import hash_api_key


def _verify(base_url: str, api_key: str, timeout: float, bypass_secret: str | None) -> int:
    headers = {"Authorization": f"Bearer {api_key}"}
    if bypass_secret:
        # Vercel Deployment Protection bypass for automation.
        headers["x-vercel-protection-bypass"] = bypass_secret
    base = base_url.rstrip("/") + "/api/v1"
    with httpx.Client(timeout=15) as client:

        def check(name: str, response: httpx.Response, expected: int) -> Any:
            ok = response.status_code == expected
            print(f"[{'ok' if ok else 'FAIL'}] {name}: HTTP {response.status_code}")
            if not ok:
                print(response.text[:500])
                raise SystemExit(1)
            return response.json()

        check("liveness", client.get(f"{base}/health", headers=headers), 200)
        check("readiness", client.get(f"{base}/ready", headers=headers), 200)
        check("auth enforced", client.get(f"{base}/tasks"), 401)
        created = check(
            "submit echo task",
            client.post(
                f"{base}/tasks",
                headers=headers,
                json={
                    "task_type": "echo",
                    "payload": {"verify": time.time()},
                    "retry_policy": {
                        "max_attempts": 3,
                        "initial_backoff_seconds": 1,
                        "max_backoff_seconds": 5,
                    },
                },
            ),
            201,
        )
        task_id = created["task"]["task_id"]
        deadline = time.monotonic() + timeout
        state = created["task"]["state"]
        while time.monotonic() < deadline:
            task = client.get(f"{base}/tasks/{task_id}", headers=headers).json()
            state = task["state"]
            if state == "succeeded":
                print(f"[ok] task {task_id} executed on attempt {task['attempt']}")
                return 0
            if state in {"permanently_failed", "cancelled", "rejected"}:
                break
            time.sleep(2)
        print(f"[FAIL] task {task_id} ended in state '{state}'")
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus.cli")
    sub = parser.add_subparsers(dest="command", required=True)
    p_hash = sub.add_parser("hash-api-key")
    p_hash.add_argument("secret")
    p_new = sub.add_parser("new-api-key")
    p_new.add_argument("tenant_id")
    p_verify = sub.add_parser("verify-deployment")
    p_verify.add_argument("base_url")
    p_verify.add_argument("--api-key", required=True)
    p_verify.add_argument("--timeout", type=float, default=90)
    p_verify.add_argument("--protection-bypass", default=None)
    args = parser.parse_args(argv)

    if args.command == "hash-api-key":
        print(hash_api_key(args.secret))
        return 0
    if args.command == "new-api-key":
        key = f"nx_{secrets.token_urlsafe(32)}"
        print(f"API key (give to client): {key}")
        print(f"API_KEYS entry:           {args.tenant_id}:{hash_api_key(key)}")
        return 0
    return _verify(args.base_url, args.api_key, args.timeout, args.protection_bypass)


if __name__ == "__main__":
    sys.exit(main())
