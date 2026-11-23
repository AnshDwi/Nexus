"""Built-in task handlers.

NEXUS executes *registered* task types only; it never runs code supplied in a
request. New task types are added by registering a handler here (or in a
deployment-specific module) -- an explicit code change that goes through
review and CI.

Built-ins:
  echo          returns its payload (smoke tests, pipelines).
  http.webhook  POSTs the payload to an allow-listed HTTPS host; 5xx/timeouts
                are retried, 4xx fail permanently. Sends the task's
                idempotency key so receivers can deduplicate.
  test.sleep    (test handlers only) sleeps `seconds`.
  test.flaky    (test handlers only) fails until attempt >= `succeed_on_attempt`.
  test.fail     (test handlers only) always fails; `permanent` controls retry.
"""

import asyncio
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from nexus.application.interfaces.task_handler import (
    PermanentTaskError,
    TaskContext,
    TaskHandler,
)


class InMemoryTaskHandlerRegistry:
    def __init__(self, handlers: Mapping[str, TaskHandler]) -> None:
        self._handlers = dict(handlers)

    def get(self, task_type: str) -> TaskHandler | None:
        return self._handlers.get(task_type)

    def task_types(self) -> frozenset[str]:
        return frozenset(self._handlers)


async def echo_handler(payload: dict[str, Any], context: TaskContext) -> Any:
    return {"echo": payload, "attempt": context.attempt}


class WebhookHandler:
    def __init__(self, allowed_hosts: frozenset[str], *, timeout_seconds: float = 10.0) -> None:
        self._allowed_hosts = allowed_hosts
        self._timeout = timeout_seconds

    async def __call__(self, payload: dict[str, Any], context: TaskContext) -> Any:
        url = payload.get("url")
        if not isinstance(url, str):
            raise PermanentTaskError("payload.url must be a string")
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        if parts.scheme != "https" or host not in self._allowed_hosts:
            # SSRF guard: only explicitly allow-listed HTTPS hosts.
            raise PermanentTaskError(f"host '{host}' is not in WEBHOOK_ALLOWED_HOSTS or not https")
        body = payload.get("body", {})
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=False) as client:
            response = await client.post(
                url,
                json=body,
                headers={
                    "Idempotency-Key": context.idempotency_key,
                    "X-Nexus-Task-Id": str(context.task_id),
                    "X-Nexus-Attempt": str(context.attempt),
                },
            )
        if 400 <= response.status_code < 500 and response.status_code not in (408, 429):
            raise PermanentTaskError(f"webhook returned {response.status_code}")
        response.raise_for_status()
        return {"status_code": response.status_code}


async def sleep_handler(payload: dict[str, Any], context: TaskContext) -> Any:
    seconds = float(payload.get("seconds", 1))
    await asyncio.sleep(seconds)
    return {"slept": seconds}


async def flaky_handler(payload: dict[str, Any], context: TaskContext) -> Any:
    succeed_on = int(payload.get("succeed_on_attempt", 2))
    if context.attempt < succeed_on:
        raise RuntimeError(f"flaky failure on attempt {context.attempt}")
    return {"succeeded_on_attempt": context.attempt}


async def fail_handler(payload: dict[str, Any], context: TaskContext) -> Any:
    message = str(payload.get("message", "requested failure"))
    if payload.get("permanent"):
        raise PermanentTaskError(message)
    raise RuntimeError(message)


def build_registry(
    *, enable_test_handlers: bool, webhook_allowed_hosts: frozenset[str]
) -> InMemoryTaskHandlerRegistry:
    handlers: dict[str, TaskHandler] = {"echo": echo_handler}
    if webhook_allowed_hosts:
        handlers["http.webhook"] = WebhookHandler(webhook_allowed_hosts)
    if enable_test_handlers:
        handlers.update(
            {"test.sleep": sleep_handler, "test.flaky": flaky_handler, "test.fail": fail_handler}
        )
    return InMemoryTaskHandlerRegistry(handlers)
