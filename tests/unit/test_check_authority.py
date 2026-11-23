from typing import Any
from uuid import uuid4

import httpx
import pytest

from nexus.application.interfaces.task_handler import AuthorityLostError, TaskContext
from nexus.infrastructure.execution.handlers import WebhookHandler


def _context(check: Any = None) -> TaskContext:
    return TaskContext(
        task_id=uuid4(),
        tenant_id="acme",
        attempt=1,
        max_attempts=3,
        deadline=None,
        authority_check=check,
    )


def test_check_authority_is_a_no_op_without_a_tracker():
    _context().check_authority()


def test_check_authority_passes_while_valid_and_raises_once_lost():
    valid = {"now": True}
    context = _context(lambda: valid["now"])
    context.check_authority()
    valid["now"] = False
    with pytest.raises(AuthorityLostError):
        context.check_authority()


async def test_webhook_is_not_sent_after_authority_is_lost(monkeypatch: pytest.MonkeyPatch):
    sent: list[str] = []

    async def post(self: httpx.AsyncClient, url: str, **kwargs: Any) -> httpx.Response:
        sent.append(url)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    handler = WebhookHandler(frozenset({"hooks.example.com"}))
    payload = {"url": "https://hooks.example.com/x", "body": {"a": 1}}

    with pytest.raises(AuthorityLostError):
        await handler(payload, _context(lambda: False))
    assert sent == []

    assert await handler(payload, _context(lambda: True)) == {"status_code": 200}
    assert sent == ["https://hooks.example.com/x"]
