"""Guards of the opt-in demo seed (`python -m nexus.demo`)."""

from __future__ import annotations

from typing import Any

from nexus.demo import Client, scenario, seed

_TYPES = ["test.echo", "test.flaky", "test.fail", "test.sleep"]


class FakeClient(Client):
    def __init__(self, system: dict[str, Any]) -> None:
        super().__init__("http://api.invalid", "k")
        self.system = system
        self.calls: list[tuple[str, str]] = []

    def request(
        self, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> Any:
        self.calls.append((method, path))
        if path == "/system":
            return self.system
        if path == "/queue":
            return {"broker": {"dead_letter_messages": 0}}
        if method == "POST" and path == "/jobs":
            return {"job_id": f"id-{len(self.calls)}", "state": "queued"}
        raise AssertionError(f"unexpected call {method} {path}")


def _system(env: str, types: list[str] = _TYPES) -> dict[str, Any]:
    return {"environment": env, "job_types": types, "tenant_id": "dev"}


def test_refuses_non_local_environment() -> None:
    client = FakeClient(_system("production"))
    assert seed(client, allow_non_local=False, wait=False, timeout=1) == 2
    assert all(method == "GET" for method, _ in client.calls)


def test_refuses_when_test_handlers_are_disabled() -> None:
    client = FakeClient(_system("local", ["test.echo"]))
    assert seed(client, allow_non_local=False, wait=False, timeout=1) == 2
    assert ("POST", "/jobs") not in client.calls


def test_submits_whole_scenario_through_public_api() -> None:
    client = FakeClient(_system("local"))
    assert seed(client, allow_non_local=False, wait=False, timeout=1) == 0
    posts = [c for c in client.calls if c == ("POST", "/jobs")]
    assert len(posts) == len(scenario())


def test_scenario_covers_required_outcomes() -> None:
    jobs = scenario()
    outcomes = {(j.expected_state, j.expected_attempts, j.expect_dead_lettered) for j in jobs}
    assert ("completed", 1, False) in outcomes  # plain success
    assert ("completed", 2, False) in outcomes  # retryable failure that recovers
    assert ("failed", 1, True) in outcomes  # permanent failure, no retries
    assert ("failed", 3, True) in outcomes  # retries exhausted → dead-letter
    # enough concurrent load to occupy both workers (capacities 4 + 2)
    assert sum(j.job_type == "test.sleep" for j in jobs) > 6
