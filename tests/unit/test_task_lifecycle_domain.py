from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.task_lifecycle import (
    FailureDecision,
    blocks_dependents,
    decide_after_failure,
    plan_cancel,
    plan_claim,
    plan_failure,
    plan_success,
    retry_backoff,
)
from nexus.domain.task_state import InvalidTaskTransitionError, TaskInstance, TaskState

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)


def _task(state: TaskState) -> TaskInstance:
    return TaskInstance(task_id=uuid4(), state=state)


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0), (10, 30.0)],
)
def test_retry_backoff_is_exponential_and_capped(attempt, expected):
    backoff = retry_backoff(attempt, initial_backoff_seconds=2, max_backoff_seconds=30)
    assert backoff == timedelta(seconds=expected)


def test_retry_backoff_rejects_zero_based_attempt():
    with pytest.raises(ValueError):
        retry_backoff(0, initial_backoff_seconds=1, max_backoff_seconds=1)


def test_decide_after_failure():
    assert decide_after_failure(attempt=1, max_attempts=3) is FailureDecision.RETRY
    assert decide_after_failure(attempt=3, max_attempts=3) is FailureDecision.GIVE_UP


def test_plan_claim_goes_ready_dispatched_running_with_lease():
    plan = plan_claim(
        _task(TaskState.READY),
        now=NOW,
        timeout=timedelta(seconds=60),
        lease_grace=timedelta(seconds=30),
    )
    assert plan.instance.state is TaskState.RUNNING
    assert [(r.from_state, r.to_state) for r in plan.records] == [
        (TaskState.READY, TaskState.DISPATCHED),
        (TaskState.DISPATCHED, TaskState.RUNNING),
    ]
    assert plan.lease_expires_at == NOW + timedelta(seconds=90)


@pytest.mark.parametrize("state", [TaskState.PENDING, TaskState.RUNNING, TaskState.SUCCEEDED])
def test_plan_claim_refuses_non_ready_tasks(state):
    with pytest.raises(InvalidTaskTransitionError):
        plan_claim(_task(state), now=NOW, timeout=timedelta(1), lease_grace=timedelta(0))


def test_plan_failure_retries_with_backoff_when_attempts_remain():
    plan = plan_failure(
        _task(TaskState.RUNNING),
        attempt=1,
        max_attempts=3,
        initial_backoff_seconds=5,
        max_backoff_seconds=60,
        now=NOW,
        reason="boom",
    )
    assert plan.decision is FailureDecision.RETRY
    assert plan.instance.state is TaskState.READY
    assert plan.next_attempt == 2
    assert plan.retry_not_before == NOW + timedelta(seconds=5)
    assert [r.to_state for r in plan.records] == [TaskState.FAILED, TaskState.READY]


def test_plan_failure_gives_up_when_attempts_exhausted():
    plan = plan_failure(
        _task(TaskState.RUNNING),
        attempt=3,
        max_attempts=3,
        initial_backoff_seconds=5,
        max_backoff_seconds=60,
        now=NOW,
        reason="boom",
    )
    assert plan.decision is FailureDecision.GIVE_UP
    assert plan.instance.state is TaskState.PERMANENTLY_FAILED
    assert plan.next_attempt is None
    assert [r.to_state for r in plan.records] == [TaskState.FAILED, TaskState.PERMANENTLY_FAILED]


def test_plan_success_only_from_running():
    instance, record = plan_success(_task(TaskState.RUNNING), now=NOW)
    assert instance.state is TaskState.SUCCEEDED
    assert record.from_state is TaskState.RUNNING
    with pytest.raises(InvalidTaskTransitionError):
        plan_success(_task(TaskState.READY), now=NOW)


@pytest.mark.parametrize(
    "state", [TaskState.PENDING, TaskState.READY, TaskState.RUNNING, TaskState.AWAITING_ADMISSION]
)
def test_plan_cancel_from_active_states(state):
    instance, _ = plan_cancel(_task(state), now=NOW, reason="user")
    assert instance.state is TaskState.CANCELLED


@pytest.mark.parametrize(
    "state", [TaskState.SUCCEEDED, TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED]
)
def test_plan_cancel_refuses_terminal_states(state):
    with pytest.raises(InvalidTaskTransitionError):
        plan_cancel(_task(state), now=NOW, reason="user")


def test_blocks_dependents():
    assert blocks_dependents(TaskState.PERMANENTLY_FAILED)
    assert blocks_dependents(TaskState.CANCELLED)
    assert blocks_dependents(TaskState.REJECTED)
    assert not blocks_dependents(TaskState.SUCCEEDED)
    assert not blocks_dependents(TaskState.RUNNING)
