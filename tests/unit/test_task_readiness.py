from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.scheduled_task import ScheduledTask
from nexus.domain.task_readiness import (
    TaskNotEligibleError,
    is_dependency_satisfied,
    is_eligible_for_ready,
    promote_to_ready,
)
from nexus.domain.task_state import TaskInstance, TaskState

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _task(state: TaskState = TaskState.PENDING, **overrides) -> ScheduledTask:
    defaults = {
        "instance": TaskInstance(task_id=uuid4(), state=state),
        "tenant_id": "acme",
        "deadline": None,
        "not_before": None,
        "fair_share_weight": 1.0,
        "depends_on": frozenset(),
        "resource_requirements": None,
        "ready_since": None,
    }
    defaults.update(overrides)
    return ScheduledTask(**defaults)


# ---- is_dependency_satisfied ------------------------------------------------


def test_no_dependencies_is_always_satisfied():
    task = _task(depends_on=frozenset())
    assert is_dependency_satisfied(task, frozenset()) is True


def test_dependencies_satisfied_when_subset_of_completed():
    dep_a, dep_b = uuid4(), uuid4()
    task = _task(depends_on=frozenset({dep_a, dep_b}))
    assert is_dependency_satisfied(task, frozenset({dep_a, dep_b, uuid4()})) is True


def test_dependencies_not_satisfied_when_one_is_missing():
    dep_a, dep_b = uuid4(), uuid4()
    task = _task(depends_on=frozenset({dep_a, dep_b}))
    assert is_dependency_satisfied(task, frozenset({dep_a})) is False


# ---- is_eligible_for_ready ---------------------------------------------------


@pytest.mark.parametrize(
    "state",
    [
        TaskState.AWAITING_ADMISSION,
        TaskState.READY,
        TaskState.DISPATCHED,
        TaskState.RUNNING,
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.PERMANENTLY_FAILED,
        TaskState.CANCELLED,
        TaskState.REJECTED,
    ],
)
def test_only_pending_tasks_are_eligible(state):
    task = _task(state=state)
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is False


def test_pending_with_no_dependencies_is_eligible():
    task = _task(state=TaskState.PENDING)
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is True


def test_pending_with_unmet_dependency_is_not_eligible():
    dep = uuid4()
    task = _task(state=TaskState.PENDING, depends_on=frozenset({dep}))
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is False


def test_pending_with_met_dependency_is_eligible():
    dep = uuid4()
    task = _task(state=TaskState.PENDING, depends_on=frozenset({dep}))
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset({dep})) is True


def test_not_before_in_the_future_makes_task_ineligible():
    task = _task(state=TaskState.PENDING, not_before=NOW + timedelta(minutes=1))
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is False


def test_not_before_exactly_now_is_eligible():
    task = _task(state=TaskState.PENDING, not_before=NOW)
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is True


def test_not_before_in_the_past_is_eligible():
    task = _task(state=TaskState.PENDING, not_before=NOW - timedelta(minutes=1))
    assert is_eligible_for_ready(task, now=NOW, completed_task_ids=frozenset()) is True


# ---- promote_to_ready ---------------------------------------------------------


def test_promote_to_ready_transitions_state_and_sets_ready_since():
    task = _task(state=TaskState.PENDING)

    promoted, record = promote_to_ready(task, now=NOW, completed_task_ids=frozenset())

    assert promoted.state == TaskState.READY
    assert promoted.ready_since == NOW
    assert record.from_state == TaskState.PENDING
    assert record.to_state == TaskState.READY


def test_promote_to_ready_does_not_mutate_the_original():
    task = _task(state=TaskState.PENDING)

    promote_to_ready(task, now=NOW, completed_task_ids=frozenset())

    assert task.state == TaskState.PENDING
    assert task.ready_since is None


def test_promote_to_ready_raises_for_wrong_state():
    task = _task(state=TaskState.RUNNING)

    with pytest.raises(TaskNotEligibleError) as exc_info:
        promote_to_ready(task, now=NOW, completed_task_ids=frozenset())

    assert exc_info.value.task_id == task.task_id
    assert "not pending" in exc_info.value.reason


def test_promote_to_ready_raises_for_unmet_dependencies():
    dep = uuid4()
    task = _task(state=TaskState.PENDING, depends_on=frozenset({dep}))

    with pytest.raises(TaskNotEligibleError) as exc_info:
        promote_to_ready(task, now=NOW, completed_task_ids=frozenset())

    assert "unmet dependencies" in exc_info.value.reason
    assert str(dep) in exc_info.value.reason


def test_promote_to_ready_raises_for_future_not_before():
    task = _task(state=TaskState.PENDING, not_before=NOW + timedelta(hours=1))

    with pytest.raises(TaskNotEligibleError) as exc_info:
        promote_to_ready(task, now=NOW, completed_task_ids=frozenset())

    assert "not_before" in exc_info.value.reason
