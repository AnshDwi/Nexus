import itertools
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from nexus.domain.task_state import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATES,
    InvalidTaskTransitionError,
    TaskInstance,
    TaskState,
    is_valid_transition,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _instance(state: TaskState) -> TaskInstance:
    return TaskInstance(task_id=uuid4(), state=state)


def test_transition_table_covers_every_state():
    assert set(ALLOWED_TRANSITIONS.keys()) == set(TaskState)


def test_terminal_states_have_no_outgoing_transitions():
    for state in TERMINAL_STATES:
        assert ALLOWED_TRANSITIONS[state] == frozenset()


@pytest.mark.parametrize(("from_state", "to_state"), list(itertools.product(TaskState, TaskState)))
def test_transition_behavior_matches_the_table_exhaustively(from_state, to_state):
    """The single source of truth: every one of the 100 (from, to) pairs is
    checked against ALLOWED_TRANSITIONS, so this test is exhaustive by
    construction -- it can't miss a case the way a hand-picked list could,
    and it automatically re-verifies itself if the table ever changes."""
    instance = _instance(from_state)

    if is_valid_transition(from_state, to_state):
        new_instance, record = instance.transition_to(to_state, occurred_at=NOW)
        assert new_instance.state == to_state
        assert new_instance.task_id == instance.task_id
        assert record.from_state == from_state
        assert record.to_state == to_state
        assert record.occurred_at == NOW
    else:
        with pytest.raises(InvalidTaskTransitionError) as exc_info:
            instance.transition_to(to_state, occurred_at=NOW)
        assert exc_info.value.from_state == from_state
        assert exc_info.value.to_state == to_state


def test_transitioning_to_the_same_state_is_always_rejected():
    for state in TaskState:
        with pytest.raises(InvalidTaskTransitionError):
            _instance(state).transition_to(state, occurred_at=NOW)


def test_original_instance_is_never_mutated_by_a_transition():
    original = _instance(TaskState.READY)

    new_instance, _record = original.transition_to(TaskState.DISPATCHED, occurred_at=NOW)

    assert original.state == TaskState.READY
    assert new_instance.state == TaskState.DISPATCHED
    assert new_instance is not original


def test_two_concurrent_attempts_from_the_same_original_instance_are_independent():
    """Domain-level duplicate-attempt scenario: two callers holding the same
    (immutable) original instance both attempt a transition. Both succeed
    independently at the domain level -- serializing/rejecting one of them
    under real concurrency is a persistence-layer concern for a later
    phase, not something a pure domain object can or should decide."""
    original = _instance(TaskState.READY)

    first_new, _ = original.transition_to(TaskState.DISPATCHED, occurred_at=NOW)
    second_new, _ = original.transition_to(TaskState.CANCELLED, occurred_at=NOW)

    assert first_new.state == TaskState.DISPATCHED
    assert second_new.state == TaskState.CANCELLED
    assert original.state == TaskState.READY


@pytest.mark.parametrize(
    "state",
    [
        TaskState.AWAITING_ADMISSION,
        TaskState.PENDING,
        TaskState.READY,
        TaskState.DISPATCHED,
        TaskState.RUNNING,
        TaskState.FAILED,
    ],
)
def test_cancellation_is_allowed_from_every_non_terminal_state(state):
    _, record = _instance(state).transition_to(
        TaskState.CANCELLED, occurred_at=NOW, reason="operator requested cancellation"
    )
    assert record.to_state == TaskState.CANCELLED
    assert record.reason == "operator requested cancellation"


@pytest.mark.parametrize("state", sorted(TERMINAL_STATES, key=lambda s: s.value))
def test_terminal_states_remain_immutable_against_every_possible_target(state):
    for target in TaskState:
        with pytest.raises(InvalidTaskTransitionError):
            _instance(state).transition_to(target, occurred_at=NOW)


def test_failed_task_can_retry_to_ready():
    instance = _instance(TaskState.FAILED)

    new_instance, record = instance.transition_to(
        TaskState.READY, occurred_at=NOW, reason="attempt 2 of 5"
    )

    assert new_instance.state == TaskState.READY
    assert record.reason == "attempt 2 of 5"


def test_failed_task_can_exhaust_retries_to_permanently_failed():
    instance = _instance(TaskState.FAILED)

    new_instance, record = instance.transition_to(
        TaskState.PERMANENTLY_FAILED, occurred_at=NOW, reason="max attempts exceeded"
    )

    assert new_instance.state == TaskState.PERMANENTLY_FAILED
    assert record.reason == "max attempts exceeded"


def test_full_lifecycle_with_one_retry_reaches_succeeded():
    """A realistic multi-hop sequence, not just isolated single transitions:
    admitted -> ready -> dispatched -> running -> failed -> retried -> ...
    -> succeeded."""
    instance = _instance(TaskState.AWAITING_ADMISSION)
    path = [
        TaskState.PENDING,
        TaskState.READY,
        TaskState.DISPATCHED,
        TaskState.RUNNING,
        TaskState.FAILED,
        TaskState.READY,
        TaskState.DISPATCHED,
        TaskState.RUNNING,
        TaskState.SUCCEEDED,
    ]

    records = []
    for next_state in path:
        instance, record = instance.transition_to(next_state, occurred_at=NOW)
        records.append(record)

    assert instance.state == TaskState.SUCCEEDED
    assert [r.to_state for r in records] == path


def test_admission_rejection_is_terminal():
    instance = _instance(TaskState.AWAITING_ADMISSION)

    new_instance, record = instance.transition_to(
        TaskState.REJECTED, occurred_at=NOW, reason="schedulability check failed"
    )

    assert new_instance.state == TaskState.REJECTED
    assert new_instance.state in TERMINAL_STATES


def test_dispatched_task_can_be_requeued_if_no_worker_picks_it_up():
    instance = _instance(TaskState.DISPATCHED)

    new_instance, record = instance.transition_to(
        TaskState.READY, occurred_at=NOW, reason="no TaskStarted event within timeout"
    )

    assert new_instance.state == TaskState.READY
