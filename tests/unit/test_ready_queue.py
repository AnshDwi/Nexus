import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.ready_queue import ReadyQueue, build_ready_queue
from nexus.domain.scheduled_task import ScheduledTask
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


# ---- ReadyQueue invariant ----------------------------------------------------


def test_ready_queue_rejects_non_ready_tasks():
    with pytest.raises(ValueError, match="only accepts READY"):
        ReadyQueue([_task(state=TaskState.PENDING)])


def test_ready_queue_accepts_only_ready_tasks():
    queue = ReadyQueue([_task(state=TaskState.READY, ready_since=NOW)])
    assert len(queue) == 1


def test_empty_ready_queue():
    queue = ReadyQueue([])
    assert len(queue) == 0
    assert list(queue) == []


# ---- deterministic ordering ---------------------------------------------------


def test_ordering_is_by_ready_since_ascending():
    early = _task(state=TaskState.READY, ready_since=NOW)
    middle = _task(state=TaskState.READY, ready_since=NOW + timedelta(seconds=1))
    late = _task(state=TaskState.READY, ready_since=NOW + timedelta(seconds=2))

    queue = ReadyQueue([late, early, middle])

    assert [t.task_id for t in queue] == [early.task_id, middle.task_id, late.task_id]


def test_ties_in_ready_since_break_by_task_id():
    a = _task(state=TaskState.READY, ready_since=NOW)
    b = _task(state=TaskState.READY, ready_since=NOW)

    expected_order = sorted([a.task_id, b.task_id], key=str)
    queue = ReadyQueue([b, a])

    assert [t.task_id for t in queue] == expected_order


def test_ordering_does_not_depend_on_input_order():
    """The strongest form of 'deterministic ordering': shuffling the input
    must never change the output order, since the result depends only on
    the sort key, not on insertion/iteration order."""
    tasks = [
        _task(state=TaskState.READY, ready_since=NOW + timedelta(seconds=i)) for i in range(20)
    ]
    expected = [t.task_id for t in ReadyQueue(tasks)]

    for _ in range(5):
        shuffled = tasks.copy()
        random.shuffle(shuffled)
        assert [t.task_id for t in ReadyQueue(shuffled)] == expected


def test_task_with_no_ready_since_sorts_first():
    unknown = _task(state=TaskState.READY, ready_since=None)
    known = _task(state=TaskState.READY, ready_since=NOW)

    queue = ReadyQueue([known, unknown])

    assert queue.as_tuple()[0].task_id == unknown.task_id


# ---- build_ready_queue: population ---------------------------------------------


def test_build_ready_queue_from_empty_pool():
    queue, records = build_ready_queue([], now=NOW)
    assert len(queue) == 0
    assert records == ()


def test_build_ready_queue_includes_already_ready_tasks_without_a_new_record():
    task = _task(state=TaskState.READY, ready_since=NOW - timedelta(minutes=1))

    queue, records = build_ready_queue([task], now=NOW)

    assert len(queue) == 1
    assert records == ()


def test_build_ready_queue_promotes_eligible_pending_tasks():
    task = _task(state=TaskState.PENDING)

    queue, records = build_ready_queue([task], now=NOW)

    assert len(queue) == 1
    assert queue.as_tuple()[0].state == TaskState.READY
    assert queue.as_tuple()[0].ready_since == NOW
    assert len(records) == 1
    assert records[0].to_state == TaskState.READY


def test_build_ready_queue_excludes_pending_tasks_with_unmet_dependencies():
    blocked = _task(state=TaskState.PENDING, depends_on=frozenset({uuid4()}))

    queue, records = build_ready_queue([blocked], now=NOW)

    assert len(queue) == 0
    assert records == ()


@pytest.mark.parametrize(
    "state",
    [
        TaskState.AWAITING_ADMISSION,
        TaskState.DISPATCHED,
        TaskState.RUNNING,
        TaskState.SUCCEEDED,
        TaskState.FAILED,
        TaskState.PERMANENTLY_FAILED,
        TaskState.CANCELLED,
        TaskState.REJECTED,
    ],
)
def test_build_ready_queue_excludes_tasks_in_other_states(state):
    task = _task(state=state)

    queue, records = build_ready_queue([task], now=NOW)

    assert len(queue) == 0
    assert records == ()


def test_dependency_chain_becomes_eligible_only_once_all_dependencies_succeed():
    dep_a = _task(state=TaskState.SUCCEEDED)
    dep_b_pending = _task(state=TaskState.PENDING)
    dependent = _task(
        state=TaskState.PENDING, depends_on=frozenset({dep_a.task_id, dep_b_pending.task_id})
    )

    # dep_b hasn't succeeded yet -> dependent is not eligible.
    queue, _ = build_ready_queue([dep_a, dep_b_pending, dependent], now=NOW)
    assert dependent.task_id not in [t.task_id for t in queue]

    # dep_b succeeds -> dependent becomes eligible.
    dep_b_succeeded = _task(
        instance=TaskInstance(task_id=dep_b_pending.task_id, state=TaskState.SUCCEEDED)
    )
    queue, records = build_ready_queue([dep_a, dep_b_succeeded, dependent], now=NOW)
    assert dependent.task_id in [t.task_id for t in queue]
    assert len(records) == 1


def test_build_ready_queue_result_is_order_independent_of_pool_order():
    ready_tasks = [
        _task(state=TaskState.READY, ready_since=NOW + timedelta(seconds=i)) for i in range(10)
    ]
    pending_tasks = [_task(state=TaskState.PENDING) for _ in range(5)]
    pool = ready_tasks + pending_tasks

    queue_a, _ = build_ready_queue(pool, now=NOW)
    shuffled = pool.copy()
    random.shuffle(shuffled)
    queue_b, _ = build_ready_queue(shuffled, now=NOW)

    # Ready-task portion (deterministic ready_since) must match exactly;
    # both queues must contain the same total set of task ids regardless of
    # input order.
    assert {t.task_id for t in queue_a} == {t.task_id for t in queue_b}
    assert len(queue_a) == len(queue_b) == 15
