from datetime import UTC, datetime, timedelta
from uuid import uuid4

from nexus.domain.admission import evaluate_schedulability
from nexus.domain.task_submission import TaskSubmission

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _submission(**overrides) -> TaskSubmission:
    defaults = {"task_id": uuid4(), "job_id": uuid4()}
    defaults.update(overrides)
    return TaskSubmission(**defaults)


def test_unconstrained_when_no_deadline():
    result = evaluate_schedulability(
        _submission(deadline=None, estimated_duration=timedelta(minutes=5)), now=NOW
    )
    assert result.feasible is True


def test_unconstrained_when_no_estimated_duration():
    result = evaluate_schedulability(
        _submission(deadline=NOW + timedelta(hours=1), estimated_duration=None), now=NOW
    )
    assert result.feasible is True


def test_feasible_when_ample_lead_time():
    result = evaluate_schedulability(
        _submission(deadline=NOW + timedelta(hours=1), estimated_duration=timedelta(minutes=5)),
        now=NOW,
    )
    assert result.feasible is True


def test_feasible_at_exact_boundary_where_available_equals_duration():
    result = evaluate_schedulability(
        _submission(deadline=NOW + timedelta(minutes=5), estimated_duration=timedelta(minutes=5)),
        now=NOW,
    )
    assert result.feasible is True


def test_infeasible_when_duration_exceeds_available_lead_time():
    result = evaluate_schedulability(
        _submission(deadline=NOW + timedelta(minutes=5), estimated_duration=timedelta(minutes=10)),
        now=NOW,
    )
    assert result.feasible is False


def test_infeasible_when_not_before_is_after_deadline():
    result = evaluate_schedulability(
        _submission(
            deadline=NOW + timedelta(hours=1),
            not_before=NOW + timedelta(hours=2),
            estimated_duration=timedelta(minutes=5),
        ),
        now=NOW,
    )
    assert result.feasible is False


def test_feasible_when_not_before_leaves_enough_room():
    result = evaluate_schedulability(
        _submission(
            deadline=NOW + timedelta(hours=2),
            not_before=NOW + timedelta(hours=1),
            estimated_duration=timedelta(minutes=30),
        ),
        now=NOW,
    )
    assert result.feasible is True


def test_infeasible_when_not_before_leaves_too_little_room():
    result = evaluate_schedulability(
        _submission(
            deadline=NOW + timedelta(hours=1, minutes=5),
            not_before=NOW + timedelta(hours=1),
            estimated_duration=timedelta(minutes=30),
        ),
        now=NOW,
    )
    assert result.feasible is False
