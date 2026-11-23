from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from nexus.domain.job import (
    InvalidJobTransitionError,
    Job,
    JobNotRetryableError,
    JobPriority,
    JobState,
)

NOW = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
WORKER = uuid4()


def _job(state: JobState = JobState.PENDING, **kw) -> Job:
    base = dict(
        job_id=uuid4(),
        tenant_id="t",
        name="n",
        job_type="echo",
        payload={},
        priority=JobPriority.NORMAL,
        state=state,
        attempt=1,
        max_attempts=3,
        retry_limit=3,
        initial_backoff_seconds=2,
        max_backoff_seconds=60,
        timeout_seconds=30,
        created_at=NOW,
        updated_at=NOW,
    )
    base.update(kw)
    return Job(**base)


def test_happy_path_pending_queued_running_completed():
    job, t1 = _job().mark_queued(now=NOW)
    job, starts = job.start(worker_id=WORKER, now=NOW, lease_grace=timedelta(seconds=10))
    job, t3 = job.complete(result={"ok": 1}, now=NOW)
    assert job.state is JobState.COMPLETED
    assert [(t.from_state, t.to_state) for t in [t1, *starts, t3]] == [
        (JobState.PENDING, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.COMPLETED),
    ]


def test_start_sets_worker_and_lease():
    job, _ = _job(JobState.QUEUED).start(
        worker_id=WORKER, now=NOW, lease_grace=timedelta(seconds=10)
    )
    assert job.worker_id == WORKER
    assert job.lease_expires_at == NOW + timedelta(seconds=40)


def test_start_records_missing_queued_step_after_dispatcher_crash():
    job, transitions = _job(JobState.PENDING).start(
        worker_id=WORKER, now=NOW, lease_grace=timedelta(0)
    )
    assert [t.to_state for t in transitions] == [JobState.QUEUED, JobState.RUNNING]


def test_failure_with_attempts_left_goes_failed_then_retrying_with_backoff():
    job = _job(JobState.RUNNING, attempt=2)
    retrying, transitions = job.fail(error="boom", now=NOW)
    assert [(t.from_state, t.to_state) for t in transitions] == [
        (JobState.RUNNING, JobState.FAILED),
        (JobState.FAILED, JobState.RETRYING),
    ]
    assert retrying.state is JobState.RETRYING
    assert retrying.attempt == 3
    assert retrying.next_attempt_at == NOW + timedelta(seconds=4)  # 2 * 2^(2-1)
    assert retrying.worker_id is None


def test_retrying_returns_to_queued():
    retrying, _ = _job(JobState.RUNNING).fail(error="x", now=NOW)
    queued, t = retrying.mark_queued(now=NOW)
    assert (t.from_state, t.to_state) == (JobState.RETRYING, JobState.QUEUED)


def test_exhausted_attempts_stay_failed_and_dead_lettered():
    job = _job(JobState.RUNNING, attempt=3, next_attempt_at=NOW)  # left over from a retry
    failed, transitions = job.fail(error="boom", now=NOW)
    assert failed.state is JobState.FAILED
    assert failed.dead_lettered_at == NOW
    assert failed.next_attempt_at is None
    assert len(transitions) == 1
    assert "attempts exhausted" in (failed.last_error or "")


def test_permanent_error_skips_retries():
    failed, _ = _job(JobState.RUNNING, attempt=1).fail(error="bad", now=NOW, permanent=True)
    assert failed.state is JobState.FAILED
    assert failed.dead_lettered_at is not None


def test_manual_retry_grants_a_fresh_budget():
    failed, _ = _job(JobState.RUNNING, attempt=3).fail(error="boom", now=NOW)
    retried, t = failed.retry_manually(now=NOW)
    assert (t.from_state, t.to_state) == (JobState.FAILED, JobState.RETRYING)
    assert retried.attempt == 4
    assert retried.max_attempts == 6
    assert retried.dead_lettered_at is None


@pytest.mark.parametrize("state", [JobState.PENDING, JobState.COMPLETED, JobState.RUNNING])
def test_manual_retry_only_for_failed(state):
    with pytest.raises(JobNotRetryableError):
        _job(state).retry_manually(now=NOW)


@pytest.mark.parametrize(
    "state", [JobState.PENDING, JobState.QUEUED, JobState.RETRYING, JobState.RUNNING]
)
def test_cancel_allowed_while_active(state):
    cancelled, _ = _job(state).cancel(now=NOW, reason="user")
    assert cancelled.state is JobState.CANCELLED


@pytest.mark.parametrize("state", [JobState.COMPLETED, JobState.CANCELLED, JobState.FAILED])
def test_cancel_refused_when_final(state):
    with pytest.raises(InvalidJobTransitionError):
        _job(state).cancel(now=NOW, reason="user")


def test_completed_is_terminal():
    with pytest.raises(InvalidJobTransitionError):
        _job(JobState.COMPLETED).mark_queued(now=NOW)


def test_priority_maps_to_broker_priority_order():
    ranks = [p.broker_priority for p in JobPriority]
    assert ranks == sorted(ranks)
    assert JobPriority.CRITICAL.broker_priority == 10
