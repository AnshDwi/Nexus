"""Store-level guarantees of the task lifecycle against a real PostgreSQL:
claim exclusivity, attempt fencing, lease expiry and concurrent dependency
completion. These are the properties the Vercel Queues pipeline relies on
when a message is delivered more than once."""

import asyncio
from datetime import timedelta

import pytest

from nexus.application.use_cases.admission_controller import AdmissionController
from nexus.application.use_cases.reconcile_tasks import reconcile_tasks
from nexus.application.use_cases.submit_task import SubmitTaskCommand, submit_task
from nexus.application.use_cases.task_lifecycle import (
    ClaimOutcome,
    claim_task,
    record_failure,
    record_success,
)
from nexus.domain.task_state import TaskState
from nexus.domain.task_submission import RetryPolicy
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.execution.handlers import build_registry

pytestmark = pytest.mark.integration

_HANDLERS = build_registry(enable_test_handlers=True, webhook_allowed_hosts=frozenset())
_GRACE = timedelta(seconds=0)


async def _submit(factory, *, depends_on=(), timeout=60, max_attempts=3, backoff=0.0):
    async with factory() as session:
        result = await submit_task(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            _HANDLERS,
            AdmissionController(),
            SubmitTaskCommand(
                tenant_id="t1",
                task_type="echo",
                payload={},
                timeout_seconds=timeout,
                retry_policy=RetryPolicy(
                    max_attempts=max_attempts,
                    initial_backoff_seconds=backoff,
                    max_backoff_seconds=max(backoff, 1.0),
                ),
                depends_on=tuple(depends_on),
            ),
        )
        await session.commit()
    return result.task


async def _claim(factory, task_id, attempt=1, grace=_GRACE):
    async with factory() as session:
        result = await claim_task(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            task_id=task_id,
            attempt=attempt,
            lease_grace=grace,
        )
        await session.commit()
    return result


async def _get(factory, task_id):
    async with factory() as session:
        return await PostgresTaskStore(session).get(task_id)


async def test_concurrent_claims_of_the_same_attempt_admit_exactly_one(task_session_factory):
    task = await _submit(task_session_factory)
    assert task.state is TaskState.READY

    results = await asyncio.gather(*(_claim(task_session_factory, task.task_id) for _ in range(10)))

    outcomes = [r.outcome for r in results]
    assert outcomes.count(ClaimOutcome.CLAIMED) == 1
    # Everyone else saw a live lease and was told to come back later.
    assert outcomes.count(ClaimOutcome.DEFERRED) == 9
    assert all(
        r.retry_after and r.retry_after > timedelta(0)
        for r in results
        if r.outcome is ClaimOutcome.DEFERRED
    )


async def test_stale_attempt_messages_are_ignored(task_session_factory):
    task = await _submit(task_session_factory)
    result = await _claim(task_session_factory, task.task_id, attempt=7)
    assert result.outcome is ClaimOutcome.STALE
    assert (await _get(task_session_factory, task.task_id)).state is TaskState.READY


async def test_expired_lease_fails_attempt_and_fences_the_old_worker(task_session_factory):
    task = await _submit(task_session_factory, timeout=1)
    first = await _claim(task_session_factory, task.task_id)
    assert first.outcome is ClaimOutcome.CLAIMED

    await asyncio.sleep(1.2)  # lease = timeout (1s) + grace (0s)
    # The redelivered message (same attempt) finds the lease lapsed.
    redelivered = await _claim(task_session_factory, task.task_id)
    assert redelivered.outcome is ClaimOutcome.LEASE_EXPIRED
    current = await _get(task_session_factory, task.task_id)
    assert current.state is TaskState.READY
    assert current.attempt == 2
    assert "lease expired" in (current.last_error or "")

    # The original worker finally reports: its attempt is no longer live.
    async with task_session_factory() as session:
        discarded = await record_success(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            task_id=task.task_id,
            attempt=1,
            result={"late": True},
        )
        await session.commit()
    assert discarded is None
    assert (await _get(task_session_factory, task.task_id)).result is None


async def test_reconciler_recovers_a_crashed_worker_without_any_redelivery(task_session_factory):
    task = await _submit(task_session_factory, timeout=1)
    assert (await _claim(task_session_factory, task.task_id)).outcome is ClaimOutcome.CLAIMED
    await asyncio.sleep(1.2)

    async with task_session_factory() as session:
        report = await reconcile_tasks(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            batch_size=10,
            redispatch_after=timedelta(minutes=5),
        )
        await session.commit()

    assert report.expired_leases == 1
    current = await _get(task_session_factory, task.task_id)
    assert (current.state, current.attempt) == (TaskState.READY, 2)
    # And the retry is claimable as attempt 2.
    assert (
        await _claim(task_session_factory, task.task_id, attempt=2)
    ).outcome is ClaimOutcome.CLAIMED


async def test_retry_exhaustion_cancels_dependents_transitively(task_session_factory):
    root = await _submit(task_session_factory, max_attempts=1)
    child = await _submit(task_session_factory, depends_on=[root.task_id])
    grandchild = await _submit(task_session_factory, depends_on=[child.task_id])
    assert child.state is TaskState.PENDING

    await _claim(task_session_factory, root.task_id)
    async with task_session_factory() as session:
        failed = await record_failure(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            task_id=root.task_id,
            attempt=1,
            error="boom",
        )
        await session.commit()

    assert failed is not None and failed.state is TaskState.PERMANENTLY_FAILED
    assert (await _get(task_session_factory, child.task_id)).state is TaskState.CANCELLED
    assert (await _get(task_session_factory, grandchild.task_id)).state is TaskState.CANCELLED


@pytest.mark.parametrize("_round", range(5))
async def test_concurrent_dependency_completion_promotes_dependent_exactly_once(
    task_session_factory, _round
):
    a = await _submit(task_session_factory)
    b = await _submit(task_session_factory)
    c = await _submit(task_session_factory, depends_on=[a.task_id, b.task_id])
    await _claim(task_session_factory, a.task_id)
    await _claim(task_session_factory, b.task_id)

    async def _complete(task_id):
        async with task_session_factory() as session:
            await record_success(
                PostgresTaskStore(session),
                PostgresOutboxWriter(session),
                task_id=task_id,
                attempt=1,
                result=None,
            )
            await session.commit()

    await asyncio.gather(_complete(a.task_id), _complete(b.task_id))

    assert (await _get(task_session_factory, c.task_id)).state is TaskState.READY
    async with task_session_factory() as session:
        transitions = await PostgresTaskStore(session).list_transitions(c.task_id)
    assert [t.to_state for t in transitions].count(TaskState.READY) == 1
