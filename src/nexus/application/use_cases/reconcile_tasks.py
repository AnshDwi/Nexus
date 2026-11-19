"""Periodic safety net for the event-driven execution path.

The normal path never needs it: submission, completion and failure all
write the next dispatch request in the same transaction as the state change,
and the queue redelivers anything a worker did not acknowledge. The
reconciler repairs what can still slip through:

* a worker died and its dispatch message expired or was exhausted before the
  lease lapsed -> RUNNING with an expired lease is failed and retried;
* a dispatch message was lost (relay dead-lettered it, queue retention
  expired while the database was down) -> READY tasks with an old dispatch
  request are re-dispatched;
* a PENDING task whose not_before passed beyond a message's retention, or
  whose dependency completed while the promotion transaction failed ->
  promoted and dispatched;
* a PENDING task stuck behind a dependency that can never succeed ->
  cancelled.

Every repair is guarded by state and attempt, so running the reconciler
concurrently with workers (or with another reconciler) is safe; it only ever
does work the normal path would have done.
"""

from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from nexus.application.interfaces.outbox_writer import OutboxWriter
from nexus.application.interfaces.task_store import TaskStore
from nexus.application.use_cases.task_events import emit_lifecycle_event, request_dispatch
from nexus.application.use_cases.task_lifecycle import cancel_dependents, fail_attempt
from nexus.domain.task_lifecycle import plan_cancel
from nexus.domain.task_readiness import promote_to_ready
from nexus.domain.task_state import TaskState


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    expired_leases: int = 0
    redispatched: int = 0
    promoted: int = 0
    cancelled_blocked: int = 0

    @property
    def total(self) -> int:
        return self.expired_leases + self.redispatched + self.promoted + self.cancelled_blocked


async def reconcile_tasks(
    store: TaskStore,
    outbox: OutboxWriter,
    *,
    batch_size: int,
    redispatch_after: timedelta,
) -> ReconcileReport:
    now = await store.now()

    expired = await store.expired_leases_for_update(now, batch_size)
    for record in expired:
        await fail_attempt(
            store, outbox, record, now=now, reason="lease expired: worker lost or timed out"
        )

    cancelled = 0
    for record in await store.blocked_pending_for_update(batch_size):
        reason = "a dependency can no longer succeed"
        instance, transition = plan_cancel(record.instance, now=now, reason=reason)
        updated = record.with_instance(instance, finished_at=now, last_error=reason)
        await store.append_transitions(updated.task_id, updated.attempt, [transition])
        updated = await store.save(updated)
        await emit_lifecycle_event(outbox, updated, now=now, detail=reason)
        cancelled += 1 + await cancel_dependents(store, outbox, updated, now=now)

    promoted = 0
    for record in await store.due_pending_for_update(now, batch_size):
        completed = frozenset[UUID](record.depends_on)  # query guarantees all SUCCEEDED
        task, transition = promote_to_ready(
            record.to_scheduled_task(), now=now, completed_task_ids=completed
        )
        updated = record.with_instance(task.instance, ready_since=now)
        await store.append_transitions(updated.task_id, updated.attempt, [transition])
        updated = await request_dispatch(outbox, updated, now=now)
        await store.save(updated)
        promoted += 1

    redispatched = 0
    for record in await store.stale_ready_for_update(now - redispatch_after, batch_size):
        if record.state is not TaskState.READY:
            continue
        updated = await request_dispatch(outbox, record, now=now, deliver_after=record.not_before)
        await store.save(updated)
        redispatched += 1

    return ReconcileReport(
        expired_leases=len(expired),
        redispatched=redispatched,
        promoted=promoted,
        cancelled_blocked=cancelled,
    )
