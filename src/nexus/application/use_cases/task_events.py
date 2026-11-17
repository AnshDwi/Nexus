"""Outbox events emitted by the task lifecycle.

`task.dispatch_requested` is the work-carrying message: the relay routes it to
the dispatch topic and the executor consumes it. Every other task event is a
notification for downstream consumers (audit, webhooks, dashboards).
"""

import dataclasses
from datetime import datetime
from typing import Any

from nexus.application.interfaces.outbox_writer import OutboxWriter
from nexus.application.interfaces.task_store import TaskRecord
from nexus.domain.events import DomainEvent

TASK_AGGREGATE = "task"
DISPATCH_EVENT_TYPE = "dispatch_requested"
DELIVER_AFTER_METADATA_KEY = "deliver_after"


async def request_dispatch(
    outbox: OutboxWriter,
    record: TaskRecord,
    *,
    now: datetime,
    deliver_after: datetime | None = None,
) -> TaskRecord:
    """Writes a dispatch request for the task's *current* attempt and returns
    the record with dispatch_requested_at set (caller saves it). The message
    carries the attempt number, which is what makes a stale or duplicated
    delivery detectable at claim time."""
    metadata: dict[str, Any] = {"tenant_id": record.tenant_id}
    if deliver_after is not None and deliver_after > now:
        metadata[DELIVER_AFTER_METADATA_KEY] = deliver_after.isoformat()
    await outbox.add(
        DomainEvent(
            aggregate_id=str(record.task_id),
            aggregate_type=TASK_AGGREGATE,
            event_type=DISPATCH_EVENT_TYPE,
            payload={"task_id": str(record.task_id), "attempt": record.attempt},
            occurred_at=now,
            metadata=metadata,
        )
    )
    return dataclasses.replace(record, dispatch_requested_at=now)


async def emit_lifecycle_event(
    outbox: OutboxWriter, record: TaskRecord, *, now: datetime, detail: str | None = None
) -> None:
    await outbox.add(
        DomainEvent(
            aggregate_id=str(record.task_id),
            aggregate_type=TASK_AGGREGATE,
            event_type=record.state.value,
            payload={
                "task_id": str(record.task_id),
                "tenant_id": record.tenant_id,
                "job_id": str(record.job_id),
                "state": record.state.value,
                "attempt": record.attempt,
                "detail": detail,
            },
            occurred_at=now,
            metadata={"tenant_id": record.tenant_id},
        )
    )
