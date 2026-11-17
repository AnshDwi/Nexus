"""One maintenance pass: reconcile tasks, then drain the outbox.

Triggered by the Vercel Cron endpoint and by the scheduler daemon. Passes are
serialized through the existing PostgreSQL leader lease (lease name
`nexus-maintenance`), so overlapping cron invocations and multiple daemon
instances do not duplicate reconciler work. Mutual exclusion here is an
efficiency measure, not a correctness requirement: every reconciler write is
guarded by task state and attempt number, which is what protects against a
paused ("zombie") holder acting after it lost the lease.
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.use_cases.reconcile_tasks import ReconcileReport, reconcile_tasks
from nexus.infrastructure.config import Settings
from nexus.infrastructure.coordination.leader_election import PostgresLeaderElection
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.metrics import (
    outbox_relayed_total,
    reconciler_repairs_total,
)

logger = logging.getLogger("nexus.maintenance")

MAINTENANCE_LEASE = "nexus-maintenance"


@dataclass(frozen=True, slots=True)
class MaintenanceReport:
    leader: bool
    fencing_token: int | None
    reconcile: ReconcileReport
    relayed: int


async def run_maintenance_pass(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher | None,
    settings: Settings,
    *,
    replica_id: UUID,
    trigger: str,
    relay: bool = True,
) -> MaintenanceReport:
    async with session_factory() as session:
        election = PostgresLeaderElection(
            session,
            replica_id=replica_id,
            lease_name=MAINTENANCE_LEASE,
            lease_duration=timedelta(seconds=settings.scheduler_lease_duration_seconds),
        )
        status = await election.try_acquire_or_renew()
        await session.commit()

    if not status.is_leader:
        return MaintenanceReport(
            leader=False, fencing_token=None, reconcile=ReconcileReport(), relayed=0
        )

    async with session_factory() as session:
        report = await reconcile_tasks(
            PostgresTaskStore(session),
            PostgresOutboxWriter(session),
            batch_size=settings.reconciler_batch_size,
            redispatch_after=timedelta(seconds=settings.reconciler_redispatch_after_seconds),
        )
        await session.commit()

    for kind, count in (
        ("expired_lease", report.expired_leases),
        ("redispatch", report.redispatched),
        ("promote", report.promoted),
        ("cancel_blocked", report.cancelled_blocked),
    ):
        if count:
            reconciler_repairs_total.labels(kind=kind).inc(count)

    relayed = 0
    if relay:
        relayed = await relay_pending_events(
            session_factory, publisher, settings, max_events=settings.reconciler_batch_size
        )
    outbox_relayed_total.labels(trigger=trigger).inc(relayed)
    logger.info(
        "maintenance pass",
        extra={
            "trigger": trigger,
            "fencing_token": status.fencing_token,
            "repairs": report.total,
            "relayed": relayed,
        },
    )
    return MaintenanceReport(
        leader=True, fencing_token=status.fencing_token, reconcile=report, relayed=relayed
    )
