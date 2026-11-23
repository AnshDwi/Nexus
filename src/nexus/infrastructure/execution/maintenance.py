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
from nexus.application.use_cases.jobs import RecoveryReport, recover_jobs
from nexus.application.use_cases.reconcile_tasks import ReconcileReport, reconcile_tasks
from nexus.infrastructure.config import Settings
from nexus.infrastructure.coordination.leader_election import PostgresLeaderElection
from nexus.infrastructure.coordination.observation_window import HeartbeatObservationWindow
from nexus.infrastructure.coordination.worker_registry import PostgresWorkerRegistry
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.metrics import (
    job_recoveries_total,
    outbox_relayed_total,
    reconciler_repairs_total,
    scheduler_dead_worker_sweeps_deferred_total,
)

logger = logging.getLogger("nexus.maintenance")

MAINTENANCE_LEASE = "nexus-maintenance"


@dataclass(frozen=True, slots=True)
class MaintenanceReport:
    leader: bool
    fencing_token: int | None
    reconcile: ReconcileReport
    relayed: int
    jobs: RecoveryReport = RecoveryReport()
    dead_workers: int = 0
    fenced_out: bool = False
    # The leader skipped the dead-worker sweep this pass: it had not yet
    # watched heartbeats for a full TTL (see observation_window.py).
    dead_worker_sweep_deferred: bool = False


async def _recover_jobs_fenced(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    replica_id: UUID,
    fencing_token: int,
    sweep_dead_workers: bool = True,
) -> tuple[RecoveryReport, int, bool]:
    """Dead-worker detection and job recovery, committed only under the
    fencing token this replica was granted. Returns (report, dead workers,
    fenced_out). With `sweep_dead_workers=False` no worker is treated as
    dead (or forgotten) this pass; expired leases and stale dispatches are
    still recovered."""
    lease_duration = timedelta(seconds=settings.scheduler_lease_duration_seconds)
    ttl = timedelta(seconds=settings.worker_heartbeat_ttl_seconds)
    async with session_factory() as session:
        election = PostgresLeaderElection(
            session,
            replica_id=replica_id,
            lease_name=MAINTENANCE_LEASE,
            lease_duration=lease_duration,
        )
        if not await election.holds_lease(fencing_token):
            await session.rollback()
            logger.warning("fenced out: lease lost before recovery", extra={"token": fencing_token})
            return RecoveryReport(), 0, True
        registry = PostgresWorkerRegistry(session)
        dead = await registry.list_dead_workers(ttl) if sweep_dead_workers else []
        report = await recover_jobs(
            PostgresJobStore(session),
            PostgresOutboxWriter(session),
            dead_worker_ids=[w.worker_id for w in dead],
            redispatch_after=timedelta(seconds=settings.job_redispatch_after_seconds),
            batch_size=settings.reconciler_batch_size,
        )
        # Forget workers that have been silent for a long time and own no
        # running jobs any more (their jobs were just recovered).
        if sweep_dead_workers:
            forget_after = ttl * settings.worker_forget_after_ttls
            for worker in await registry.list_dead_workers(forget_after):
                await registry.deregister(worker.worker_id)
        await session.commit()
    for kind, count in (
        ("dead_worker", report.lost_on_dead_workers),
        ("lease_expired", report.expired_leases),
        ("redispatch", report.redispatched),
    ):
        if count:
            job_recoveries_total.labels(kind=kind).inc(count)
    return report, len(dead), False


async def run_maintenance_pass(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher | None,
    settings: Settings,
    *,
    replica_id: UUID,
    trigger: str,
    relay: bool = True,
    observation: HeartbeatObservationWindow | None = None,
) -> MaintenanceReport:
    """One pass. `observation` is the long-running scheduler's heartbeat
    observation window: when given, the dead-worker sweep waits until this
    replica has led, with working database access, for a full heartbeat TTL,
    and any failure resets it. Without one (the stateless cron endpoint),
    the sweep runs every pass as before."""
    try:
        return await _maintenance_pass(
            session_factory,
            publisher,
            settings,
            replica_id=replica_id,
            trigger=trigger,
            relay=relay,
            observation=observation,
        )
    except Exception:
        if observation is not None:
            observation.reset("maintenance pass failed")
        raise


async def _maintenance_pass(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: EventPublisher | None,
    settings: Settings,
    *,
    replica_id: UUID,
    trigger: str,
    relay: bool,
    observation: HeartbeatObservationWindow | None,
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
        if observation is not None:
            observation.reset("not the leader")
        return MaintenanceReport(
            leader=False, fencing_token=None, reconcile=ReconcileReport(), relayed=0
        )

    sweep_dead_workers = True
    if observation is not None:
        observation.begin_leader_pass(newly_acquired=status.newly_acquired)
        sweep_dead_workers = observation.allows_dead_worker_sweep
        if not sweep_dead_workers:
            scheduler_dead_worker_sweeps_deferred_total.inc()
            logger.info(
                "dead-worker sweep deferred until heartbeats were observable for a full TTL",
                extra={
                    "observed_seconds": round(observation.observed_seconds, 1),
                    "ttl_seconds": settings.worker_heartbeat_ttl_seconds,
                    "since": observation.last_reset_reason,
                },
            )

    jobs_report, dead_workers, fenced_out = await _recover_jobs_fenced(
        session_factory,
        settings,
        replica_id=replica_id,
        fencing_token=status.fencing_token,
        sweep_dead_workers=sweep_dead_workers,
    )
    if fenced_out:
        if observation is not None:
            observation.reset("fenced out")
        return MaintenanceReport(
            leader=False,
            fencing_token=status.fencing_token,
            reconcile=ReconcileReport(),
            relayed=0,
            fenced_out=True,
            dead_worker_sweep_deferred=not sweep_dead_workers,
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
            "job_recoveries": jobs_report.total,
            "dead_workers": dead_workers,
            "relayed": relayed,
        },
    )
    return MaintenanceReport(
        leader=True,
        fencing_token=status.fencing_token,
        reconcile=report,
        relayed=relayed,
        jobs=jobs_report,
        dead_workers=dead_workers,
        dead_worker_sweep_deferred=not sweep_dead_workers,
    )
