import logging
import time
from dataclasses import replace
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.leader_election import LeaseStatus, classify_transition
from nexus.infrastructure.db.models.leader_lease import LeaderLeaseModel
from nexus.infrastructure.observability.metrics import (
    scheduler_leadership_transitions_total,
    scheduler_lease_acquisition_latency_seconds,
    scheduler_lease_renewal_failures_total,
    scheduler_lease_renewals_total,
)

logger = logging.getLogger("nexus.scheduler.leader_election")


class PostgresLeaderElection:
    """One implementation of the storage-agnostic LeaderElection port.

    Correctness rests entirely on a single atomic INSERT ... ON CONFLICT DO
    UPDATE ... WHERE statement: the WHERE clause only lets an UPDATE through
    if the caller already holds the lease or it has expired, and the row is
    left untouched (and RETURNING yields nothing) otherwise. The fencing
    token increments inside that same atomic statement, so it is strictly
    monotonic by construction -- there is no window where two replicas can
    both believe they hold the same fencing token.

    A preceding, purely informational SELECT determines whether this call
    represents a transition (for metrics/logging only). It is not a
    correctness dependency: leadership truth always comes from the atomic
    statement's result, never from this SELECT, so a race between the two
    can at worst mislabel a metric, never the actual leadership decision.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        replica_id: UUID,
        lease_name: str,
        lease_duration: timedelta,
    ) -> None:
        self._session = session
        self._replica_id = replica_id
        self._lease_name = lease_name
        self._lease_duration = lease_duration

    async def try_acquire_or_renew(self) -> LeaseStatus:
        start = time.perf_counter()
        try:
            # Expiry is computed and compared with the database's clock, never
            # a replica's: replicas with skewed clocks must agree on whether a
            # lease has lapsed, or a fast clock could steal a live lease.
            now = func.statement_timestamp()
            was_leader = await self._was_leader_before(now)
            status = await self._acquire_or_renew(now)
            self._record_outcome(was_leader=was_leader, status=status)
            if status.is_leader and not was_leader:
                status = replace(status, newly_acquired=True)
            return status
        except Exception as exc:
            scheduler_lease_renewal_failures_total.labels(reason=type(exc).__name__).inc()
            logger.error(
                "lease acquire/renew failed",
                extra={"lease_name": self._lease_name, "replica_id": str(self._replica_id)},
                exc_info=exc,
            )
            raise
        finally:
            scheduler_lease_acquisition_latency_seconds.observe(time.perf_counter() - start)

    async def holds_lease(self, fencing_token: int) -> bool:
        """Fencing check for a leader about to write.

        True only if this replica still holds the lease *with this exact
        token* and it has not expired (database clock). The row is locked
        FOR SHARE until the caller's transaction ends, so a competing
        replica cannot take the lease over (which needs an UPDATE) while
        the caller's writes are in flight: the writes either commit under a
        valid lease or the caller aborts. A paused leader that wakes up with
        an old token is therefore rejected here.
        """
        row = (
            await self._session.execute(
                select(LeaderLeaseModel.lease_name)
                .where(
                    LeaderLeaseModel.lease_name == self._lease_name,
                    LeaderLeaseModel.holder_id == self._replica_id,
                    LeaderLeaseModel.fencing_token == fencing_token,
                    LeaderLeaseModel.expires_at > func.statement_timestamp(),
                )
                .with_for_update(read=True)
            )
        ).first()
        return row is not None

    async def _was_leader_before(self, now: ColumnElement[datetime]) -> bool:
        row = (
            await self._session.execute(
                select(LeaderLeaseModel.holder_id, LeaderLeaseModel.expires_at > now).where(
                    LeaderLeaseModel.lease_name == self._lease_name
                )
            )
        ).first()
        if row is None:
            return False
        holder_id, still_valid = row
        return bool(holder_id == self._replica_id and still_valid)

    async def _acquire_or_renew(self, now: ColumnElement[datetime]) -> LeaseStatus:
        new_expiry = now + self._lease_duration
        stmt = (
            pg_insert(LeaderLeaseModel)
            .values(
                lease_name=self._lease_name,
                holder_id=self._replica_id,
                fencing_token=1,
                expires_at=new_expiry,
            )
            .on_conflict_do_update(
                index_elements=[LeaderLeaseModel.lease_name],
                set_={
                    "holder_id": self._replica_id,
                    "fencing_token": LeaderLeaseModel.fencing_token + 1,
                    "expires_at": new_expiry,
                },
                where=or_(
                    LeaderLeaseModel.holder_id == self._replica_id,
                    LeaderLeaseModel.expires_at < now,
                ),
            )
            .returning(
                LeaderLeaseModel.holder_id,
                LeaderLeaseModel.fencing_token,
                LeaderLeaseModel.expires_at,
            )
        )
        row = (await self._session.execute(stmt)).first()
        if row is not None:
            holder_id, fencing_token, expires_at = row
            return LeaseStatus(
                is_leader=True,
                holder_id=holder_id,
                fencing_token=fencing_token,
                expires_at=expires_at,
            )
        return await self._current_state()

    async def _current_state(self) -> LeaseStatus:
        row = (
            await self._session.execute(
                select(
                    LeaderLeaseModel.holder_id,
                    LeaderLeaseModel.fencing_token,
                    LeaderLeaseModel.expires_at,
                ).where(LeaderLeaseModel.lease_name == self._lease_name)
            )
        ).one()
        holder_id, fencing_token, expires_at = row
        return LeaseStatus(
            is_leader=False,
            holder_id=holder_id,
            fencing_token=fencing_token,
            expires_at=expires_at,
        )

    def _record_outcome(self, *, was_leader: bool, status: LeaseStatus) -> None:
        transition = classify_transition(was_leader=was_leader, is_leader=status.is_leader)
        log_fields = {
            "lease_name": self._lease_name,
            "replica_id": str(self._replica_id),
            "holder_id": str(status.holder_id),
            "fencing_token": status.fencing_token,
        }
        if transition == "acquired":
            scheduler_leadership_transitions_total.labels(transition="acquired").inc()
            logger.info("leadership acquired", extra=log_fields)
        elif transition == "lost":
            scheduler_leadership_transitions_total.labels(transition="lost").inc()
            logger.warning("leadership lost", extra=log_fields)
        elif status.is_leader:
            scheduler_lease_renewals_total.inc()
            logger.debug("lease renewed", extra=log_fields)
        else:
            logger.debug("lease held by another replica", extra=log_fields)
