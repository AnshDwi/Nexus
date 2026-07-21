from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.events import DomainEvent
from nexus.infrastructure.db.models.outbox import OutboxRecordModel


class PostgresOutboxWriter:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: DomainEvent) -> None:
        self._session.add(
            OutboxRecordModel(
                event_id=event.event_id,
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                event_type=event.event_type,
                event_version=event.event_version,
                occurred_at=event.occurred_at,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                payload=event.payload,
                event_metadata=event.metadata,
            )
        )


class PostgresOutboxRelayStore:
    def __init__(
        self,
        session: AsyncSession,
        *,
        max_attempts: int,
        initial_backoff_seconds: float,
        max_backoff_seconds: float,
    ) -> None:
        self._session = session
        self._max_attempts = max_attempts
        self._initial_backoff_seconds = initial_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds

    def _backoff_seconds(self, attempts: int) -> float:
        growth = float(2 ** (attempts - 1))
        return min(self._initial_backoff_seconds * growth, self._max_backoff_seconds)

    async def claim_next(self) -> DomainEvent | None:
        now = datetime.now(UTC)
        stmt = (
            select(OutboxRecordModel)
            .where(
                OutboxRecordModel.dispatched_at.is_(None),
                OutboxRecordModel.failed_permanently_at.is_(None),
                OutboxRecordModel.available_at <= now,
            )
            .order_by(OutboxRecordModel.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        record = (await self._session.execute(stmt)).scalar_one_or_none()
        if record is None:
            return None
        return DomainEvent(
            aggregate_id=record.aggregate_id,
            aggregate_type=record.aggregate_type,
            event_type=record.event_type,
            payload=record.payload,
            event_id=record.event_id,
            event_version=record.event_version,
            occurred_at=record.occurred_at,
            correlation_id=record.correlation_id,
            causation_id=record.causation_id,
            metadata=record.event_metadata,
        )

    async def mark_dispatched(self, event_id: UUID) -> None:
        await self._session.execute(
            update(OutboxRecordModel)
            .where(OutboxRecordModel.event_id == event_id)
            .values(dispatched_at=datetime.now(UTC))
        )

    async def record_failure(self, event_id: UUID, error: str) -> None:
        record = await self._session.get(OutboxRecordModel, event_id)
        if record is None:
            return

        attempts = record.attempts + 1
        values: dict[str, Any] = {"attempts": attempts, "last_error": error}
        if attempts >= self._max_attempts:
            values["failed_permanently_at"] = datetime.now(UTC)
        else:
            values["available_at"] = datetime.now(UTC) + timedelta(
                seconds=self._backoff_seconds(attempts)
            )

        await self._session.execute(
            update(OutboxRecordModel).where(OutboxRecordModel.event_id == event_id).values(**values)
        )
