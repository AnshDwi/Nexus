from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.idempotency import IdempotencyKey
from nexus.infrastructure.db.models.inbox import InboxRecordModel


class PostgresInboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def mark_processed_if_new(self, key: IdempotencyKey) -> bool:
        stmt = (
            insert(InboxRecordModel)
            .values(idempotency_key=key.value)
            .on_conflict_do_nothing(index_elements=[InboxRecordModel.idempotency_key])
            .returning(InboxRecordModel.idempotency_key)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
