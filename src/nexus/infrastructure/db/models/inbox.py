from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import Mapped, mapped_column

from nexus.infrastructure.db.base import Base


class InboxRecordModel(Base):
    __tablename__ = "inbox_records"

    idempotency_key: Mapped[str] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
