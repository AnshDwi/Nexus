from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, DateTime, Index, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.infrastructure.db.base import Base


class WorkerRegistrationModel(Base):
    __tablename__ = "worker_registrations"
    __table_args__ = (Index("ix_worker_registrations_last_heartbeat_at", "last_heartbeat_at"),)

    worker_id: Mapped[UUID] = mapped_column(primary_key=True)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    capacity_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    draining: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
