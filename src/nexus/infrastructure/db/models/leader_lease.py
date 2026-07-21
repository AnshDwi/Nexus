from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from nexus.infrastructure.db.base import Base


class LeaderLeaseModel(Base):
    __tablename__ = "scheduler_leader_lease"

    lease_name: Mapped[str] = mapped_column(primary_key=True)
    holder_id: Mapped[UUID] = mapped_column(nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
