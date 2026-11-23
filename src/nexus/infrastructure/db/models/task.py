from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from nexus.infrastructure.db.base import Base


class TaskModel(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ix_tasks_tenant_created", "tenant_id", "created_at"),
        Index(
            "uq_tasks_tenant_idempotency_key",
            "tenant_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        # Reconciler scans: only non-terminal rows are ever looked at.
        Index(
            "ix_tasks_active_state",
            "state",
            "updated_at",
            postgresql_where=text("state IN ('pending', 'ready', 'dispatched', 'running')"),
        ),
        Index("ix_tasks_depends_on", "depends_on", postgresql_using="gin"),
        CheckConstraint("attempt >= 1", name="ck_tasks_attempt_positive"),
        CheckConstraint("max_attempts >= 1", name="ck_tasks_max_attempts_positive"),
        CheckConstraint("attempt <= max_attempts", name="ck_tasks_attempt_within_max"),
        CheckConstraint("timeout_seconds > 0", name="ck_tasks_timeout_positive"),
        CheckConstraint("version >= 1", name="ck_tasks_version_positive"),
    )

    task_id: Mapped[UUID] = mapped_column(primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)
    job_id: Mapped[UUID] = mapped_column(nullable=False)
    task_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)

    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    initial_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    max_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    not_before: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    estimated_duration_seconds: Mapped[float | None] = mapped_column(Float)
    fair_share_weight: Mapped[float] = mapped_column(Float, nullable=False)
    depends_on: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), nullable=False, server_default=text("'{}'::uuid[]")
    )
    resource_requirements: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    idempotency_key: Mapped[str | None] = mapped_column(String)

    result: Mapped[Any | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    admission_reason: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    ready_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dispatch_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")


class TaskTransitionModel(Base):
    """Append-only audit/replay log; the shape TaskTransitionRecord was
    designed for (see domain/task_state.py)."""

    __tablename__ = "task_transitions"
    __table_args__ = (Index("ix_task_transitions_task_id", "task_id", "id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("tasks.task_id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    from_state: Mapped[str] = mapped_column(String, nullable=False)
    to_state: Mapped[str] = mapped_column(String, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
