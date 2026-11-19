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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from nexus.infrastructure.db.base import Base


class JobModel(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_tenant_created", "tenant_id", "created_at"),
        Index("ix_jobs_tenant_state", "tenant_id", "state"),
        Index(
            "uq_jobs_tenant_idempotency_key",
            "tenant_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index(
            "ix_jobs_active",
            "state",
            "updated_at",
            postgresql_where=text("state IN ('pending', 'queued', 'running', 'retrying')"),
        ),
        Index("ix_jobs_worker_running", "worker_id", postgresql_where=text("state = 'running'")),
        CheckConstraint("attempt >= 1", name="ck_jobs_attempt_positive"),
        CheckConstraint("max_attempts >= 1", name="ck_jobs_max_attempts_positive"),
        CheckConstraint("retry_limit >= 1", name="ck_jobs_retry_limit_positive"),
        CheckConstraint("timeout_seconds > 0", name="ck_jobs_timeout_positive"),
        CheckConstraint(
            "priority IN ('low', 'normal', 'high', 'critical')", name="ck_jobs_priority"
        ),
        CheckConstraint(
            "state IN ('pending', 'queued', 'running', 'completed', 'failed', 'retrying', "
            "'cancelled')",
            name="ck_jobs_state",
        ),
    )

    job_id: Mapped[UUID] = mapped_column(primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    priority: Mapped[str] = mapped_column(String(16), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    initial_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    max_backoff_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(200))
    correlation_id: Mapped[str | None] = mapped_column(String(200))
    result: Mapped[Any | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    worker_id: Mapped[UUID | None] = mapped_column()
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dispatch_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")


class JobTransitionModel(Base):
    __tablename__ = "job_transitions"
    __table_args__ = (
        Index("ix_job_transitions_job", "job_id", "id"),
        Index("ix_job_transitions_occurred", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.job_id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    from_state: Mapped[str] = mapped_column(String(16), nullable=False)
    to_state: Mapped[str] = mapped_column(String(16), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)


class JobExecutionModel(Base):
    """One row per attempt actually started by a worker."""

    __tablename__ = "job_executions"
    __table_args__ = (
        Index("uq_job_executions_attempt", "job_id", "attempt", unique=True),
        Index("ix_job_executions_started", "started_at"),
        CheckConstraint(
            "status IN ('running', 'completed', 'failed', 'timed_out', 'lost', 'cancelled')",
            name="ck_job_executions_status",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(
        ForeignKey("jobs.job_id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column()
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
