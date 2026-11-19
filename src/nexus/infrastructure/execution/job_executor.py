"""Executes one job dispatch message. Transport-agnostic: the RabbitMQ
worker maps the returned Disposition to ack / reject-to-DLQ / requeue.

    1. claim   (tx)  inbox record for the message + QUEUED -> RUNNING,
                     execution row, lease = timeout + grace
    2. execute       registered handler under asyncio timeout; no DB
                     connection is held while user code runs
    3. report  (tx)  COMPLETED, or FAILED -> RETRYING (+ outbox dispatch
                     after backoff) / FAILED (dead-lettered); fenced by
                     attempt + worker_id
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.application.interfaces.task_handler import (
    PermanentTaskError,
    TaskContext,
    TaskHandlerRegistry,
)
from nexus.application.use_cases.jobs import (
    ClaimDecision,
    claim_job,
    report_failure,
    report_success,
)
from nexus.domain.job import Job, JobState
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.observability.logging import correlation_id_var
from nexus.infrastructure.observability.metrics import job_executions_total

logger = logging.getLogger("nexus.jobs.executor")
_MAX_ERROR = 4000
_REPORT_RETRIES = 3


class Disposition(StrEnum):
    ACK = "ack"  # done (or nothing to do): remove the message
    DEAD_LETTER = "dead_letter"  # poison message or final failure: reject to DLQ
    REQUEUE = "requeue"  # infrastructure error before claiming: deliver again


@dataclass(frozen=True, slots=True)
class JobMessage:
    job_id: UUID
    attempt: int
    message_id: str

    @classmethod
    def parse(cls, body: bytes, message_id: str | None) -> "JobMessage":
        """Accepts the outbox envelope ({"payload": {...}}) or a bare payload."""
        data = json.loads(body)
        payload = data.get("payload", data)
        return cls(
            job_id=UUID(str(payload["job_id"])),
            attempt=int(payload["attempt"]),
            message_id=message_id or str(data.get("event_id") or ""),
        )


class JobExecutor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: TaskHandlerRegistry,
        *,
        worker_id: UUID,
        lease_grace: timedelta,
    ) -> None:
        self._session_factory = session_factory
        self._handlers = handlers
        self._worker_id = worker_id
        self._lease_grace = lease_grace

    @property
    def worker_id(self) -> UUID:
        return self._worker_id

    async def handle(self, message: JobMessage) -> Disposition:
        if not message.message_id:
            logger.error("job message without message_id", extra={"job_id": str(message.job_id)})
            return Disposition.DEAD_LETTER
        try:
            async with self._session_factory() as session:
                decision, job = await claim_job(
                    PostgresJobStore(session),
                    PostgresInboxRepository(session),
                    job_id=message.job_id,
                    attempt=message.attempt,
                    message_id=message.message_id,
                    worker_id=self._worker_id,
                    lease_grace=self._lease_grace,
                )
                await session.commit()
        except Exception:
            logger.exception("job claim failed; message will be redelivered")
            return Disposition.REQUEUE

        log = {"job_id": str(message.job_id), "attempt": message.attempt}
        if decision is not ClaimDecision.RUN or job is None:
            logger.info("job message skipped", extra={**log, "reason": decision.value})
            return Disposition.ACK

        token = correlation_id_var.set(job.correlation_id)
        try:
            return await self._execute(job, log)
        finally:
            correlation_id_var.reset(token)

    async def _execute(self, job: Job, log: dict[str, Any]) -> Disposition:
        logger.info("job started", extra={**log, "job_type": job.job_type})
        handler = self._handlers.get(job.job_type)
        context = TaskContext(
            task_id=job.job_id,
            tenant_id=job.tenant_id,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            deadline=None,
        )
        started = time.perf_counter()
        error: str | None = None
        status = "failed"
        permanent = False
        result: Any = None
        try:
            if handler is None:
                raise PermanentTaskError(f"no handler registered for '{job.job_type}'")
            result = await asyncio.wait_for(handler(job.payload, context), job.timeout_seconds)
            json.dumps(result)
        except TimeoutError:
            error, status = f"timed out after {job.timeout_seconds}s", "timed_out"
        except PermanentTaskError as exc:
            error, permanent = f"{exc}", True
        except (TypeError, ValueError) as exc:
            error, permanent = f"{type(exc).__name__}: {exc}", True
        except Exception as exc:  # noqa: BLE001 - any handler failure is a job failure
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - started

        recorded = await self._report(
            job, result=result, error=error, permanent=permanent, status=status
        )
        outcome = "completed" if error is None else status
        job_executions_total.labels(job_type=job.job_type, outcome=outcome).inc()
        logger.info(
            "job finished",
            extra={
                **log,
                "outcome": outcome,
                "duration_ms": round(elapsed * 1000),
                "state": recorded.state.value if recorded else "fenced",
                "error": error,
            },
        )
        if recorded is not None and recorded.state is JobState.FAILED:
            return Disposition.DEAD_LETTER
        return Disposition.ACK

    async def _report(
        self, job: Job, *, result: Any, error: str | None, permanent: bool, status: str
    ) -> Job | None:
        delay = 0.5
        for attempt in range(1, _REPORT_RETRIES + 1):
            try:
                async with self._session_factory() as session:
                    store = PostgresJobStore(session)
                    if error is None:
                        recorded = await report_success(
                            store,
                            job_id=job.job_id,
                            attempt=job.attempt,
                            worker_id=self._worker_id,
                            result=result,
                        )
                    else:
                        recorded = await report_failure(
                            store,
                            PostgresOutboxWriter(session),
                            job_id=job.job_id,
                            attempt=job.attempt,
                            worker_id=self._worker_id,
                            error=error[:_MAX_ERROR],
                            permanent=permanent,
                            execution_status=status,
                        )
                    await session.commit()
                    return recorded
            except Exception:
                if attempt == _REPORT_RETRIES:
                    # The lease will expire and the scheduler will retry the job.
                    logger.exception(
                        "could not record job result", extra={"job_id": str(job.job_id)}
                    )
                    return None
                await asyncio.sleep(delay)
                delay *= 2
        return None

    async def abandon(self, job_id: UUID, attempt: int, reason: str) -> None:
        """Graceful shutdown: hand an unfinished attempt back for retry."""
        async with self._session_factory() as session:
            await report_failure(
                PostgresJobStore(session),
                PostgresOutboxWriter(session),
                job_id=job_id,
                attempt=attempt,
                worker_id=self._worker_id,
                error=reason,
                execution_status="lost",
            )
            await session.commit()
