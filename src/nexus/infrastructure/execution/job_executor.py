"""Executes one job dispatch message. Transport-agnostic: the RabbitMQ
worker maps the returned Disposition to ack / reject-to-DLQ / requeue.

    1. claim   (tx)  inbox record for the message + QUEUED -> RUNNING,
                     execution row, lease = timeout + grace
    2. execute       registered handler under asyncio timeout, in its own
                     task; no DB connection is held while user code runs
    3. report  (tx)  COMPLETED, or FAILED -> RETRYING (+ outbox dispatch
                     after backoff) / FAILED (dead-lettered); fenced by
                     attempt + worker_id

Reporting is retried with backoff until it commits, the database rejects it
(the attempt was recovered or cancelled meanwhile), or the attempt's lease
deadline passes. Retrying is always safe: every report is a compare-and-set
on (state RUNNING, attempt, worker_id) under the job's row lock, so a late
report either records a result nobody else has acted on, or is rejected.

Self-fencing (when the worker gives the executor an ExecutionAuthority):
* no claim is attempted without authority, and an attempt whose authority
  was lost while claiming never starts its handler;
* when the worker fences, it cancels the handlers of attempts from older
  epochs (`cancel_handlers`); only the handler is cancelled, never a report
  in progress;
* an outcome is reported only if the handler finished while its attempt
  still had authority. Otherwise the attempt is *handed back*: recorded as
  `lost` (and retried per the job's policy) as soon as the database
  accepts it -- a compare-and-set as well, so a no-op if the scheduler
  already recovered it. Results produced within authority are still
  recorded after the authority lapses (Option A): the compare-and-set alone
  decides whether they are still wanted.
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable
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
from nexus.infrastructure.coordination.execution_authority import ExecutionAuthority
from nexus.infrastructure.db.repositories.inbox_repository import PostgresInboxRepository
from nexus.infrastructure.db.repositories.job_repository import PostgresJobStore
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.observability.logging import correlation_id_var
from nexus.infrastructure.observability.metrics import (
    job_executions_total,
    worker_handbacks_total,
)

logger = logging.getLogger("nexus.jobs.executor")
_MAX_ERROR = 4000
_REPORT_INITIAL_DELAY = 0.5
_REPORT_MAX_DELAY = 5.0
SHUTDOWN_REASON = "worker shut down before completion"
FENCED_REASON = "worker lost execution authority (self-fenced); attempt handed back"
STRANDED_REASON = "claim committed but its outcome was lost; no handler ran (stranded claim)"

AttemptKey = tuple[UUID, int]


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


@dataclass
class _Attempt:
    """An attempt this executor is claiming or running."""

    epoch: int | None
    handler_task: "asyncio.Task[Any] | None" = None
    cancel_reason: str | None = None
    within_authority: bool = True  # stamped when the handler finishes


class JobExecutor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: TaskHandlerRegistry,
        *,
        worker_id: UUID,
        lease_grace: timedelta,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._session_factory = session_factory
        self._handlers = handlers
        self._worker_id = worker_id
        self._lease_grace = lease_grace
        self._clock = clock
        # Set by a worker that self-fences; None keeps the original behaviour.
        self.authority: ExecutionAuthority | None = None
        self._attempts: dict[AttemptKey, _Attempt] = {}
        self._handbacks: dict[AttemptKey, str] = {}
        # Plain int, written only on the event loop: safe for the fail-stop
        # thread to read while the loop is blocked.
        self._live_handlers = 0

    @property
    def worker_id(self) -> UUID:
        return self._worker_id

    @property
    def pending_handbacks(self) -> int:
        return len(self._handbacks)

    @property
    def live_handlers(self) -> int:
        """Handlers started and not yet finished (thread-safe to read)."""
        return self._live_handlers

    def _handler_done(self, _: "asyncio.Task[Any]") -> None:
        self._live_handlers -= 1

    async def handle(self, message: JobMessage) -> Disposition:
        if not message.message_id:
            logger.error("job message without message_id", extra={"job_id": str(message.job_id)})
            return Disposition.DEAD_LETTER
        authority = self.authority
        if authority is not None and not authority.is_authorized:
            return Disposition.REQUEUE  # fenced: claim nothing until authority returns
        key: AttemptKey = (message.job_id, message.attempt)
        owned = key not in self._attempts
        if owned:
            self._attempts[key] = _Attempt(epoch=authority.epoch if authority else None)
        try:
            return await self._handle(message, key, owned=owned)
        finally:
            if owned:
                self._attempts.pop(key, None)

    async def _handle(self, message: JobMessage, key: AttemptKey, *, owned: bool) -> Disposition:
        # Taken before the claim: the database computes the lease from its own
        # (later) clock, so claim_started + timeout + grace never exceeds it.
        claim_started = self._clock()
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
        if decision is ClaimDecision.DUPLICATE and owned:
            # No other delivery of this attempt is in flight here. If the job
            # is nevertheless RUNNING under this worker, an earlier claim of
            # this message committed although we saw an error (ambiguous
            # commit) and no handler ever ran: hand it back for retry.
            try:
                stranded = await self._is_stranded(message)
            except Exception:
                logger.exception("could not check for a stranded claim; redelivering")
                return Disposition.REQUEUE
            if stranded:
                return await self._hand_back(key, log, STRANDED_REASON)
        if decision is not ClaimDecision.RUN or job is None:
            logger.info("job message skipped", extra={**log, "reason": decision.value})
            return Disposition.ACK

        state = self._attempts[key]
        if self.authority is not None and not self.authority.is_valid(state.epoch or 0):
            # Authority was lost while the claim was in flight: never start.
            return await self._hand_back(key, log, "authority lost while claiming")

        token = correlation_id_var.set(job.correlation_id)
        try:
            report_deadline = (
                claim_started + job.timeout_seconds + self._lease_grace.total_seconds()
            )
            return await self._execute(job, state, log, report_deadline)
        finally:
            correlation_id_var.reset(token)

    async def _is_stranded(self, message: JobMessage) -> bool:
        async with self._session_factory() as session:
            job = await PostgresJobStore(session).get(message.job_id)
        return (
            job is not None
            and job.state is JobState.RUNNING
            and job.attempt == message.attempt
            and job.worker_id == self._worker_id
        )

    def _stamp_authority(self, state: _Attempt) -> Callable[["asyncio.Task[Any]"], None]:
        def stamp(_: "asyncio.Task[Any]") -> None:
            if self.authority is not None:
                state.within_authority = self.authority.is_valid(state.epoch or 0)

        return stamp

    async def _execute(
        self, job: Job, state: _Attempt, log: dict[str, Any], report_deadline: float
    ) -> Disposition:
        logger.info("job started", extra={**log, "job_type": job.job_type})
        handler = self._handlers.get(job.job_type)
        authority, epoch = self.authority, state.epoch or 0
        context = TaskContext(
            task_id=job.job_id,
            tenant_id=job.tenant_id,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            deadline=None,
            authority_check=(lambda: authority.is_valid(epoch)) if authority else None,
        )
        started = time.perf_counter()

        async def run() -> Any:
            if handler is None:
                raise PermanentTaskError(f"no handler registered for '{job.job_type}'")
            return await asyncio.wait_for(handler(job.payload, context), job.timeout_seconds)

        handler_task = asyncio.get_running_loop().create_task(run())
        state.handler_task = handler_task
        self._live_handlers += 1
        handler_task.add_done_callback(self._handler_done)
        handler_task.add_done_callback(self._stamp_authority(state))
        try:
            await asyncio.wait({handler_task})
        except asyncio.CancelledError:
            handler_task.cancel()  # this message's task itself is being cancelled
            raise
        if not state.within_authority:
            return await self._hand_back((job.job_id, job.attempt), log, FENCED_REASON)

        error: str | None = None
        status = "failed"
        permanent = False
        result: Any = None
        try:
            result = handler_task.result()
            json.dumps(result)
        except asyncio.CancelledError:  # the handler (only) was cancelled at shutdown
            error, status = state.cancel_reason or SHUTDOWN_REASON, "lost"
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
            job,
            result=result,
            error=error,
            permanent=permanent,
            status=status,
            deadline=report_deadline,
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

    # --- self-fencing support -------------------------------------------------

    def cancel_handlers(self, reason: str, *, stale_only: bool) -> int:
        """Cancel running handlers -- only the handler, never a report in
        progress. `stale_only`: just those of attempts from an older epoch
        than the authority's current one (the worker has fenced)."""
        current = self.authority.epoch if self.authority is not None else None
        cancelled = 0
        for state in self._attempts.values():
            task = state.handler_task
            if task is None or task.done():
                continue
            if stale_only and state.epoch == current:
                continue
            state.cancel_reason = reason
            task.cancel()
            cancelled += 1
        return cancelled

    async def _hand_back(self, key: AttemptKey, log: dict[str, Any], reason: str) -> Disposition:
        """Give an attempt this worker may no longer finish back to the
        scheduler's retry path. The message is acknowledged: the retry is a
        new outbox dispatch, and a redelivery of this one is an inbox no-op."""
        logger.warning("attempt handed back", extra={**log, "reason": reason})
        self._handbacks[key] = reason
        await self.flush_handbacks()
        return Disposition.ACK

    async def flush_handbacks(self) -> int:
        """Record pending hand-backs as `lost` attempts. Stops at the first
        database error (they stay pending for the next try). Returns how many
        were settled."""
        settled = 0
        for (job_id, attempt), reason in list(self._handbacks.items()):
            try:
                async with self._session_factory() as session:
                    recorded = await report_failure(
                        PostgresJobStore(session),
                        PostgresOutboxWriter(session),
                        job_id=job_id,
                        attempt=attempt,
                        worker_id=self._worker_id,
                        error=reason,
                        execution_status="lost",
                    )
                    await session.commit()
            except Exception:
                logger.info(
                    "hand-back not recorded yet; will retry",
                    extra={"job_id": str(job_id), "attempt": attempt},
                )
                break
            self._handbacks.pop((job_id, attempt), None)
            settled += 1
            worker_handbacks_total.labels(
                result="recorded" if recorded is not None else "already_recovered"
            ).inc()
        return settled

    async def _report(
        self,
        job: Job,
        *,
        result: Any,
        error: str | None,
        permanent: bool,
        status: str,
        deadline: float,
    ) -> Job | None:
        """Record the outcome, retrying until it commits or the database
        rejects it, but not past `deadline` (the attempt's lease: after it the
        scheduler recovers the job anyway). Returns the recorded job, or None
        if the attempt is no longer live (fenced) or the deadline passed."""
        delay = _REPORT_INITIAL_DELAY
        tries = 0
        maybe_committed = False  # an earlier try failed while committing
        while True:
            tries += 1
            committing = False
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
                    committing = True
                    await session.commit()
                    if recorded is None and maybe_committed:
                        recorded = await self._read_back(job, succeeded=error is None)
                    if tries > 1:
                        logger.info(
                            "job result recorded after retrying",
                            extra={"job_id": str(job.job_id), "tries": tries},
                        )
                    return recorded
            except Exception:
                maybe_committed = maybe_committed or committing
                remaining = deadline - self._clock()
                if remaining <= 0:
                    logger.exception(
                        "gave up recording job result at the attempt's lease deadline; "
                        "the scheduler will recover the job",
                        extra={"job_id": str(job.job_id), "tries": tries},
                    )
                    return None
                logger.warning(
                    "could not record job result yet; retrying",
                    extra={"job_id": str(job.job_id), "tries": tries},
                    exc_info=True,
                )
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, _REPORT_MAX_DELAY)

    async def _read_back(self, job: Job, *, succeeded: bool) -> Job | None:
        """After a commit whose outcome was unknown, the retry found the
        attempt no longer live: report what is there if it is consistent with
        our own outcome having been recorded (the scheduler may also have
        recovered the attempt; both leave the job in a valid state)."""
        async with self._session_factory() as session:
            current = await PostgresJobStore(session).get(job.job_id)
        if current is None:
            return None
        if succeeded:
            done = current.state is JobState.COMPLETED and current.attempt == job.attempt
            return current if done else None
        if current.state is JobState.FAILED and current.attempt == job.attempt:
            return current
        if current.attempt == job.attempt + 1:
            return current
        return None

    async def abandon(self, job_id: UUID, attempt: int, reason: str) -> None:
        """Graceful shutdown, last resort: hand an unfinished attempt back for
        retry. If the database is unreachable it stays a pending hand-back."""
        self._handbacks[(job_id, attempt)] = reason
        await self.flush_handbacks()
