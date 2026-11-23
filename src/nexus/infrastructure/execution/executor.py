"""Runs one dispatched task attempt. Transport-agnostic: the Vercel Queues
subscriber (push mode), a poll-mode daemon and tests all call
`TaskExecutor.handle_dispatch`.

Transaction layout (each block commits before the next starts):

  1. claim        READY -> DISPATCHED -> RUNNING, lease set      (short tx)
  2. execute      handler runs with the task's timeout           (no tx held)
  3. record       RUNNING -> SUCCEEDED | FAILED -> READY/...     (short tx)
  4. eager relay  publish the follow-up outbox rows              (best effort)

No database connection is held while user code runs.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from nexus.application.interfaces.event_publisher import EventPublisher
from nexus.application.interfaces.task_handler import (
    PermanentTaskError,
    TaskContext,
    TaskHandlerRegistry,
)
from nexus.application.use_cases.task_lifecycle import (
    ClaimOutcome,
    claim_task,
    record_failure,
    record_success,
)
from nexus.infrastructure.config import Settings
from nexus.infrastructure.db.repositories.outbox_repository import PostgresOutboxWriter
from nexus.infrastructure.db.repositories.task_repository import PostgresTaskStore
from nexus.infrastructure.messaging.relay import relay_pending_events
from nexus.infrastructure.observability.metrics import (
    task_claims_total,
    task_execution_seconds,
    task_executions_total,
)

logger = logging.getLogger("nexus.executor")

_MAX_ERROR_LENGTH = 4000


class RetryLater(Exception):  # noqa: N818 - a control-flow signal, not an error
    """The delivery arrived too early (not_before, or another worker's lease
    is still live). The transport should redeliver after `delay`."""

    def __init__(self, delay: timedelta) -> None:
        super().__init__(f"retry after {delay.total_seconds():.0f}s")
        self.delay = delay


@dataclass(frozen=True, slots=True)
class DispatchMessage:
    task_id: UUID
    attempt: int

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "DispatchMessage":
        return cls(task_id=UUID(str(payload["task_id"])), attempt=int(payload["attempt"]))


class TaskExecutor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: TaskHandlerRegistry,
        publisher: EventPublisher | None,
        settings: Settings,
        *,
        engine: AsyncEngine | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._handlers = handlers
        self._publisher = publisher
        self._settings = settings
        self._engine = engine

    async def dispose(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    async def handle_dispatch(self, message: DispatchMessage) -> ClaimOutcome:
        async with self._session_factory() as session:
            claim = await claim_task(
                PostgresTaskStore(session),
                PostgresOutboxWriter(session),
                task_id=message.task_id,
                attempt=message.attempt,
                lease_grace=timedelta(seconds=self._settings.task_lease_grace_seconds),
            )
            await session.commit()
        task_claims_total.labels(outcome=claim.outcome.value).inc()
        log = {"task_id": str(message.task_id), "attempt": message.attempt}
        logger.info("dispatch claim", extra={**log, "outcome": claim.outcome.value})

        if claim.outcome is ClaimOutcome.DEFERRED and claim.retry_after is not None:
            raise RetryLater(claim.retry_after)
        if claim.outcome is ClaimOutcome.LEASE_EXPIRED:
            await self._relay()
        if claim.outcome is not ClaimOutcome.CLAIMED or claim.task is None:
            return claim.outcome

        task = claim.task
        context = TaskContext(
            task_id=task.task_id,
            tenant_id=task.tenant_id,
            attempt=task.attempt,
            max_attempts=task.max_attempts,
            deadline=task.deadline,
        )
        handler = self._handlers.get(task.task_type)
        started = time.perf_counter()
        error: str | None = None
        permanent = False
        result: Any = None
        try:
            if handler is None:
                raise PermanentTaskError(f"no handler registered for '{task.task_type}'")
            result = await asyncio.wait_for(
                handler(task.payload, context), timeout=task.timeout_seconds
            )
            json.dumps(result)  # results are stored as JSONB
        except TimeoutError:
            error = f"timed out after {task.timeout_seconds}s"
        except PermanentTaskError as exc:
            error, permanent = f"permanent failure: {exc}", True
        except (TypeError, ValueError) as exc:
            error, permanent = f"{type(exc).__name__}: {exc}", True
        except Exception as exc:  # noqa: BLE001 - any handler failure is a task failure
            error = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - started
        outcome = "succeeded" if error is None else ("permanent_failure" if permanent else "failed")
        task_executions_total.labels(task_type=task.task_type, outcome=outcome).inc()
        task_execution_seconds.labels(task_type=task.task_type).observe(elapsed)

        async with self._session_factory() as session:
            store, outbox = PostgresTaskStore(session), PostgresOutboxWriter(session)
            if error is None:
                recorded = await record_success(
                    store, outbox, task_id=task.task_id, attempt=task.attempt, result=result
                )
            else:
                recorded = await record_failure(
                    store,
                    outbox,
                    task_id=task.task_id,
                    attempt=task.attempt,
                    error=error[:_MAX_ERROR_LENGTH],
                    permanent=permanent,
                )
            await session.commit()

        logger.info(
            "task attempt finished",
            extra={
                **log,
                "outcome": outcome,
                "duration_s": round(elapsed, 3),
                "recorded_state": recorded.state.value if recorded else "discarded",
                "error": error,
            },
        )
        await self._relay()
        return ClaimOutcome.CLAIMED

    async def _relay(self) -> None:
        await relay_pending_events(
            self._session_factory,
            self._publisher,
            self._settings,
            max_events=self._settings.outbox_eager_relay_max_events,
        )
