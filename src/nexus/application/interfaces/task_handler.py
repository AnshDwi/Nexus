from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class TaskContext:
    task_id: UUID
    tenant_id: str
    attempt: int
    max_attempts: int
    deadline: datetime | None

    @property
    def idempotency_key(self) -> str:
        """Stable across retries of the same task. Execution is
        at-least-once, so handlers with external side effects should pass
        this to the systems they call."""
        return str(self.task_id)


class PermanentTaskError(Exception):
    """Raised by a handler for failures that retrying cannot fix (bad input,
    4xx from a downstream API). The task goes straight to PERMANENTLY_FAILED."""


class TaskHandler(Protocol):
    async def __call__(self, payload: dict[str, Any], context: TaskContext) -> Any: ...


class TaskHandlerRegistry(Protocol):
    def get(self, task_type: str) -> TaskHandler | None: ...

    def task_types(self) -> frozenset[str]: ...
