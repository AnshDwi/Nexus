from collections.abc import Callable
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
    # Set by a self-fencing worker: True while this attempt may still act.
    authority_check: Callable[[], bool] | None = None

    def check_authority(self) -> None:
        """Call right before an external side effect. Raises
        AuthorityLostError if the worker has lost its authority for this
        attempt (another worker may already be running it). A no-op where
        the runtime does not track authority."""
        if self.authority_check is not None and not self.authority_check():
            raise AuthorityLostError(f"attempt {self.attempt} lost its execution authority")

    @property
    def idempotency_key(self) -> str:
        """Stable across retries of the same task. Execution is
        at-least-once, so handlers with external side effects should pass
        this to the systems they call."""
        return str(self.task_id)


class AuthorityLostError(Exception):
    """Raised by TaskContext.check_authority: the attempt must stop without
    further side effects. The worker hands the attempt back for retry."""


class PermanentTaskError(Exception):
    """Raised by a handler for failures that retrying cannot fix (bad input,
    4xx from a downstream API). The task goes straight to PERMANENTLY_FAILED."""


class TaskHandler(Protocol):
    async def __call__(self, payload: dict[str, Any], context: TaskContext) -> Any: ...


class TaskHandlerRegistry(Protocol):
    def get(self, task_type: str) -> TaskHandler | None: ...

    def task_types(self) -> frozenset[str]: ...
