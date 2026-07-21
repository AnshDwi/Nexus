from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4


@dataclass(frozen=True, slots=True)
class DomainEvent:
    aggregate_id: str
    aggregate_type: str
    event_type: str
    payload: dict[str, Any]
    event_id: UUID = field(default_factory=uuid4)
    event_version: int = 1
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID = field(default_factory=uuid4)
    causation_id: UUID | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.aggregate_id:
            raise ValueError("DomainEvent.aggregate_id must not be empty")
        if not self.aggregate_type:
            raise ValueError("DomainEvent.aggregate_type must not be empty")
        if not self.event_type:
            raise ValueError("DomainEvent.event_type must not be empty")
