from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("IdempotencyKey must not be empty")
