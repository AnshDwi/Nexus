from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class GpuCapability:
    model: str
    count: int
    memory_mb: int

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError("GpuCapability.count must be positive")
        if self.memory_mb <= 0:
            raise ValueError("GpuCapability.memory_mb must be positive")


@dataclass(frozen=True, slots=True)
class WorkerCapabilities:
    """Structured worker capabilities, replacing free-form tags.

    `custom` is the deliberate extensibility escape hatch: new capability
    kinds don't require a schema change here, only a new key and (later) a
    matching constraint evaluator on the consumer side.
    """

    cpu_cores: float
    memory_mb: int
    architecture: str
    region: str
    runtime: str
    gpu: GpuCapability | None = None
    custom: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.cpu_cores <= 0:
            raise ValueError("WorkerCapabilities.cpu_cores must be positive")
        if self.memory_mb <= 0:
            raise ValueError("WorkerCapabilities.memory_mb must be positive")
        if not self.architecture:
            raise ValueError("WorkerCapabilities.architecture must not be empty")
        if not self.region:
            raise ValueError("WorkerCapabilities.region must not be empty")
        if not self.runtime:
            raise ValueError("WorkerCapabilities.runtime must not be empty")
