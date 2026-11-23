"""A worker's own, conservative view of whether it may still execute jobs.

The scheduler declares a worker dead -- and retries its RUNNING jobs on
another worker -- once the worker's `last_heartbeat_at` is older than the
heartbeat TTL. A worker that cannot reach PostgreSQL cannot know whether
that has happened, so it must stop executing *before* it could have:

    deadline = start of the last successful heartbeat + fence_after
    fence_after < TTL - heartbeat interval   (validated)

The start time is taken before the heartbeat statement runs and the
database stamps `last_heartbeat_at` later, so the worker's deadline is
always earlier than the scheduler's verdict; no clock synchronisation is
needed, only clocks running at the same rate.

When the deadline passes (or the worker learns it was deregistered) the
worker *fences*: the epoch increases and every attempt started under an
older epoch loses its authority for good. A later successful heartbeat
starts a new epoch for new work; it never gives old attempts back their
authority (sticky fencing).

Pure logic with an injectable clock; the worker drives it.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SelfFencingThresholds:
    fence_after: float  # stop executing this long after the last good heartbeat started
    kill_after: float  # fail-stop deadline for handlers that ignore cancellation


def self_fencing_thresholds(
    *, ttl: float, heartbeat_interval: float, fence_after: float | None = None
) -> SelfFencingThresholds:
    """Defaults to fence_after = TTL - max(2 x interval, 20% of TTL), i.e. 20 s for
    the 30 s / 5 s defaults, and kill_after = TTL - interval (25 s). Raises
    ValueError for settings under which self-fencing cannot be safe."""
    if ttl <= 0 or heartbeat_interval <= 0:
        raise ValueError("heartbeat TTL and interval must be positive")
    if fence_after is None:
        fence_after = ttl - max(2 * heartbeat_interval, 0.2 * ttl)
    kill_after = ttl - heartbeat_interval
    if fence_after < 2 * heartbeat_interval:
        raise ValueError(
            f"self-fencing after {fence_after:g}s would fence on one late heartbeat: it must be "
            f">= 2 x WORKER_HEARTBEAT_INTERVAL_SECONDS ({2 * heartbeat_interval:g}s); raise "
            "WORKER_HEARTBEAT_TTL_SECONDS or lower the interval"
        )
    if fence_after >= kill_after:
        raise ValueError(
            f"self-fencing after {fence_after:g}s leaves no time to stop handlers before the "
            f"scheduler may declare the worker dead: it must be < TTL - interval "
            f"({kill_after:g}s)"
        )
    return SelfFencingThresholds(fence_after=fence_after, kill_after=kill_after)


class ExecutionAuthority:
    def __init__(self, *, fence_after: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._fence_after = fence_after
        self._clock = clock
        self._last_ok: float | None = None
        self._epoch = 0
        self._fenced = True  # no authority before the first successful registration
        self.fence_reason: str | None = "not registered yet"

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def fenced(self) -> bool:
        return self._fenced

    @property
    def last_ok(self) -> float | None:
        return self._last_ok

    @property
    def deadline(self) -> float | None:
        return None if self._last_ok is None else self._last_ok + self._fence_after

    def expired(self) -> bool:
        deadline = self.deadline
        return deadline is None or self._clock() >= deadline

    @property
    def is_authorized(self) -> bool:
        """May the worker start new attempts now?"""
        return not self._fenced and not self.expired()

    def is_valid(self, epoch: int) -> bool:
        """Does an attempt started under `epoch` still have authority?"""
        return epoch == self._epoch and self.is_authorized

    def fence(self, reason: str) -> bool:
        """Revoke the authority of every attempt started so far. Returns False
        if already fenced (the epoch only moves once per fence)."""
        if self._fenced:
            return False
        self._fenced = True
        self._epoch += 1
        self.fence_reason = reason
        return True

    def confirm(self, started_at: float) -> None:
        """A heartbeat (or registration) that started at `started_at` succeeded.

        Extends the deadline. If the worker was fenced, a new epoch's authority
        begins -- unless this heartbeat started so long ago that its own
        deadline has passed too. A heartbeat that completes after the current
        deadline first fences (its success says nothing about the gap)."""
        if not self._fenced and self.expired():
            self.fence("heartbeat deadline passed before a heartbeat succeeded")
        if self._last_ok is None or started_at > self._last_ok:
            self._last_ok = started_at
        if self._fenced and not self.expired():
            self._fenced = False
            self.fence_reason = None
