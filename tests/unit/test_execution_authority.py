import pytest

from nexus.infrastructure.coordination.execution_authority import (
    ExecutionAuthority,
    self_fencing_thresholds,
)
from nexus.infrastructure.messaging.rabbitmq_worker import WorkerConfig


class FakeClock:
    def __init__(self) -> None:
        self.now = 500.0

    def __call__(self) -> float:
        return self.now


def _authority(clock: FakeClock, fence_after: float = 20) -> ExecutionAuthority:
    return ExecutionAuthority(fence_after=fence_after, clock=clock)


# --- thresholds ---------------------------------------------------------------


def test_default_thresholds_for_production_settings():
    t = self_fencing_thresholds(ttl=30, heartbeat_interval=5)
    assert (t.fence_after, t.kill_after) == (20, 25)


@pytest.mark.parametrize(
    ("ttl", "interval", "fence_after"),
    [(6, 1, 4), (2, 0.5, 1), (30, 5, 20), (60, 5, 48)],
)
def test_defaults_are_valid_for_supported_settings(ttl, interval, fence_after):
    assert self_fencing_thresholds(ttl=ttl, heartbeat_interval=interval).fence_after == fence_after


@pytest.mark.parametrize(
    ("ttl", "interval", "fence_after", "message"),
    [
        (30, 5, 9, "one late heartbeat"),  # < 2 x interval
        (30, 5, 25, "no time to stop handlers"),  # >= TTL - interval
        (10, 5, None, "one late heartbeat"),  # interval too close to the TTL
        (0, 5, None, "positive"),
    ],
)
def test_unsafe_thresholds_are_rejected(ttl, interval, fence_after, message):
    with pytest.raises(ValueError, match=message):
        self_fencing_thresholds(ttl=ttl, heartbeat_interval=interval, fence_after=fence_after)


def test_worker_config_without_ttl_keeps_self_fencing_off():
    assert WorkerConfig().thresholds is None


def test_worker_config_validates_at_construction():
    assert WorkerConfig(heartbeat_interval=5, heartbeat_ttl=30).thresholds is not None
    with pytest.raises(ValueError):
        WorkerConfig(heartbeat_interval=5, heartbeat_ttl=30, fence_after=29)


# --- authority ------------------------------------------------------------------


def test_no_authority_before_the_first_confirmation():
    clock = FakeClock()
    authority = _authority(clock)
    assert authority.fenced and not authority.is_authorized
    authority.confirm(clock.now)
    assert authority.is_authorized and authority.epoch == 0
    assert authority.deadline == clock.now + 20


def test_authority_lapses_at_the_deadline_measured_from_the_heartbeat_start():
    clock = FakeClock()
    authority = _authority(clock)
    started = clock.now
    clock.now += 3  # the heartbeat took 3 s to succeed
    authority.confirm(started)
    clock.now = started + 19.9
    assert authority.is_authorized
    clock.now = started + 20
    assert authority.expired() and not authority.is_authorized


def test_fence_revokes_every_attempt_started_before_it():
    clock = FakeClock()
    authority = _authority(clock)
    authority.confirm(clock.now)
    epoch = authority.epoch
    assert authority.fence("test") is True
    assert authority.epoch == epoch + 1
    assert not authority.is_valid(epoch)
    assert authority.fence("again") is False  # one epoch per fence
    assert authority.epoch == epoch + 1


def test_fencing_is_sticky_a_later_heartbeat_starts_a_new_epoch_only():
    clock = FakeClock()
    authority = _authority(clock)
    authority.confirm(clock.now)
    old = authority.epoch
    authority.fence("partition")
    clock.now += 30
    authority.confirm(clock.now)
    assert authority.is_authorized
    assert authority.is_valid(authority.epoch)
    assert not authority.is_valid(old)


def test_a_heartbeat_completing_after_the_deadline_fences_before_extending():
    clock = FakeClock()
    authority = _authority(clock)
    authority.confirm(clock.now)
    old = authority.epoch
    started = clock.now + 15  # started in time...
    clock.now += 25  # ...but only succeeded after the 20 s deadline
    authority.confirm(started)
    assert authority.epoch == old + 1  # attempts from before the gap lost authority
    assert not authority.is_valid(old)


def test_a_stale_heartbeat_does_not_restore_authority():
    clock = FakeClock()
    authority = _authority(clock)
    authority.confirm(clock.now)
    authority.fence("partition")
    stale_start = clock.now  # a heartbeat that started long ago
    clock.now += 60
    authority.confirm(stale_start)
    assert authority.fenced and not authority.is_authorized
