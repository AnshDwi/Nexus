from nexus.infrastructure.coordination.observation_window import HeartbeatObservationWindow

TTL, LEASE, TICK = 30.0, 15.0, 5.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _window(clock: FakeClock) -> HeartbeatObservationWindow:
    return HeartbeatObservationWindow(ttl_seconds=TTL, lease_seconds=LEASE, clock=clock)


def _passes(window: HeartbeatObservationWindow, clock: FakeClock, seconds: float) -> None:
    """Steady leader passes every TICK seconds for `seconds`."""
    elapsed = 0.0
    while elapsed < seconds:
        clock.advance(TICK)
        elapsed += TICK
        window.begin_leader_pass(newly_acquired=False)


def test_closed_until_the_first_leader_pass():
    clock = FakeClock()
    window = _window(clock)
    assert window.observed_seconds == 0
    assert not window.allows_dead_worker_sweep
    clock.advance(TTL * 10)
    assert not window.allows_dead_worker_sweep  # time alone opens nothing


def test_sweep_allowed_only_after_a_full_ttl_of_steady_passes():
    clock = FakeClock()
    window = _window(clock)
    window.begin_leader_pass(newly_acquired=True)
    _passes(window, clock, TTL - TICK)
    assert window.observed_seconds == TTL - TICK
    assert not window.allows_dead_worker_sweep
    _passes(window, clock, TICK)
    assert window.allows_dead_worker_sweep


def test_a_failure_restarts_the_window():
    clock = FakeClock()
    window = _window(clock)
    window.begin_leader_pass(newly_acquired=True)
    _passes(window, clock, TTL)
    assert window.allows_dead_worker_sweep

    window.reset("maintenance pass failed")
    assert not window.allows_dead_worker_sweep
    assert window.last_reset_reason == "maintenance pass failed"
    clock.advance(TICK)
    window.begin_leader_pass(newly_acquired=False)
    assert window.observed_seconds == 0
    _passes(window, clock, TTL - TICK)
    assert not window.allows_dead_worker_sweep
    _passes(window, clock, TICK)
    assert window.allows_dead_worker_sweep


def test_newly_acquired_leadership_restarts_the_window():
    clock = FakeClock()
    window = _window(clock)
    window.begin_leader_pass(newly_acquired=True)
    _passes(window, clock, TTL)
    assert window.allows_dead_worker_sweep

    clock.advance(TICK)
    window.begin_leader_pass(newly_acquired=True)
    assert window.last_reset_reason == "leadership acquired"
    assert window.observed_seconds == 0
    assert not window.allows_dead_worker_sweep


def test_a_gap_longer_than_the_lease_restarts_the_window():
    """The lease may have lapsed between passes (e.g. the replica was paused),
    even though no error was raised."""
    clock = FakeClock()
    window = _window(clock)
    window.begin_leader_pass(newly_acquired=True)
    _passes(window, clock, TTL)
    assert window.allows_dead_worker_sweep

    clock.advance(LEASE + 0.1)
    window.begin_leader_pass(newly_acquired=False)
    assert window.last_reset_reason == "leader lease lapsed between passes"
    assert not window.allows_dead_worker_sweep


def test_a_gap_within_the_lease_keeps_the_window():
    clock = FakeClock()
    window = _window(clock)
    window.begin_leader_pass(newly_acquired=True)
    for _ in range(3):
        clock.advance(LEASE)  # slow passes, but the lease never lapsed
        window.begin_leader_pass(newly_acquired=False)
    assert window.observed_seconds == 3 * LEASE
    assert window.allows_dead_worker_sweep


def test_reset_while_closed_is_harmless():
    clock = FakeClock()
    window = _window(clock)
    window.reset("not the leader")
    window.reset("not the leader")
    assert not window.allows_dead_worker_sweep
