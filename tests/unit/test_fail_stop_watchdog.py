import threading

import pytest

from nexus.infrastructure.config import Settings
from nexus.infrastructure.coordination.execution_authority import ExecutionAuthority
from nexus.infrastructure.coordination.fail_stop import EXIT_CODE, FailStopWatchdog
from nexus.infrastructure.messaging.rabbitmq_worker import WorkerConfig

KILL_AFTER = 25.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class Exits:
    def __init__(self) -> None:
        self.codes: list[int] = []
        self.called = threading.Event()

    def __call__(self, code: int) -> None:
        self.codes.append(code)
        self.called.set()


def _watchdog(
    clock: FakeClock, live: list[int], exits: Exits
) -> tuple[FailStopWatchdog, ExecutionAuthority]:
    authority = ExecutionAuthority(fence_after=20, clock=clock)
    watchdog = FailStopWatchdog(
        authority=authority,
        kill_after=KILL_AFTER,
        live_handlers=lambda: live[0],
        exit_fn=exits,
        clock=clock,
        poll_interval=0.01,
    )
    return watchdog, authority


def test_inert_before_the_first_heartbeat():
    clock, live, exits = FakeClock(), [3], Exits()
    watchdog, _ = _watchdog(clock, live, exits)
    clock.now += 1000
    assert not watchdog.should_fire()


@pytest.mark.parametrize(
    ("elapsed", "live", "fires"),
    [
        (KILL_AFTER - 0.1, 1, False),  # not overdue
        (KILL_AFTER, 0, False),  # overdue, but every handler stopped (cooperative fence)
        (KILL_AFTER, 1, True),  # overdue and a handler is still alive
    ],
)
def test_fires_only_when_overdue_and_a_handler_is_alive(elapsed, live, fires):
    clock, exits = FakeClock(), Exits()
    watchdog, authority = _watchdog(clock, [live], exits)
    authority.confirm(clock.now)
    clock.now += elapsed
    assert watchdog.should_fire() is fires


def test_the_thread_exits_through_the_injected_function():
    clock, live, exits = FakeClock(), [1], Exits()
    watchdog, authority = _watchdog(clock, live, exits)
    authority.confirm(clock.now)
    watchdog.start()
    try:
        clock.now += KILL_AFTER
        assert exits.called.wait(2)
        assert exits.codes == [EXIT_CODE] and watchdog.fired
    finally:
        watchdog.stop()


def test_a_stopped_watchdog_never_exits():
    clock, live, exits = FakeClock(), [1], Exits()
    watchdog, authority = _watchdog(clock, live, exits)
    authority.confirm(clock.now)
    watchdog.start()
    watchdog.stop()
    clock.now += KILL_AFTER * 10
    assert not exits.called.wait(0.1)


def test_fail_stop_requires_self_fencing():
    with pytest.raises(ValueError, match="requires self-fencing"):
        WorkerConfig(fail_stop=True)
    assert WorkerConfig(heartbeat_interval=5, heartbeat_ttl=30, fail_stop=True).fail_stop
    assert WorkerConfig().fail_stop is False  # direct constructions never exit


def test_the_daemon_setting_defaults_to_enabled():
    assert Settings().worker_fail_stop is True
