"""Last line of self-fencing: stop the process if handlers outlive authority.

Self-fencing cancels handlers when the worker's authority lapses
(`fence_after`). That only works for handlers that yield to the event loop
and honour cancellation. A handler that swallows the cancellation, or code
that blocks the event loop (which also stops the heartbeat and the fencing
watchdog), would keep running after the scheduler may have handed its job
to another worker.

This watchdog runs in its own *thread*, so a blocked event loop cannot stop
it. If the worker's last successful heartbeat started more than
`kill_after` seconds ago (TTL - heartbeat interval: after the fence, before
the scheduler can declare the worker dead) and any handler is still alive,
it logs CRITICAL and exits the process with EXIT_CODE. The container is
restarted by its restart policy; unacknowledged messages return to the
queue and the scheduler recovers the RUNNING jobs once the heartbeat ages
out. Every attempt in the process had already lost its authority, so
exiting kills nothing that was still allowed to run.

Inert until the first successful heartbeat, and stopped by the worker
before it stops heartbeating at shutdown. Tests inject `exit_fn`; the real
default is os._exit.
"""

import contextlib
import logging
import os
import threading
import time
from collections.abc import Callable

from nexus.infrastructure.coordination.execution_authority import ExecutionAuthority

logger = logging.getLogger("nexus.worker.fail_stop")

EXIT_CODE = 70  # EX_SOFTWARE


class FailStopWatchdog:
    def __init__(
        self,
        *,
        authority: ExecutionAuthority,
        kill_after: float,
        live_handlers: Callable[[], int],
        exit_fn: Callable[[int], None] = os._exit,
        clock: Callable[[], float] = time.monotonic,
        poll_interval: float = 0.25,
    ) -> None:
        self._authority = authority
        self._kill_after = kill_after
        self._live_handlers = live_handlers
        self._exit = exit_fn
        self._clock = clock
        self._poll = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.fired = False

    def should_fire(self) -> bool:
        last_ok = self._authority.last_ok
        if last_ok is None:
            return False  # not registered yet
        overdue = self._clock() >= last_ok + self._kill_after
        return overdue and self._live_handlers() > 0

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="nexus-fail-stop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self._poll * 4, 1.0))
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._poll):
            if self.should_fire():
                self.fired = True
                logger.critical(
                    "handlers still running after the worker lost its execution authority; "
                    "exiting so they cannot overlap a retry on another worker",
                    extra={"live_handlers": self._live_handlers(), "exit_code": EXIT_CODE},
                )
                for handler in logging.getLogger().handlers:
                    with contextlib.suppress(Exception):  # best effort before exiting
                        handler.flush()
                self._exit(EXIT_CODE)
                return
