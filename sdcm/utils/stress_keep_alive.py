"""Keep a during-upgrade stress command running until the last node is upgraded.

Cloud-sized stress can end early, so the last nodes are upgraded without load. The first pass
runs as written, so later reads find every row it writes. If it ends early, short timed passes
of the same command follow.
"""

import re
import threading
import time
from collections.abc import Callable

KEEP_ALIVE_PASS_MINUTES = 5

CS_ROW_COUNT = re.compile(r"(?<!\S)n=\d+[a-zA-Z]?(?!\S)")
CS_DURATION = re.compile(r"(?<!\S)duration=\d+[smhd]?(?!\S)")
CS_POPULATION = re.compile(r"(?<!\S)-pop(?!\S)")
YCSB_MAX_TIME = re.compile(r"maxexecutiontime=\d+")


def duration_bound_cmd(stress_cmd: str, minutes: int = KEEP_ALIVE_PASS_MINUTES) -> str | None:
    """Return ``stress_cmd`` limited to ``minutes``, or None for a tool without a duration-bound form."""
    if "cassandra-stress" in stress_cmd:
        if CS_ROW_COUNT.search(stress_cmd) and not CS_POPULATION.search(stress_cmd):
            return None
        for pattern in (CS_ROW_COUNT, CS_DURATION):
            if pattern.search(stress_cmd):
                return pattern.sub(f"duration={minutes}m", stress_cmd, count=1)
        return None

    if stress_cmd.startswith("bin/ycsb"):
        if YCSB_MAX_TIME.search(stress_cmd):
            return YCSB_MAX_TIME.sub(f"maxexecutiontime={minutes * 60}", stress_cmd, count=1)
        return f"{stress_cmd} -p maxexecutiontime={minutes * 60}"

    return None


class StressKeepAlive:
    """Rerun a stress command in timed passes until finish().

    ``first_pass`` is the thread pools of the command as written. ``relaunch`` starts one more
    pass and returns its thread pools; with None, only the first pass is verified.
    """

    def __init__(self, tester, stress_cmd: str, first_pass: list, relaunch: Callable[[str], list] | None):
        self._tester = tester
        self._stress_cmd = stress_cmd
        self._first_pass = first_pass
        self._relaunch = relaunch
        self._next_cmd = duration_bound_cmd(stress_cmd) if relaunch else None
        self._started = time.monotonic()
        self._first_pass_seconds: float | None = None
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread: threading.Thread | None = None
        self.passes = 0
        if relaunch and self._next_cmd is None:
            tester.log.warning("no duration-bound form of this stress command, it is not kept alive: %s", stress_cmd)
        if self._next_cmd:
            self._thread = threading.Thread(target=self._keep_alive, name="stress-keep-alive", daemon=True)
            self._thread.start()

    def _keep_alive(self) -> None:
        try:
            pools = self._first_pass
            while True:
                clean = [self._tester.verify_stress_thread(pool) for pool in pools]  # blocks until the pass ends
                if self._first_pass_seconds is None:
                    self._first_pass_seconds = time.monotonic() - self._started
                if self._stop.is_set() or getattr(self._tester, "teardown_started", False):
                    return
                if not all(clean):
                    self._tester.log.warning("stress pass failed, not relaunched: %s", self._stress_cmd)
                    return
                pools = self._relaunch(self._next_cmd)
                self.passes += 1
        except Exception as exc:  # noqa: BLE001
            self._error = exc

    def stop(self) -> None:
        """Stop relaunching after the current pass, without waiting for it."""
        self._stop.set()

    def finish(self) -> None:
        """Stop relaunching, wait for the current pass and verify it; raise what the helper thread hit."""
        if self._thread is None:
            for pool in self._first_pass:
                self._tester.verify_stress_thread(pool)
            return

        self.stop()
        self._thread.join()
        if self._error is not None:
            raise self._error

        self._tester.actions_log.info(
            f"stress kept alive: the first pass ran {self._first_pass_seconds or 0:.0f} s, "
            f"{self.passes} keep-alive pass(es) of {KEEP_ALIVE_PASS_MINUTES} min followed: {self._stress_cmd[:100]}"
        )


def finish_all(keepers: list[StressKeepAlive]) -> None:
    """Stop every keeper first, then wait for each, so that none relaunches while another is waited for."""
    for keeper in keepers:
        keeper.stop()
    error = None
    for keeper in keepers:
        try:
            keeper.finish()
        except Exception as exc:  # noqa: BLE001
            if error is None:
                error = exc
            else:
                keeper._tester.log.error("stress keep-alive failed too, only the first failure is raised: %r", exc)
    if error is not None:
        raise error
