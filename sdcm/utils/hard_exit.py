# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Arm-then-exit escalation for a process stuck at interpreter shutdown.

Normal interpreter shutdown runs ``threading._shutdown()``, which calls
``concurrent.futures.thread._python_exit()``.  That function joins *every*
``ThreadPoolExecutor`` worker thread ever created in the process, with no
timeout.  If one of those workers is permanently stuck (e.g. a nemesis thread
that ignored its termination event), the whole process hangs forever, even
after ``sys.exit()`` has been called.

This module has no sdcm imports (leaf module, avoids import cycles): a
caller that detects a thread it cannot safely kill calls ``request_hard_exit``
with a reason and the implicated thread(s), and whichever real process-exit
point runs last calls ``exit_process`` instead of ``sys.exit`` directly. If
nothing armed it, behavior is unchanged; if it was armed, ``exit_process``
re-checks whether any implicated thread is still alive -- a thread stuck
during, say, ``stop_nemesis()`` can finish naturally during the remaining
cleanup steps that run before the eventual ``exit_process`` call, and forcing
a hard exit at that point would just be a false positive overriding the real
test pass/fail result. Only if at least one implicated thread is still alive
does ``os._exit()`` bypass the untimed shutdown join; otherwise this falls
through to normal ``sys.exit`` behavior, as if never armed.
"""

import logging
import os
import sys
import threading

LOGGER = logging.getLogger(__name__)

STUCK_THREAD_EXIT_CODE = 86

_hard_exit_reason: str | None = None
_hard_exit_threads: list[threading.Thread] = []


def request_hard_exit(reason: str, threads: list[threading.Thread]) -> None:
    global _hard_exit_reason, _hard_exit_threads  # noqa: PLW0603
    # Logged here, at arm-time, rather than in exit_process(): this call site runs in
    # the caller's own thread, not in the shutdown crisis exit_process() may later run
    # in, so it is safe to log from -- unlike exit_process()'s armed path, which must
    # not risk blocking on a logging handler held by the very stuck thread that
    # triggered the hard exit.
    LOGGER.error("Hard exit requested: %s", reason)
    _hard_exit_reason = reason
    _hard_exit_threads = list(threads)


def exit_process(exit_code: int) -> None:
    if not _hard_exit_reason or not any(thread.is_alive() for thread in _hard_exit_threads):
        # Either never armed, or every implicated thread has since finished naturally
        # (e.g. during log collection/clean_resources() that ran between arming and
        # this call) -- the danger that justified arming has passed, so this is a
        # false positive: behave exactly as if never armed.
        sys.exit(exit_code)
        return

    # Deliberately not calling logging.shutdown()/sys.stdout.flush()/sys.stderr.flush()/
    # LOGGER.error() (or any other logging call) here: those can themselves block (not
    # just raise) on locks or I/O held by the very stuck thread that triggered this hard
    # exit, which would defeat the whole point of this function. A try/except cannot
    # protect against a call that blocks rather than raises, so nothing may run here
    # before os._exit() -- it must be the very first statement in this branch.
    os._exit(STUCK_THREAD_EXIT_CODE)
