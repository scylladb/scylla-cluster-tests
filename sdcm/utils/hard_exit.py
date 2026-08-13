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
timeout.  If one of those workers is permanently stuck (e.g. a nested
``ThreadPoolExecutor`` spun up internally by a nemesis, whose worker thread
ignored its own termination signal), the whole process hangs forever, even
after ``sys.exit()`` has been called. Nemesis threads themselves are daemon
threads (see ``start_nemesis()`` in ``sdcm/cluster.py``) and are *not* what
blocks shutdown -- interpreter shutdown never joins daemon threads -- so
callers must check for a live non-daemon ``ThreadPoolExecutor`` worker (e.g.
via ``concurrent.futures.thread._threads_queues``), not a nemesis thread's own
``is_alive()``.

This module has no sdcm imports (leaf module, avoids import cycles): a
caller that detects a thread it cannot safely kill calls ``request_hard_exit``
with a reason and the implicated thread(s), and whichever real process-exit
point runs last calls ``exit_process`` instead of ``sys.exit`` directly. If
nothing armed it, behavior is unchanged; if it was armed, ``exit_process``
re-checks whether any implicated thread is still alive -- a worker thread
stuck during, say, ``stop_nemesis()`` can finish naturally during the
remaining cleanup steps that run before the eventual ``exit_process`` call,
and forcing a hard exit at that point would just be a false positive
overriding the real test pass/fail result. Only if at least one implicated
thread is still alive does ``os._exit()`` bypass the untimed shutdown join;
otherwise this falls through to normal ``sys.exit`` behavior, as if never
armed.
"""

import logging
import os
import sys
import threading

LOGGER = logging.getLogger(__name__)

STUCK_THREAD_EXIT_CODE = 86

_hard_exit_reason: str | None = None
_hard_exit_threads: list[threading.Thread] = []
# Guards the read-modify-write merge in request_hard_exit() below: multiple call
# sites can arm concurrently from different threads during teardown (e.g. a
# nemesis-monitor thread and a main teardown thread both calling
# request_hard_exit() around the same time), and an unsynchronized merge could lose
# one caller's contribution to another's.
_hard_exit_lock = threading.Lock()


def request_hard_exit(reason: str, threads: list[threading.Thread]) -> None:
    global _hard_exit_reason, _hard_exit_threads  # noqa: PLW0603
    # More than one call site can arm during a single test's teardown (e.g.
    # stop_nemesis() and, later, a ParallelObject used during log collection): merge
    # into the existing tracked threads (deduped by identity, since the same thread
    # object could plausibly be passed twice) rather than replacing them outright.
    # Replacing would let a later call's threads finishing naturally hide an earlier
    # call's thread that is still genuinely stuck, silently defeating exit_process()'s
    # liveness re-check below. Reasons are concatenated for the same reason: nothing
    # from an earlier arm should be lost.
    with _hard_exit_lock:
        seen_ids = {id(thread) for thread in _hard_exit_threads}
        merged_threads = list(_hard_exit_threads)
        for thread in threads:
            if id(thread) not in seen_ids:
                seen_ids.add(id(thread))
                merged_threads.append(thread)
        _hard_exit_threads = merged_threads
        _hard_exit_reason = f"{_hard_exit_reason}; {reason}" if _hard_exit_reason else reason

    # Logged here, at arm-time, rather than in exit_process(): this call site runs in
    # the caller's own thread, not in the shutdown crisis exit_process() may later run
    # in, so it is safe to log from -- unlike exit_process()'s armed path, which must
    # not risk blocking on a logging handler held by the very stuck thread that
    # triggered the hard exit. Logged *after* the state above is assigned, not before:
    # if a stuck thread happens to hold a logging handler's internal lock, this call
    # could itself block, and the arm state must already be set by the time that
    # happens -- otherwise exit_process() would never see it.
    LOGGER.error("Hard exit requested: %s", reason)


def exit_process(exit_code: int) -> None:
    # Guard this read with the same lock request_hard_exit() uses for its
    # read-modify-write merge: _hard_exit_reason and _hard_exit_threads are updated
    # together (but as two separate assignments) under that lock, so an unguarded read
    # here could race a concurrent request_hard_exit() call and observe one field
    # updated but not the other (e.g. the new reason but the old, shorter thread list).
    with _hard_exit_lock:
        hard_exit_reason = _hard_exit_reason
        hard_exit_threads = list(_hard_exit_threads)
    if not hard_exit_reason or not any(thread.is_alive() for thread in hard_exit_threads):
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
