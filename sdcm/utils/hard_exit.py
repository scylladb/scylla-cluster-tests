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
with a reason, and whichever real process-exit point runs last calls
``exit_process`` instead of ``sys.exit`` directly. If nothing armed it,
behavior is unchanged; if it was armed, ``os._exit()`` bypasses the
untimed shutdown join entirely.
"""

import logging
import os
import sys

LOGGER = logging.getLogger(__name__)

STUCK_THREAD_EXIT_CODE = 86

_hard_exit_reason: str | None = None


def request_hard_exit(reason: str) -> None:
    global _hard_exit_reason  # noqa: PLW0603
    _hard_exit_reason = reason


def exit_process(exit_code: int) -> None:
    if not _hard_exit_reason:
        sys.exit(exit_code)
        return

    try:
        LOGGER.error("Hard exit requested: %s", _hard_exit_reason)
        sys.stdout.flush()
        sys.stderr.flush()
        logging.shutdown()
    except Exception:  # noqa: BLE001
        # Diagnostics/flush are best-effort: os._exit() below must run no
        # matter what, or we fall back into the untimed shutdown join this
        # module exists to bypass.
        pass
    os._exit(STUCK_THREAD_EXIT_CODE)
