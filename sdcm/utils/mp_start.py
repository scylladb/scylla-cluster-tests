"""Multiprocessing start method helper for SCT.

This centralizes logic for choosing a safe multiprocessing start method.
We avoid setting the start method inside deeply imported modules to prevent
surprising side effects on import and to allow external embedding code to
opt-in. Call ensure_start_method() as early as possible in program startup
(e.g. at top of sct.py and unit_tests/conftest.py) before any processes
or pools are created.

On non-POSIX platforms we do not override Python's default
"""

import multiprocessing
import os
import logging
from typing import Optional

LOGGER = logging.getLogger(__name__)

_DEF_POSIX_METHOD = "fork"


def ensure_start_method(preferred: Optional[str] = None) -> None:
    """Ensure a deterministic multiprocessing start method early in startup.

    Returns the method actually in effect after this call.
    """
    requested = preferred or _DEF_POSIX_METHOD

    if os.name == 'posix':
        multiprocessing.set_start_method(requested, force=True)
        LOGGER.info("Multiprocessing start method forcibly changed to %s", requested)
    else:
        LOGGER.warning("Multiprocessing start method not changed on non-POSIX platform; using default %s, SCT event system isn't gonna work",
                       multiprocessing.get_start_method())


__all__ = ["ensure_start_method"]
