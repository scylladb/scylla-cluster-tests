"""Multiprocessing start method helper for SCT.

This centralizes logic for choosing a safe multiprocessing start method.
We avoid setting the start method inside deeply imported modules to prevent
surprising side effects on import and to allow external embedding code to
opt-in. Call ensure_start_method() as early as possible in program startup
(e.g. at top of sct.py and unit_tests/conftest.py) before any processes
or pools are created.

Environment variables:
  SCT_MP_START_METHOD   Explicit method to use (e.g. 'fork', 'spawn').
                        If unset, we default to 'fork' on POSIX systems.

On non-POSIX platforms we do not override Python's default unless
SCT_MP_START_METHOD is provided.
"""
from __future__ import annotations

import multiprocessing
import os
import logging
from typing import Optional

LOGGER = logging.getLogger(__name__)

_DEF_POSIX_METHOD = "fork"


def ensure_start_method(preferred: Optional[str] = None) -> str:
    """Ensure a deterministic multiprocessing start method early in startup.

    Returns the method actually in effect after this call.
    """
    requested = os.environ.get("SCT_MP_START_METHOD") or preferred

    try:
        current = multiprocessing.get_start_method(allow_none=True)
    except RuntimeError:
        current = None  # Should not normally happen here

    if current:
        if not requested and os.name == 'posix' and current != _DEF_POSIX_METHOD:
            requested = _DEF_POSIX_METHOD
        if requested and requested != current and (os.name == 'posix' and requested == 'fork'):
            try:
                multiprocessing.set_start_method(requested, force=True)
                LOGGER.info("Multiprocessing start method forcibly changed from %s to %s", current, requested)
                return multiprocessing.get_start_method()
            except (RuntimeError, ValueError) as exc:
                LOGGER.warning("Failed to force start method to %s: %s", requested, exc)
        # Already set; nothing to do
        return current

    # Not set yet; choose method
    if not requested:
        if os.name == "posix":
            requested = _DEF_POSIX_METHOD
        else:
            # Let Python decide on non-posix unless user requested one
            requested = current or multiprocessing.get_start_method(allow_none=True) or "spawn"

    try:
        multiprocessing.set_start_method(requested)
        LOGGER.info("Multiprocessing start method set to %s", requested)
    except (RuntimeError, ValueError) as exc:
        LOGGER.warning("Could not set multiprocessing start method to %s: %s", requested, exc)

    try:
        return multiprocessing.get_start_method()
    except RuntimeError:
        return requested  # Fallback


__all__ = ["ensure_start_method"]
