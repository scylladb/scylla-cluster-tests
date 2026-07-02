import logging
import os
import sys
from typing import IO


LOGGER = logging.getLogger(__name__)


def clamp_ts_to_milliseconds(ts: float) -> float:
    return round(ts, 3)


if sys.platform == "darwin":
    import fcntl

    _F_FULLFSYNC = fcntl.F_FULLFSYNC

    def _sync_impl(fd: int) -> None:
        # F_FULLFSYNC forces the drive to flush its write cache, giving real
        # platter-level durability. os.fsync on macOS only pushes to the
        # drive cache, which a power loss can still erase.
        fcntl.fcntl(fd, _F_FULLFSYNC)
elif hasattr(os, "fdatasync"):
    def _sync_impl(fd: int) -> None:
        # Linux/BSD: fdatasync skips metadata (mtime) writes, which most
        # callers do not need -- file size changes are already implied by
        # content.
        os.fdatasync(fd)
else:
    def _sync_impl(fd: int) -> None:
        os.fsync(fd)


def durable_sync(f: IO) -> None:
    """Flush ``f`` and force its buffered data to durable storage.

    Flushes user-space buffers (``f.flush()``) then issues the strongest
    portable kernel/drive sync available: ``F_FULLFSYNC`` on macOS,
    ``fdatasync`` on Linux/BSD, ``fsync`` everywhere else.
    """
    f.flush()
    _sync_impl(f.fileno())
