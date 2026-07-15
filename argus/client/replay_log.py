"""JSONL replay log for Argus client API calls.

Every mutating (POST) request is recorded as JSONL lines so that, when Argus
is unavailable, the recorded calls can be replayed against the server once it
recovers. See ``docs/plans/request_replay.md`` for the full design.

After each call the caller invokes :meth:`ReplayLog.write` with the request and
its outcome, appending one JSONL line whose ``success``/``error`` capture how
the call went. A replay/ingest tool re-sends any record whose ``success`` is
not ``True``. This module is a generic sink: what counts as a successful
response is decided by the caller (see ``argus/client/base.py``), not here.

Writes are synchronous, serialized by a single lock -- there is no background
thread or queue, so a failing write can never grow unbounded memory and a slow
disk just adds latency to the calling request instead of losing data silently.
A write or shutdown failure is always logged and swallowed rather than raised:
logging must never be the reason the real Argus API call doesn't happen, and
never the reason a caller's cleanup path raises.
"""
from __future__ import annotations

import atexit
import functools
import itertools
import json
import logging
import os
import re
import threading
import time
import weakref
from pathlib import Path
from typing import IO

_instance_counter = itertools.count()

LOGGER = logging.getLogger(__name__)

# Allow only characters that are unambiguously safe inside a filename.
_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")


def _sanitize_for_filename(value: str) -> str:
    # Strip any path-significant characters (slashes, dots) so a hostile run-id
    # cannot escape ``log_dir``. Dots are dropped entirely rather than mapped
    # to ``_`` so ``..`` cannot survive the substitution.
    return _UNSAFE_FILENAME_CHARS.sub("_", value) or "unknown"


def _now_ns() -> int:
    return time.time_ns()


class ReplayLogOnlyResponse:
    """Stub :class:`requests.Response` returned in replay-log-only mode.

    Satisfies :meth:`ArgusClient.check_response` so callers continue without
    error. The real request is preserved in the replay log for later replay.
    """

    status_code = 200
    ok = True

    def __init__(self, endpoint: str) -> None:
        self.url = f"replay-log-only:{endpoint}"
        self.request = None
        self.text = '{"status":"ok","response":{}}'
        self.content = self.text.encode("utf-8")

    def json(self) -> dict:
        return {"status": "ok", "response": {}}

    def raise_for_status(self) -> None:
        return None


class ReplayLog:
    """Append-only JSONL journal of Argus API calls for one client instance.

    Writes are synchronous and serialized by a single lock -- the calling
    thread pays for its own write, there is no background thread, queue, or
    unbounded buffer that could grow if the disk is unwritable.

    If the log file cannot be opened (bad ``log_dir``, permission error, full
    disk), the instance still constructs successfully and simply drops every
    record -- a broken replay log must never prevent the real Argus client
    from being created or from making its real HTTP calls.
    """

    def __init__(
        self,
        *,
        log_dir: str | Path,
        run_id: str | None = None,
        test_type: str | None = None,
    ) -> None:
        safe_run_id = _sanitize_for_filename(run_id or "unknown")
        log_dir_path = Path(log_dir)
        # Nanosecond clock + pid + process-wide counter guarantees uniqueness
        # across parallel processes and back-to-back instantiation, even when
        # the system clock has coarser-than-nanosecond resolution.
        suffix = f"{_now_ns()}_{os.getpid()}_{next(_instance_counter)}"
        self._path: Path = log_dir_path / f"argus_replay_log_{safe_run_id}_{suffix}.jsonl"
        self._test_type: str = test_type or "unknown"
        self._lock = threading.Lock()
        self._file: IO[str] | None = None
        try:
            log_dir_path.mkdir(parents=True, exist_ok=True)
            self._file = open(self._path, "a", encoding="utf-8")
        except OSError:
            LOGGER.exception(
                "argus replay log: could not open %s for writing; replay log disabled", self._path,
            )
        self._atexit_ref: weakref.ReferenceType[ReplayLog] = weakref.ref(self)
        self._atexit_callback = functools.partial(self._atexit_close, self._atexit_ref)
        atexit.register(self._atexit_callback)

    @staticmethod
    def _atexit_close(log_ref: "weakref.ReferenceType[ReplayLog]") -> None:
        log = log_ref()
        if log is not None:
            log.close()

    @property
    def path(self) -> Path:
        return self._path

    def __enter__(self) -> "ReplayLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _line(item: dict) -> str:
        # Compact separators (no spaces) -- ~10-15% smaller than the default.
        return json.dumps(item, default=str, separators=(",", ":"), ensure_ascii=False) + "\n"

    def write(
        self,
        method: str,
        endpoint: str,
        location_params: dict | None,
        params: dict | None,
        body: dict | None,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Write one JSONL record for a completed (or failed) call.

        ``success`` / ``error`` are the caller's verdict on the response (see
        ``_evaluate_response`` in ``argus/client/base.py``). The record is
        dropped and logged -- never raised -- if the log failed to open or is
        already closed, so recording can never block or fail a real request.
        """
        item = {
            "method": method,
            "endpoint": endpoint,
            "location_params": location_params,
            "params": params,
            "body": body,
            "test_type": self._test_type,
            "ts": _now_ns() // 1_000_000,
            "success": success,
        }
        # Omit ``error`` when there is none to keep records compact.
        if error is not None:
            item["error"] = error
        try:
            # json.dumps walks the whole method/endpoint/params/body structure
            # and can raise (e.g. RecursionError on a circular body) -- so the
            # serialization must be inside this guard.
            line = self._line(item)
        except Exception:
            LOGGER.exception("argus replay log: failed to serialize record; record dropped")
            return
        with self._lock:
            if self._file is None:
                # Already logged loudly once, in __init__, when the file
                # failed to open -- avoid re-warning on every record.
                LOGGER.debug(
                    "argus replay log: log unavailable; dropping record for %s %s",
                    method, endpoint,
                )
                return
            if self._file.closed:
                LOGGER.warning(
                    "argus replay log: log already closed; dropping record for %s %s",
                    method, endpoint,
                )
                return
            try:
                # write + flush only: fsync/fdatasync was measured (100
                # concurrent threads) to add no durability benefit here and
                # cost real latency on every request -- see PR discussion.
                self._file.write(line)
                self._file.flush()
            except Exception:
                # A write failure must never propagate into the caller's
                # request handling; the record for this one call is lost,
                # but nothing here can leak memory since there is no queue.
                LOGGER.exception("argus replay log: write failed; record dropped")

    def close(self) -> None:
        # A write() racing close() may find the file already closed and drop
        # its record (logged there) -- an in-flight request's log entry is not
        # worth blocking shutdown to drain.
        with self._lock:
            if self._file is not None and not self._file.closed:
                try:
                    self._file.close()
                except OSError:
                    LOGGER.exception("argus replay log: error closing %s", self._path)
        atexit.unregister(self._atexit_callback)
