"""JSONL replay log for Argus client API calls.

Every mutating (POST) request is recorded as a single JSONL line so that, when
Argus is unavailable, the recorded calls can be replayed against the server
once it recovers. See ``docs/plans/request_replay.md`` for the full design.

Each request thread builds a :class:`ReplayRecord` in memory, runs the HTTP
call inside the :meth:`ReplayLog.record` context manager, and the record is
enqueued onto a :class:`queue.Queue` on exit. A single background writer
thread drains the queue and writes to disk -- request threads never touch the
file.
"""
from __future__ import annotations

import atexit
import itertools
import json
import logging
import os
import queue
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import IO, Any, Iterator

from argus.common.utils import durable_sync

_instance_counter = itertools.count()

LOGGER = logging.getLogger(__name__)


class _CloseSentinel:
    """Singleton signal placed on the queue to stop the writer thread."""


_CLOSE = _CloseSentinel()

# Allow only characters that are unambiguously safe inside a filename.
_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9_-]")


def _sanitize_for_filename(value: str) -> str:
    # Strip any path-significant characters (slashes, dots) so a hostile run-id
    # cannot escape ``log_dir``. Dots are dropped entirely rather than mapped
    # to ``_`` so ``..`` cannot survive the substitution.
    return _UNSAFE_FILENAME_CHARS.sub("_", value) or "unknown"


def _now_ns() -> int:
    return time.time_ns()


@dataclass
class ReplayRecord:
    """One Argus API call. Populated during the HTTP call, written on exit."""

    method: str
    endpoint: str
    location_params: dict | None
    params: dict | None
    body: dict | None
    test_type: str
    ts: int = field(default_factory=lambda: _now_ns() // 1_000_000)
    success: bool = False
    error: str | None = None

    def record(self, response: Any) -> None:
        """Populate ``success`` / ``error`` from a ``requests.Response``.

        Mirrors :meth:`ArgusClient.check_response` discriminator: only a
        2xx with a JSON body and ``status == "ok"`` counts as success.
        A 2xx HTML page (auth proxy, gateway error) or
        ``{"status": "error", ...}`` is a failure.
        """
        if not 199 < response.status_code < 300:
            self.error = f"HTTP {response.status_code}"
            return
        try:
            payload = response.json()
        except ValueError:
            self.error = f"HTTP {response.status_code} non-JSON response"
            return
        if payload.get("status") == "ok":
            self.success = True
        else:
            self.error = f"HTTP {response.status_code} status={payload.get('status')!r}"

    def to_dict(self) -> dict:
        d = asdict(self)
        # Omit ``error`` when there is no error to keep records compact.
        if d["error"] is None:
            del d["error"]
        return d


class ReplayLogOnlyResponse:
    """Stub :class:`requests.Response` returned in replay-log-only mode.

    Satisfies :meth:`ArgusClient.check_response` so callers continue without
    error. The real request is preserved in the replay log for later replay.
    """

    status_code = 200

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

    The writer runs on a daemon background thread. On normal interpreter
    shutdown, :meth:`close` is invoked via :mod:`atexit` to drain any pending
    records. Each batch is fsynced (``F_FULLFSYNC`` on macOS, ``fdatasync``
    on Linux) so already-written records survive SIGKILL and power loss; only
    records still sitting in the in-memory queue are lost on a hard kill.
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
        log_dir_path.mkdir(parents=True, exist_ok=True)
        # Nanosecond clock + pid + process-wide counter guarantees uniqueness
        # across parallel processes and back-to-back instantiation, even when
        # the system clock has coarser-than-nanosecond resolution.
        suffix = f"{_now_ns()}_{os.getpid()}_{next(_instance_counter)}"
        self._path: Path = log_dir_path / f"argus_replay_log_{safe_run_id}_{suffix}.jsonl"
        self._test_type: str = test_type or "unknown"
        self._queue: queue.Queue = queue.Queue()
        self._closed = threading.Event()
        self._enqueue_lock = threading.Lock()
        self._writer_thread = threading.Thread(
            target=self._writer_loop, daemon=True, name="argus-replay-log-writer",
        )
        self._writer_thread.start()
        atexit.register(self.close)

    @property
    def path(self) -> Path:
        return self._path

    def __enter__(self) -> "ReplayLog":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _write_line(f: IO[str], item: dict) -> None:
        # Compact separators (no spaces) -- ~10-15% smaller than the default.
        f.write(json.dumps(item, default=str, separators=(",", ":"), ensure_ascii=False))
        f.write("\n")

    def _writer_loop(self) -> None:
        def _sync(f: IO[str]) -> None:
            try:
                durable_sync(f)
            except OSError:
                # Best-effort durability: keep the log running even if the
                # underlying fs (e.g. some network mounts) rejects fsync.
                LOGGER.exception("argus replay log: durable sync failed")

        try:
            with open(self._path, "a", encoding="utf-8") as f:
                while True:
                    item = self._queue.get()
                    if item is _CLOSE:
                        _sync(f)
                        return
                    self._write_line(f, item)
                    # Drain whatever else is already waiting before syncing,
                    # to amortize fsync cost across bursts of requests.
                    for _ in range(63):
                        try:
                            item = self._queue.get_nowait()
                        except queue.Empty:
                            break
                        if item is _CLOSE:
                            _sync(f)
                            return
                        self._write_line(f, item)
                    _sync(f)
        except Exception:
            # The writer thread must never escape -- losing the replay log is
            # bad, crashing the test run is worse.
            LOGGER.exception("argus replay log writer crashed; replay log abandoned")

    @contextmanager
    def record(
        self,
        method: str,
        endpoint: str,
        location_params: dict | None,
        params: dict | None,
        body: dict | None,
    ) -> Iterator[ReplayRecord]:
        """Yield a :class:`ReplayRecord` for the caller to populate.

        The caller runs the HTTP call inside the ``with`` block and sets
        ``rec.success`` based on the response. If the block raises, ``success``
        stays ``False`` and ``error`` captures the exception message. The
        record is enqueued on exit either way.
        """
        rec = ReplayRecord(
            method=method,
            endpoint=endpoint,
            location_params=location_params,
            params=params,
            body=body,
            test_type=self._test_type,
        )
        try:
            yield rec
        except Exception as exc:
            rec.success = False
            rec.error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            with self._enqueue_lock:
                if not self._closed.is_set():
                    self._queue.put(rec.to_dict())

    def close(self, timeout: float = 5.0) -> None:
        with self._enqueue_lock:
            if self._closed.is_set():
                return
            self._closed.set()
            self._queue.put(_CLOSE)
        self._writer_thread.join(timeout=timeout)
        atexit.unregister(self.close)
