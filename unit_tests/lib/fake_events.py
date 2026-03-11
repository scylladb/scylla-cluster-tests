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
# Copyright (c) 2025 ScyllaDB
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent


@dataclass(frozen=True, slots=True)
class EventSnapshot:
    """Immutable snapshot of an event captured at publish time.

    Events are mutable (e.g. ContinuousEvent changes period_type between
    begin and end), so we must freeze the formatted representations at the
    moment ``publish_event()`` is called — just like the real EventsDevice
    writes to raw_events_log and the real EventsFileLogger writes to
    events.log at receipt time.
    """

    formatted: str  # same format as EventsFileLogger writes to events.log
    json: str  # same format as EventsDevice writes to raw_events_log
    severity: Severity
    save_to_files: bool


class FakeEventsDevice:
    """Synchronous in-memory replacement for EventsDevice + EventsFileLogger.

    No processes, threads, files, ZMQ, or sleeps.  Thread-safe.

    Yielded directly by the ``events`` pytest fixture.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.snapshots: list[EventSnapshot] = []

    # ---- backward-compat aliases (tests access these on the ``events`` fixture) ----

    @property
    def events_main_device(self):
        return self

    def get_events_logger(self):
        return self

    def get_events_counter(self):
        return self

    # ---- formatted events log (in-memory replacement for events.log file) ----

    def get_formatted_event_lines(self) -> list[str]:
        """Return formatted event strings identical to what EventsFileLogger writes to events.log."""
        with self.lock:
            return [snap.formatted for snap in self.snapshots]

    # ---- properties expected by wait_for_n_events ----

    @property
    def events_counter(self) -> int:
        with self.lock:
            return len(self.snapshots)

    def is_alive(self) -> bool:
        return True

    # ---- publish (called by SctEvent.publish via registry lookup) ----

    def publish_event(self, event: SctEvent, timeout=None) -> None:
        # Snapshot the event state NOW, before the caller mutates it further.
        snap = EventSnapshot(
            formatted=event.format_event(),
            json=event.to_json(),
            severity=Severity(event.severity),
            save_to_files=event.save_to_files,
        )
        with self.lock:
            self.snapshots.append(snap)

    # ---- reading API (same interface as EventsFileLogger) ----

    def get_events_by_category(self, limit: int | None = None) -> dict[str, list[str]]:
        """Same return type as EventsFileLogger.get_events_by_category().

        Returns ``{severity_name: [formatted_event_str, ...]}`` built from
        in-memory storage.  Only includes events with ``save_to_files=True``
        (same filter the real file logger applies).
        """
        result: dict[str, list[str]] = {sev.name: [] for sev in Severity}
        with self.lock:
            for snap in self.snapshots:
                if snap.save_to_files:
                    result[snap.severity.name].append(snap.formatted)
        return result

    # ---- introspection helpers ----

    @property
    def published_events(self) -> list[EventSnapshot]:
        """Return snapshots of all published events."""
        with self.lock:
            return list(self.snapshots)

    def get_events_by_severity(self, severity: Severity) -> list[EventSnapshot]:
        return [s for s in self.published_events if s.severity == severity]

    def clear(self) -> None:
        with self.lock:
            self.snapshots.clear()

    # ---- wait helper for async publishers (ycsb, ndbench) ----

    @contextmanager
    def wait_for_n_events(self, subscriber, count: int, timeout: float = 10):
        """Wait for *count* new events after the wrapped block completes."""
        start = subscriber.events_counter
        yield
        target = start + count
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline and subscriber.events_counter < target:
            time.sleep(0.05)
        assert subscriber.events_counter >= target, (
            f"Expected {count} events in {timeout}s, got {subscriber.events_counter - start}"
        )
