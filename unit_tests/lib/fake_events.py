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
import json
import time
import threading
import unittest.mock
from contextlib import contextmanager
from dataclasses import dataclass

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, SystemEvent
from sdcm.sct_events.events_processes import (
    EVENTS_MAIN_DEVICE_ID,
    EVENTS_FILE_LOGGER_ID,
    EventsProcessesRegistry,
)


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
    attrs: dict  # parsed JSON dict — same keys as EventsDevice raw_events_log
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
        # The real EventsDevice.outbound_events() skips SystemEvent instances
        # (filters, etc.) before forwarding to EventsFileLogger.  Replicate
        # that behaviour so event counts match the real pipeline.
        if isinstance(event, SystemEvent):
            return

        # Snapshot the event state NOW, before the caller mutates it further.
        snap = EventSnapshot(
            formatted=event.format_event(),
            attrs=json.loads(event.to_json()),
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

        When *limit* is set, returns first *limit* events for CRITICAL and
        last *limit* events for other severities (same behaviour as the real
        file logger).
        """
        result: dict[str, list[str]] = {sev.name: [] for sev in Severity}
        with self.lock:
            for snap in self.snapshots:
                if snap.save_to_files:
                    result[snap.severity.name].append(snap.formatted)
        if limit is not None:
            for name, value in result.items():
                if name == Severity.CRITICAL.name:
                    result[name] = value[:limit]
                else:
                    result[name] = value[-limit:] if value else []
        return result

    # ---- introspection helpers ----

    @property
    def published_events(self) -> list[dict]:
        """Return attribute dicts of all published events.

        Each dict has the same keys as the JSON written to ``raw_events_log``
        by the real ``EventsDevice`` (e.g. ``"type"``, ``"severity"``,
        ``"line_number"``, ``"message"``, etc.).
        """
        with self.lock:
            return [snap.attrs for snap in self.snapshots]

    def get_event_summary(self) -> dict[str, int]:
        """Return ``{severity_name: count}`` — same format as ``summary.log``.

        The real ``summary.log`` counts ALL events forwarded by EventsDevice
        (SystemEvent instances are already filtered out in ``publish_event``),
        regardless of ``save_to_files``.
        """
        summary: dict[str, int] = {}
        with self.lock:
            for snap in self.snapshots:
                name = snap.severity.name
                summary[name] = summary.get(name, 0) + 1
        return summary

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


class FakeEventsMixin:
    """Mixin that wires a ``FakeEventsDevice`` into the events registry.

    Drop-in replacement for ``EventsUtilsMixin`` in tests that only need to
    *capture* events, not exercise the real multiprocessing infrastructure.

    Provides:
    - ``cls.events``  — a ``FakeEventsDevice`` instance

    Uses ``make_fake_events()`` internally to avoid duplicating the wiring
    logic.

    Works with both ``unittest.TestCase`` subclasses and plain pytest classes.
    Pytest always calls ``setup_class``/``teardown_class`` even on TestCase
    subclasses, so those are the primary hooks.  ``setUpClass``/``tearDownClass``
    are provided for compatibility with the plain ``unittest`` runner (which
    ignores ``setup_class``).  No guard flag is needed because pytest never
    calls both.

    Subclasses that override these hooks must call ``super()`` to keep the
    events infrastructure alive.
    """

    events: FakeEventsDevice | None = None
    _fake_events_cm = None

    @classmethod
    def _setup_fake_events(cls):
        cls._fake_events_cm = make_fake_events()
        cls.events = cls._fake_events_cm.__enter__()

    @classmethod
    def _teardown_fake_events(cls):
        if cls._fake_events_cm:
            cls._fake_events_cm.__exit__(None, None, None)
            cls._fake_events_cm = None

    # -- pytest hooks (always called by pytest, even for TestCase subclasses) --

    @classmethod
    def setup_class(cls):
        cls._setup_fake_events()

    @classmethod
    def teardown_class(cls):
        cls._teardown_fake_events()

    # -- unittest hooks (only called by the plain unittest runner) --

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_fake_events()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_fake_events()
        super().tearDownClass()


@contextmanager
def make_fake_events():
    """Create a ``FakeEventsDevice``, wire it into a fresh registry, and patch ``SctEvent``.

    Yields the device.  Used by the conftest ``events`` / ``events_function_scope``
    fixtures, by tests that override ``fixture_event_system`` (e.g. test_tester,
    test_longevity), and internally by ``FakeEventsMixin``.

    Also patches ``get_logger_event_summary`` (which normally reads summary.log
    from disk) to compute the summary from the in-memory ``FakeEventsDevice``
    instead.  This lets the real ``ClusterTester.get_event_summary()`` work
    without any override.
    """
    device = FakeEventsDevice()
    registry = EventsProcessesRegistry(log_dir="/dev/null")
    registry._registry_dict[EVENTS_MAIN_DEVICE_ID] = device
    registry._registry_dict[EVENTS_FILE_LOGGER_ID] = device

    def _fake_event_summary(_registry=None):
        target = _registry._registry_dict[EVENTS_FILE_LOGGER_ID] if _registry else device
        return target.get_event_summary()

    with (
        unittest.mock.patch("sdcm.sct_events.base.SctEvent._events_processes_registry", registry),
        unittest.mock.patch("sdcm.tester.get_logger_event_summary", side_effect=_fake_event_summary),
    ):
        yield device
