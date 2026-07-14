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

"""Tests for ArgusEventPostman draining its queue on stop() and staying time-bounded."""

import json
import time
import threading
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events import events_processes as events_processes_module
from sdcm.sct_events.base import SctEvent
from sdcm.sct_events.setup import EVENTS_DEVICE_START_DELAY, stop_events_device
from sdcm.sct_events.argus import (
    ArgusEventAggregator,
    ArgusEventCollector,
    SCTArgusEvent,
    get_argus_postman,
    start_argus_pipeline,
    start_argus_postman,
)
from sdcm.sct_events.events_device import start_events_main_device
from sdcm.sct_events.events_processes import EVENTS_ARGUS_AGGREGATOR_ID, EventsProcessesRegistry, get_events_process
from sdcm.test_config import TestConfig
from sdcm.utils.argus import Argus, ReplayOnlyArgusSCTClient


def argus_event(severity: Severity, message: str) -> SCTArgusEvent:
    """Build a minimal Argus event payload as produced by ArgusEventCollector."""
    return SCTArgusEvent(
        {
            "run_id": "test-run",
            "severity": severity.name,
            "ts": time.time(),
            "event_type": "TestEvent",
            "message": message,
        }
    )


@pytest.fixture
def argus_pipeline(tmp_path):
    """Start an isolated Argus pipeline (collector/aggregator/postman) on its own registry.

    The postman is intentionally left not-enabled, so its run() loop parks and only the
    stop()/drain path touches the aggregator's outbound_queue - making the tests deterministic.
    """
    registry = EventsProcessesRegistry(log_dir=tmp_path)
    # Start the main device first so the collector has a valid upstream to consume from.
    start_events_main_device(_registry=registry)
    time.sleep(EVENTS_DEVICE_START_DELAY)
    start_argus_pipeline(_registry=registry)
    postman = get_argus_postman(_registry=registry)
    aggregator = get_events_process(EVENTS_ARGUS_AGGREGATOR_ID, _registry=registry)
    postman._argus_client = MagicMock()
    try:
        yield postman, aggregator
    finally:
        stop_events_device(_registry=registry)


def test_stop_drains_pending_events(argus_pipeline):
    """stop() posts every event still queued in the aggregator before the thread exits."""
    postman, aggregator = argus_pipeline
    submitted = []
    postman._argus_client.submit_event.side_effect = lambda event: submitted.append(event)

    events = [
        argus_event(Severity.WARNING, "warn"),
        argus_event(Severity.ERROR, "err"),
        argus_event(Severity.NORMAL, "info"),
    ]
    for event in events:
        aggregator.outbound_queue.put(event)

    postman.stop(timeout=5)

    assert not postman.is_alive()
    assert submitted == events


def test_stop_is_bounded_when_submit_hangs(argus_pipeline):
    """stop() returns within the caller-provided timeout even when a submit blocks forever."""
    postman, aggregator = argus_pipeline

    release = threading.Event()  # never set -> submit_event blocks until teardown
    postman._argus_client.submit_event.side_effect = lambda event: release.wait()

    aggregator.outbound_queue.put(argus_event(Severity.ERROR, "err"))

    start = time.monotonic()
    postman.stop(timeout=1)
    elapsed = time.monotonic() - start

    try:
        # Generous margin over the timeout=1 above: real thread scheduling under a loaded,
        # parallelized CI run can add noticeable jitter; this only needs to catch stop()
        # ignoring the timeout altogether, not verify it precisely.
        assert elapsed < 10
        postman._argus_client.submit_event.assert_called_once()  # drain was attempted
        assert postman.is_alive()  # the hung submit is still stuck past the timeout
    finally:
        release.set()  # release the leaked daemon worker


def test_stop_without_queued_events_returns_quickly(argus_pipeline):
    """stop() with an empty queue does not block and posts nothing."""
    postman, _ = argus_pipeline

    start = time.monotonic()
    postman.stop(timeout=5)
    elapsed = time.monotonic() - start

    assert elapsed < 2
    assert not postman.is_alive()
    postman._argus_client.submit_event.assert_not_called()


def test_init_argus_client_disabled_wires_up_replay_only_postman(events_function_scope, monkeypatch, tmp_path):
    """With enable_argus=False, init_argus_client must still give the postman a client to
    submit through, instead of leaving _argus_client unset and every event a silent no-op.
    """
    # events_function_scope wires SctEvent._events_processes_registry to a fresh, isolated
    # registry (torn down after the test) so nothing here can leak into another test's
    # events. Reuse that same registry for the argus pipeline instead of hand-rolling a
    # second one, so the TestFrameworkEvent published on init_argus_client()'s fallback
    # path is captured by it too, rather than falling through to real global state.
    registry = SctEvent._events_processes_registry
    # enable_argus_posting()/start_posting_argus_events(), called from inside
    # TestConfig.init_argus_client(), resolve their registry via the process-wide default
    # (see get_default_events_process_registry) rather than accepting one explicitly.
    monkeypatch.setattr(events_processes_module, "_EVENTS_PROCESSES", registry)
    monkeypatch.setattr(Argus, "INSTANCE", None)
    monkeypatch.setattr(Argus, "INIT_DONE", threading.Event())
    monkeypatch.setattr(TestConfig, "_argus_client", None)
    monkeypatch.setattr(TestConfig, "_logdir", str(tmp_path))

    # Register the aggregator without starting it as a thread: its own run() loop reads
    # from the (unstarted, in this test) collector, which would otherwise crash the
    # thread with an AttributeError. We only need its outbound_queue as postman's input.
    aggregator = ArgusEventAggregator(_registry=registry)
    registry._registry_dict[EVENTS_ARGUS_AGGREGATOR_ID] = aggregator  # noqa: SLF001
    start_argus_postman(_registry=registry)
    postman = get_argus_postman(_registry=registry)

    try:
        test_id = str(uuid.uuid4())
        TestConfig.init_argus_client(params={"enable_argus": False}, test_id=test_id)

        assert isinstance(postman._argus_client, ReplayOnlyArgusSCTClient)
        assert postman._argus_client.run_id == test_id
        assert Argus.get().client is postman._argus_client

        aggregator.outbound_queue.put(argus_event(Severity.ERROR, "err"))
        postman.stop(timeout=5)

        replay_log_path = postman._argus_client.replay_log_path
        TestConfig._close_argus_client()  # flush and join the replay-log writer thread

        lines = replay_log_path.read_text().strip().split("\n")
        assert len(lines) == 1
        outcome = json.loads(lines[0])
        assert outcome["endpoint"] == "/sct/$id/event/submit"
        assert outcome["success"] is False  # replay-only: recorded, no real HTTP call made
    finally:
        postman.stop(timeout=5)  # idempotent: already stopped above unless an assertion failed
        TestConfig._close_argus_client()


def test_argus_event_collector_resolves_run_id_from_replay_only_client(events_function_scope, tmp_path, monkeypatch):
    """ArgusEventCollector.run() must resolve run_id via Argus.get().client even when that
    client is replay-only, not just when a real Argus client was successfully created.
    """
    monkeypatch.setattr(Argus, "INSTANCE", None)
    monkeypatch.setattr(Argus, "INIT_DONE", threading.Event())

    replay_client = ReplayOnlyArgusSCTClient(run_id=str(uuid.uuid4()), log_dir=str(tmp_path))
    try:
        Argus.init_global(replay_client)

        # events_function_scope isolates SctEvent._events_processes_registry for this test;
        # reuse it rather than constructing a throwaway registry no other test can leak into.
        collector = ArgusEventCollector(_registry=SctEvent._events_processes_registry)
        fake_event = SimpleNamespace(
            publish_to_argus=True,
            severity=Severity.WARNING,
            timestamp=time.time(),
            duration=None,
            event_id="fake-event-id",
            known_issue=None,
            nemesis_name=None,
            nemesis_status=None,
            node=None,
            received_timestamp=None,
            target_node=None,
        )
        monkeypatch.setattr(collector, "inbound_events", lambda: iter([("FakeEvent", fake_event)]))

        collector.run()

        evt = collector.outbound_queue.get_nowait()
        assert evt["run_id"] == replay_client.run_id
    finally:
        replay_client.close()
