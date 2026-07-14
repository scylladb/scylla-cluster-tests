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
from sdcm.utils.argus import Argus, ArgusError, ReplayOnlyArgusSCTClient


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


@pytest.fixture
def unwired_argus_postman(events_function_scope, monkeypatch, tmp_path):
    """A real, running ArgusEventPostman + aggregator with no Argus client wired up yet -
    the state right before init_argus_client() runs.

    Reuses the isolated registry events_function_scope already set up on
    SctEvent._events_processes_registry, and points the process-wide default registry
    lookup at it too, since enable_argus_posting()/start_posting_argus_events() (called
    from inside TestConfig.init_argus_client()) resolve via that default rather than
    accepting a registry explicitly.
    """
    registry = SctEvent._events_processes_registry
    monkeypatch.setattr(events_processes_module, "_EVENTS_PROCESSES", registry)
    monkeypatch.setattr(Argus, "INSTANCE", None)
    monkeypatch.setattr(TestConfig, "_argus_client", None)
    monkeypatch.setattr(TestConfig, "_logdir", str(tmp_path))

    # Registered but not started as a thread: nothing in this test publishes real events
    # through the main device, so it's only needed as a queue for postman to read from.
    aggregator = ArgusEventAggregator(_registry=registry)
    registry._registry_dict[EVENTS_ARGUS_AGGREGATOR_ID] = aggregator  # noqa: SLF001
    start_argus_postman(_registry=registry)
    postman = get_argus_postman(_registry=registry)
    try:
        yield postman, aggregator
    finally:
        postman.stop(timeout=5)
        TestConfig._close_argus_client()


def test_init_argus_client_alone_does_not_touch_event_pipeline(unwired_argus_postman):
    """init_argus_client() is also called from SCTConfiguration.update_argus_with_version()
    during config resolution, which only needs a client for a couple of synchronous
    submissions - it must not wire the real-time event pipeline. Only
    start_argus_event_pipeline() (called separately, from the actual start of a test) does.
    """
    postman, _ = unwired_argus_postman

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id=str(uuid.uuid4()))

    assert postman._argus_client is None
    assert Argus.get() is None


class _EnableArgusParams(dict):
    """Minimal SCTConfiguration stand-in: dict-style .get() plus attribute access."""

    argus_use_ssh_tunnel = False


def test_init_argus_client_reuses_real_client_for_the_same_run(monkeypatch, tmp_path):
    """init_argus_client() runs more than once per real test run - once during config
    resolution, again in ClusterTester.setUp(). Both calls resolve the same run_id (it
    normally comes from an env var fixed before either runs). If the first call already
    connected a real client, the second call must reuse it rather than closing a working
    connection and reconnecting for no reason.
    """
    monkeypatch.setattr(TestConfig, "_argus_client", None)
    monkeypatch.setattr(TestConfig, "_logdir", str(tmp_path))
    monkeypatch.setattr("sdcm.test_config.get_job_name", lambda: "some-jenkins-job")

    real_client = MagicMock()
    real_client.run_id = "same-run"  # the reuse check compares this against the resolved run_id
    monkeypatch.setattr("sdcm.test_config.get_argus_client", lambda **kwargs: real_client)
    params = _EnableArgusParams(enable_argus=True)

    TestConfig.init_argus_client(params=params, test_id="same-run")
    TestConfig.init_argus_client(params=params, test_id="same-run")

    assert TestConfig._argus_client is real_client  # reused, not closed and recreated
    real_client.close.assert_not_called()


def test_init_argus_client_always_retries_replay_only_client_for_the_same_run(unwired_argus_postman):
    """Unlike a real client, a replay-only one is never reused across calls, even for a
    matching run_id - see test_init_argus_client_retries_real_connection_after_transient_failure
    for why: reusing it would permanently lock a transient connection failure into
    replay-only mode. Here enable_argus=False on both calls, so the replacement is just a
    second, distinct replay-only client rather than an upgrade to a real one.
    """
    postman, _ = unwired_argus_postman

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id="same-run")
    first_client = TestConfig._argus_client

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id="same-run")

    assert isinstance(TestConfig._argus_client, ReplayOnlyArgusSCTClient)
    assert TestConfig._argus_client is not first_client


def test_start_argus_event_pipeline_wires_up_replay_only_postman(unwired_argus_postman, monkeypatch):
    """With enable_argus=False, start_argus_event_pipeline() must give the postman a real
    client to submit through - instead of leaving it unset, which makes every event a
    silent no-op.
    """
    postman, aggregator = unwired_argus_postman
    test_id = str(uuid.uuid4())

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id=test_id)
    TestConfig.start_argus_event_pipeline()

    assert isinstance(postman._argus_client, ReplayOnlyArgusSCTClient)
    assert postman._argus_client.run_id == test_id
    assert Argus.get().client is postman._argus_client  # what ArgusEventCollector.run() reads

    submit_event = MagicMock()
    monkeypatch.setattr(postman._argus_client, "submit_event", submit_event)
    aggregator.outbound_queue.put(argus_event(Severity.ERROR, "err"))
    postman.stop(timeout=5)

    submit_event.assert_called_once()  # drained and posted, not a silent no-op


def test_start_argus_event_pipeline_second_call_takes_over_argus_singleton(unwired_argus_postman):
    """start_argus_event_pipeline() can run more than once in the same process (e.g. across
    multiple tests sharing a process, each with its own init_argus_client() call first).
    The latest call's client must become Argus.get().client; if it stays stuck on an
    earlier (now-closed) client, the event pipeline silently drops every event for the
    rest of the run - the same failure mode this whole fix exists to prevent.
    """
    postman, _ = unwired_argus_postman

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id="first-run")
    TestConfig.start_argus_event_pipeline()
    first_client = TestConfig._argus_client

    TestConfig.init_argus_client(params={"enable_argus": False}, test_id="second-run")
    TestConfig.start_argus_event_pipeline()
    second_client = TestConfig._argus_client

    assert second_client is not first_client
    assert Argus.get().client is second_client
    assert postman._argus_client is second_client


def test_init_argus_client_retries_real_connection_after_transient_failure(monkeypatch, tmp_path):
    """A transient get_argus_client() failure on one call must not permanently lock a run
    into replay-only mode. init_argus_client() only reuses an *already-real* client for a
    matching run_id - a replay-only fallback is always retried, so a later call for the
    same run_id (e.g. ClusterTester.init_argus_run(), after an earlier failed attempt
    during config resolution) gets a real connection if one is available this time.
    """
    monkeypatch.setattr(TestConfig, "_argus_client", None)
    monkeypatch.setattr(TestConfig, "_logdir", str(tmp_path))
    monkeypatch.setattr("sdcm.test_config.get_job_name", lambda: "some-jenkins-job")

    real_client = MagicMock()
    attempts = []

    def fake_get_argus_client(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise ArgusError("transient failure")
        return real_client

    monkeypatch.setattr("sdcm.test_config.get_argus_client", fake_get_argus_client)
    params = _EnableArgusParams(enable_argus=True)

    TestConfig.init_argus_client(params=params, test_id="same-run")
    assert isinstance(TestConfig._argus_client, ReplayOnlyArgusSCTClient)  # first call fell back

    TestConfig.init_argus_client(params=params, test_id="same-run")

    assert TestConfig._argus_client is real_client  # second call retried and got a real one
    assert len(attempts) == 2


def test_start_argus_event_pipeline_handles_postman_not_yet_registered(monkeypatch, tmp_path):
    """enable_argus_posting()/start_posting_argus_events() raise AttributeError - not
    RuntimeError - when the default events registry exists but the argus postman process
    specifically hasn't been registered in it yet (get_events_process() returns None
    instead of raising). start_argus_event_pipeline() must catch that too, rather than
    letting it escape into ClusterTester.init_argus_run()'s broad except Exception and
    silently skip the submit_sct_run()/set_sct_runner() calls that follow it.
    """
    registry = EventsProcessesRegistry(log_dir=tmp_path)  # exists, but nothing registered in it
    monkeypatch.setattr(events_processes_module, "_EVENTS_PROCESSES", registry)
    monkeypatch.setattr(Argus, "INSTANCE", None)
    replay_client = ReplayOnlyArgusSCTClient(run_id="r1", log_dir=str(tmp_path))
    monkeypatch.setattr(TestConfig, "_argus_client", replay_client)
    try:
        TestConfig.start_argus_event_pipeline()  # must not raise AttributeError
    finally:
        replay_client.close()


def test_start_argus_event_pipeline_failure_does_not_discard_client(monkeypatch, tmp_path):
    """If wiring the event pipeline fails (e.g. the events-processes registry isn't up
    yet), the Argus client itself must be untouched - only the real-time event pipeline
    wiring is skipped, direct/synchronous Argus calls keep working via that same client.
    """
    monkeypatch.setattr(events_processes_module, "_EVENTS_PROCESSES", None)  # registry not ready
    monkeypatch.setattr(Argus, "INSTANCE", None)
    monkeypatch.setattr(TestConfig, "_argus_client", None)
    monkeypatch.setattr(TestConfig, "_logdir", str(tmp_path))
    monkeypatch.setattr("sdcm.test_config.get_job_name", lambda: "some-jenkins-job")

    real_client = MagicMock()
    monkeypatch.setattr("sdcm.test_config.get_argus_client", lambda **kwargs: real_client)

    params = _EnableArgusParams(enable_argus=True)
    TestConfig.init_argus_client(params=params, test_id=str(uuid.uuid4()))
    TestConfig.start_argus_event_pipeline()

    assert TestConfig._argus_client is real_client  # kept, not discarded
    real_client.close.assert_not_called()


def _fake_argus_event(**overrides) -> SimpleNamespace:
    fields = {
        "publish_to_argus": True,
        "severity": Severity.WARNING,
        "timestamp": time.time(),
        "duration": None,
        "event_id": "fake-event-id",
        "known_issue": None,
        "nemesis_name": None,
        "nemesis_status": None,
        "node": None,
        "received_timestamp": None,
        "target_node": None,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def test_argus_event_collector_resolves_run_id_per_event_not_once_at_thread_start(
    events_function_scope, tmp_path, monkeypatch
):
    """ArgusEventCollector.run() starts (as a thread, via the autouse event_system pytest
    fixture / start_events_device()) before TestConfig.start_argus_event_pipeline() calls
    Argus.init_global() in ClusterTester.setUp() - so it must resolve run_id fresh for each
    event, not once when the thread starts. Otherwise every event for the whole test would
    be stamped with whatever Argus.get() returned at thread-start, almost always None.
    """
    monkeypatch.setattr(Argus, "INSTANCE", None)

    replay_client = ReplayOnlyArgusSCTClient(run_id=str(uuid.uuid4()), log_dir=str(tmp_path))
    try:
        # events_function_scope isolates SctEvent._events_processes_registry for this test;
        # reuse it rather than constructing a throwaway registry no other test can leak into.
        collector = ArgusEventCollector(_registry=SctEvent._events_processes_registry)

        def fake_inbound_events():
            # Argus.INSTANCE is still unset here, matching production: the collector thread
            # is already running by the time ClusterTester.setUp() gets around to wiring
            # Argus up, so its first event(s) can arrive before that happens.
            yield ("FakeEvent", _fake_argus_event(event_id="before-init"))
            Argus.init_global(replay_client)  # simulates setUp() completing mid-test
            yield ("FakeEvent", _fake_argus_event(event_id="after-init"))

        monkeypatch.setattr(collector, "inbound_events", fake_inbound_events)

        collector.run()

        before, after = (collector.outbound_queue.get_nowait() for _ in range(2))
        assert before["run_id"] is None  # nothing to attach yet - genuinely unresolvable
        assert after["run_id"] == replay_client.run_id  # picked up once Argus was wired
    finally:
        replay_client.close()


def test_argus_event_collector_survives_argus_wrapping_a_none_client(events_function_scope, monkeypatch):
    """Argus.get() can return a truthy Argus wrapper whose .client is itself None/falsy -
    the collector must degrade to run_id=None for that event (as it always has), not raise
    and drop the event entirely.
    """
    monkeypatch.setattr(Argus, "INSTANCE", Argus(None))

    collector = ArgusEventCollector(_registry=SctEvent._events_processes_registry)
    monkeypatch.setattr(collector, "inbound_events", lambda: iter([("FakeEvent", _fake_argus_event())]))

    collector.run()  # must not raise

    evt = collector.outbound_queue.get_nowait()
    assert evt["run_id"] is None
