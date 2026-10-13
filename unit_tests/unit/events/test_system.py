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
# Copyright (c) 2020 ScyllaDB

import pickle
from textwrap import dedent

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.system import (
    StartupTestEvent,
    TestFrameworkEvent,
    SpotTerminationEvent,
    NodeUnreachableEvent,
    ScyllaRepoEvent,
    InfoEvent,
    ThreadFailedEvent,
    CoreDumpEvent,
    SoftTimeoutEvent,
    TestResultEvent,
    INSTANCE_STATUS_EVENTS_PATTERNS,
)
from sdcm.sct_events.base import find_log_event


@pytest.mark.parametrize(
    "event_class, kwargs, expected",
    [
        pytest.param(
            StartupTestEvent,
            {},
            "(StartupTestEvent Severity.NORMAL) period_type=not-set event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e",
            id="startup-test",
        ),
        pytest.param(
            TestFrameworkEvent,
            {
                "source": "s1",
                "source_method": "m1",
                "args": ("a1", "a2"),
                "kwargs": {"k1": "v1", "k2": "v2"},
                "message": "msg1",
                "exception": "e1",
            },
            "(TestFrameworkEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e, source=s1.m1(args=('a1', 'a2'), "
            "kwargs={'k1': 'v1', 'k2': 'v2'})"
            " message=msg1\nexception=e1",
            id="test-framework",
        ),
        pytest.param(
            SpotTerminationEvent,
            {"node": "node1", "message": "m1"},
            "(SpotTerminationEvent Severity.CRITICAL) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: node=node1 message=m1",
            id="spot-termination",
        ),
        pytest.param(
            NodeUnreachableEvent,
            {"node": "loader-1", "message": "m1"},
            "(NodeUnreachableEvent Severity.CRITICAL) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: node=loader-1 message=m1",
            id="node-unreachable",
        ),
        pytest.param(
            ScyllaRepoEvent,
            {"url": "u1", "error": "e1"},
            "(ScyllaRepoEvent Severity.WARNING) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: url=u1 error=e1",
            id="scylla-repo",
        ),
        pytest.param(
            InfoEvent,
            {"message": "m1"},
            "(InfoEvent Severity.NORMAL) period_type=not-set event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: message=m1",
            id="info",
        ),
        pytest.param(
            ThreadFailedEvent,
            {"message": "m1", "traceback": "t1"},
            "(ThreadFailedEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: message=m1\nt1",
            id="thread-failed",
        ),
        pytest.param(
            CoreDumpEvent,
            {"node": "node1", "corefile_url": "url1", "backtrace": "b1", "download_instructions": "d1"},
            dedent("""\
                (CoreDumpEvent Severity.ERROR) period_type=one-time event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e node=node1
                corefile_url=url1
                backtrace=b1
                Info about modules can be found in SCT logs by search for 'Coredump Modules info'
                download_instructions:
                d1
                """),
            id="coredump",
        ),
    ],
)
def test_system_event_msgfmt(event_class, kwargs, expected):
    """Each system event renders its fields in the log format and survives a pickle round trip between processes."""
    event = event_class(**kwargs)
    event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
    assert str(event) == expected
    assert event == pickle.loads(pickle.dumps(event))


@pytest.mark.parametrize(
    "test_status, expected",
    [
        pytest.param(
            "FAILED",
            dedent("""\
                ================================= TEST RESULTS =================================

                ----- LAST g1 EVENT ----------------------------------------------------------
                e1
                e2
                ----- LAST g2 EVENT ----------------------------------------------------------
                e3
                ================================================================================
                FAILED :(
            """),
            id="failed-lists-last-events",
        ),
        pytest.param(
            "SUCCESS",
            dedent("""\
                ================================= TEST RESULTS =================================
                ================================================================================
                SUCCESS :)
            """),
            id="success-omits-events",
        ),
    ],
)
def test_test_result_event_msgfmt_by_status(test_status, expected):
    """TestResultEvent lists the last events per group only on failure and survives a pickle round trip."""
    event = TestResultEvent(test_status=test_status, events={"g1": ["e1", "e2"], "g2": ["e3"]})
    event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
    assert str(event) == expected
    loaded_event = pickle.loads(pickle.dumps(event))
    loaded_event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
    assert event == loaded_event


def test_instance_status_events_patterns(test_data_dir):
    """INSTANCE_STATUS_EVENTS_PATTERNS turn startup, reboot and power-off log lines into WARNING events, in order."""
    cloned_events = []
    with open(str(test_data_dir / "system_status_events.log"), encoding="utf-8") as sct_log:
        for index, line in enumerate(sct_log.readlines()):
            if event := find_log_event(INSTANCE_STATUS_EVENTS_PATTERNS, line):
                cloned_events.append(event.clone().add_info(node="test-node", line_number=index, line=line))

    assert len(cloned_events) == 3

    assert cloned_events[0].type == "STARTUP"
    assert cloned_events[0].regex == "kernel: Linux version"
    assert cloned_events[0].severity == Severity.WARNING

    assert cloned_events[1].type == "REBOOT"
    assert cloned_events[1].regex == "Stopped target Host and Network Name Lookups"
    assert cloned_events[1].severity == Severity.WARNING

    assert cloned_events[2].type == "POWER_OFF"
    assert cloned_events[2].regex == "Reached target Power-Off"
    assert cloned_events[2].severity == Severity.WARNING


def test_soft_timeout():
    """SoftTimeoutEvent carries the caller's frame as trace and reports the operation, its duration and the soft timeout."""
    event = SoftTimeoutEvent(soft_timeout=0.1, operation="long-one", duration=0.2)

    assert event.trace
    assert "operation 'long-one' is finished and took 0.2s (soft timeout was set to 0.1s)" in str(event)
