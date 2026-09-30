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
# Copyright (c) 2026 ScyllaDB

"""`cql_address` resolution guards.

`socket.gaierror` is a subclass of `OSError`, so an unresolvable `cql_address` used to be
swallowed by the `except OSError` branch of `is_port_used()` and look exactly like "the CQL
port is not open yet" - the caller then burned its full timeout with nothing in the log.
"""

import logging
import queue
import socket
from types import MethodType
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseNode, CqlAddressUnresolvableError, _drain_queued_failures


@pytest.fixture(name="node")
def node_fixture():
    node = MagicMock(spec=BaseNode)
    node.log = logging.getLogger("test_node")
    node.name = "test-node"
    node.CQL_PORT = BaseNode.CQL_PORT
    node.CQL_ADDRESS_RESOLVE_STEP = BaseNode.CQL_ADDRESS_RESOLVE_STEP
    node.cql_address = "ip-10-3-13-210.eu-west-2.compute.internal"
    node.is_port_used = MethodType(BaseNode.is_port_used, node)
    node.verify_cql_address_resolvable = MethodType(BaseNode.verify_cql_address_resolvable, node)
    return node


def test_is_port_used_reports_name_resolution_failure(node, caplog):
    """An unresolvable cql_address must name itself in the log, not fail silently."""
    with (
        patch("sdcm.cluster.socket.create_connection", side_effect=socket.gaierror("Name or service not known")),
        caplog.at_level(logging.ERROR, logger="test_node"),
    ):
        assert node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server") is False

    assert "Cannot resolve" in caplog.text
    assert node.cql_address in caplog.text


def test_is_port_used_stays_quiet_for_a_closed_port(node, caplog):
    """A refused connection is the normal "not up yet" case and must not be logged as an error."""
    with (
        patch("sdcm.cluster.socket.create_connection", side_effect=ConnectionRefusedError()),
        caplog.at_level(logging.ERROR, logger="test_node"),
    ):
        assert node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server") is False

    assert caplog.text == ""


def test_verify_cql_address_resolvable_passes_when_the_name_resolves(node):
    with patch("sdcm.cluster.socket.getaddrinfo") as getaddrinfo:
        node.verify_cql_address_resolvable(timeout=0)
    assert getaddrinfo.called


def test_verify_cql_address_resolvable_raises_with_the_offending_address(node):
    """A name that never resolves is a hard failure, not something to wait an hour for."""
    with (
        patch("sdcm.cluster.socket.getaddrinfo", side_effect=socket.gaierror("Name or service not known")),
        pytest.raises(CqlAddressUnresolvableError) as err,
    ):
        node.verify_cql_address_resolvable(timeout=0)

    assert node.cql_address in str(err.value)
    assert node.name in str(err.value)
    # the hint is flagged as AWS-specific: this runs on every backend, and GCE internal DNS
    # is project-wide, so a blanket "must be in the same region" claim would be wrong there
    assert "On AWS" in str(err.value)


def test_verify_cql_address_resolvable_never_sleeps_past_the_deadline(node):
    """`timeout` is an upper bound: a short timeout must not be overrun by the fixed retry step.

    Driven by a fake clock that only advances when the code sleeps, so the assertion is about
    the sleep arithmetic rather than about real elapsed time.
    """
    clock = [1000.0]
    slept = []

    def fake_sleep(duration):
        slept.append(duration)
        clock[0] += duration

    with (
        patch("sdcm.cluster.socket.getaddrinfo", side_effect=socket.gaierror("Name or service not known")),
        patch("sdcm.cluster.time.time", side_effect=lambda: clock[0]),
        patch("sdcm.cluster.time.sleep", side_effect=fake_sleep),
        pytest.raises(CqlAddressUnresolvableError),
    ):
        node.verify_cql_address_resolvable(timeout=1)

    # an unclamped `time.sleep(CQL_ADDRESS_RESOLVE_STEP)` would put 5s here for a 1s budget
    assert slept, "expected at least one retry before giving up"
    assert sum(slept) <= 1, f"slept {sum(slept)}s for a 1s timeout: {slept}"


def test_drain_queued_failures_returns_only_failures_without_blocking():
    """A shared cause fails several nodes at once; every queued failure must be reported."""
    task_queue = queue.Queue()
    task_queue.put(("node-1", ("boom-1", "traceback-1")))
    task_queue.put(("node-2", None))  # succeeded
    task_queue.put(("node-3", ("boom-3", "traceback-3")))

    failures = _drain_queued_failures(task_queue)

    assert failures == [("node-1", ("boom-1", "traceback-1")), ("node-3", ("boom-3", "traceback-3"))]
    # the queue is drained, and draining an empty queue returns immediately rather than blocking
    assert task_queue.empty()
    assert _drain_queued_failures(task_queue) == []
