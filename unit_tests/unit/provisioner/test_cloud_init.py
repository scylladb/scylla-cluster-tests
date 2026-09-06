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

"""SCT-934: cloud-init status --wait must be bounded by an independent SCT-side timeout."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from invoke.runners import Result

from sdcm.provision.helpers.cloud_init import (
    CLOUD_INIT_OUTPUT_LOG,
    CLOUD_INIT_WAIT_TIMEOUT,
    wait_cloud_init_completes,
)
from unit_tests.lib.fake_remoter import FakeRemoter

TAIL_LOG_CONTENT = "2026-01-01 00:00:00 Running module apt-configure\nstuck: waiting for mirror..."

JSON_STATUS_VERSION = Result(stdout="cloud-init 24.4.1-0ubuntu1", stderr="", exited=0)
LEGACY_STATUS_VERSION = Result(stdout="cloud-init 19.4-45.amzn2", stderr="", exited=0)
CLOUD_INIT_PRESENT = Result(stdout="/usr/bin/cloud-init", exited=0)


def _instance(name: str = "test-node"):
    """A minimal stand-in for VmInstance: wait_cloud_init_completes only needs `.name`."""
    return SimpleNamespace(name=name)


def _remoter() -> FakeRemoter:
    # user != "root" so RemoteCmdRunnerBase.sudo() actually prefixes commands with "sudo ".
    return FakeRemoter(hostname="1.2.3.4", user="scyllaadm")


def _status_call_count(sudo_spy) -> int:
    return sum(1 for call in sudo_spy.call_args_list if "cloud-init status" in call.args[0])


def test_wait_cloud_init_completes_raises_timeout_error_on_json_branch():
    """On the json-capable branch (cloud-init >= 23.4), a timed-out wait raises TimeoutError
    with an actionable message: node identifier, timeout bound, log path, and its tail."""
    remoter = _remoter()
    FakeRemoter.result_map = {
        r"sudo bash -c 'command -v cloud-init'": CLOUD_INIT_PRESENT,
        r"cloud-init --version 2>&1": JSON_STATUS_VERSION,
        r"sudo timeout --kill-after=10s --signal=TERM \d+ cloud-init status --format=json --wait": Result(
            stdout="", stderr="", exited=124
        ),
        r"sudo tail -n \d+ /var/log/cloud-init-output\.log": Result(stdout=TAIL_LOG_CONTENT, exited=0),
    }
    instance = _instance("json-branch-node")

    with pytest.raises(TimeoutError) as exc_info:
        wait_cloud_init_completes(remoter, instance)

    message = str(exc_info.value)
    assert "json-branch-node" in message
    assert str(CLOUD_INIT_WAIT_TIMEOUT) in message
    assert CLOUD_INIT_OUTPUT_LOG in message
    assert TAIL_LOG_CONTENT in message


def test_wait_cloud_init_completes_timeout_is_not_retried_20x():
    """Core regression guard: a timeout must not be silently retried by the @retrying(n=20) wrapper -
    the status/wait command is issued exactly once."""
    remoter = _remoter()
    FakeRemoter.result_map = {
        r"sudo bash -c 'command -v cloud-init'": CLOUD_INIT_PRESENT,
        r"cloud-init --version 2>&1": JSON_STATUS_VERSION,
        r"sudo timeout --kill-after=10s --signal=TERM \d+ cloud-init status --format=json --wait": Result(
            stdout="", stderr="", exited=124
        ),
        r"sudo tail -n \d+ /var/log/cloud-init-output\.log": Result(stdout=TAIL_LOG_CONTENT, exited=0),
    }
    instance = _instance("json-branch-node")

    with patch.object(remoter, "sudo", wraps=remoter.sudo) as sudo_spy:
        with pytest.raises(TimeoutError):
            wait_cloud_init_completes(remoter, instance)

    assert _status_call_count(sudo_spy) == 1


def test_wait_cloud_init_completes_raises_timeout_error_on_legacy_branch():
    """Same timeout behavior applies on the legacy (pre-23.4, non-json) status --wait branch."""
    remoter = _remoter()
    FakeRemoter.result_map = {
        r"sudo bash -c 'command -v cloud-init'": CLOUD_INIT_PRESENT,
        r"cloud-init --version 2>&1": LEGACY_STATUS_VERSION,
        r"sudo timeout --kill-after=10s --signal=TERM \d+ cloud-init status --wait": Result(
            stdout="", stderr="", exited=124
        ),
        r"sudo tail -n \d+ /var/log/cloud-init-output\.log": Result(stdout=TAIL_LOG_CONTENT, exited=0),
    }
    instance = _instance("legacy-branch-node")

    with patch.object(remoter, "sudo", wraps=remoter.sudo) as sudo_spy:
        with pytest.raises(TimeoutError) as exc_info:
            wait_cloud_init_completes(remoter, instance)

    message = str(exc_info.value)
    assert "legacy-branch-node" in message
    assert str(CLOUD_INIT_WAIT_TIMEOUT) in message
    assert CLOUD_INIT_OUTPUT_LOG in message
    assert TAIL_LOG_CONTENT in message
    assert _status_call_count(sudo_spy) == 1


def test_wait_cloud_init_completes_happy_path_applies_timeout_bound():
    """On success, behavior is unchanged (no exception), but the command actually sent to the node
    is wrapped in `timeout N`, proving the bound is applied even when cloud-init completes fine."""
    remoter = _remoter()
    FakeRemoter.result_map = {
        r"sudo bash -c 'command -v cloud-init'": CLOUD_INIT_PRESENT,
        r"cloud-init --version 2>&1": JSON_STATUS_VERSION,
        r"sudo timeout --kill-after=10s --signal=TERM \d+ cloud-init status --format=json --wait": Result(
            stdout='{"status": "done", "errors": []}', stderr="", exited=0
        ),
        r"ls /var/lib/sct/cloud-init": Result(stdout="done", exited=0),
    }
    instance = _instance("healthy-node")

    with patch.object(remoter, "sudo", wraps=remoter.sudo) as sudo_spy:
        wait_cloud_init_completes(remoter, instance)

    status_calls = [call for call in sudo_spy.call_args_list if "cloud-init status" in call.args[0]]
    assert len(status_calls) == 1
    assert (
        f"timeout --kill-after=10s --signal=TERM {CLOUD_INIT_WAIT_TIMEOUT} cloud-init status --format=json --wait"
        in status_calls[0].args[0]
    )
