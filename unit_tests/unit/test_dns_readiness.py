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

import logging
from types import MethodType
from unittest.mock import MagicMock, patch

import pytest
from invoke.exceptions import UnexpectedExit
from invoke.runners import Result

from sdcm.cluster import BaseNode, BaseScyllaCluster, NodeSetupFailed, _is_dns_network_error
from sdcm.exceptions import WaitForTimeoutError


def _fake_result(stdout="", stderr="", exit_status=0):
    result = MagicMock(spec=Result)
    result.stdout = stdout
    result.stderr = stderr
    result.exit_status = exit_status
    result.exited = exit_status
    result.hide = ()
    result.pty = False
    result.command = "test-command"
    return result


def _make_unexpected_exit(stderr: str = "", stdout: str = "") -> UnexpectedExit:
    return UnexpectedExit(result=_fake_result(stdout=stdout, stderr=stderr, exit_status=1))


def _setup_params():
    params = MagicMock()
    params.get.side_effect = lambda key: {
        "append_scylla_setup_args": "",
        "unified_package": None,
    }.get(key, "")
    return params


@pytest.fixture
def existing_node():
    node = MagicMock(spec=BaseNode)
    node.remoter = MagicMock()
    node.log = logging.getLogger("test_node")
    node.name = "test-node"
    node.check_dns_ready = MethodType(BaseNode.check_dns_ready, node)
    node._capture_dns_diagnostics = MethodType(BaseNode._capture_dns_diagnostics, node)
    node._refresh_package_cache_after_dns_error = MethodType(BaseNode._refresh_package_cache_after_dns_error, node)
    node._run_scylla_setup_with_dns_retry = MethodType(BaseNode._run_scylla_setup_with_dns_retry, node)
    node.scylla_setup = MethodType(BaseNode.scylla_setup, node)
    node.parent_cluster = MagicMock()
    node.parent_cluster.params = _setup_params()
    node.distro = MagicMock()
    node.distro.is_rhel_like = False
    return node


@pytest.fixture
def dns_wait_success(existing_node):
    existing_node.remoter.run.return_value = _fake_result(exit_status=0, stdout="93.184.216.34 scylladb.com")
    with patch("sdcm.cluster.wait.wait_for") as mock_wait_for:
        mock_wait_for.side_effect = lambda func, **kwargs: func()
        yield mock_wait_for


@pytest.fixture
def setup_command_runner(existing_node):
    def factory(*, first_error: str | None, use_yum: bool = False, mount_output: str = " /var/lib/scylla "):
        attempts = {"scylla_setup": 0, "apt_get": 0, "yum_makecache": 0}

        def run(cmd, **kwargs):
            if "scylla_setup --help" in cmd:
                return _fake_result(stdout="--swap-directory")
            if "scylla_setup --nic" in cmd:
                attempts["scylla_setup"] += 1
                if attempts["scylla_setup"] == 1 and first_error is not None:
                    raise _make_unexpected_exit(stderr=first_error)
                return _fake_result()
            if "sudo apt-get update" in cmd:
                attempts["apt_get"] += 1
                return _fake_result()
            if "sudo yum makecache" in cmd:
                attempts["yum_makecache"] += 1
                return _fake_result()
            if "cat /proc/mounts" in cmd:
                return _fake_result(stdout=mount_output)
            return _fake_result()

        existing_node.distro.is_rhel_like = use_yum
        existing_node.remoter.run.side_effect = run
        return attempts

    return factory


def test_check_dns_ready_returns_true_on_first_success(existing_node, dns_wait_success):
    """Return True when the first DNS lookup succeeds."""
    assert existing_node.check_dns_ready(timeout=10, interval=1) is True
    dns_wait_success.assert_called_once()


def test_check_dns_ready_uses_requested_host(existing_node):
    """Query the provided hostname when checking DNS readiness."""
    existing_node.remoter.run.return_value = _fake_result(exit_status=0, stdout="1.2.3.4 custom.host.example.com")

    with patch("sdcm.cluster.wait.wait_for") as mock_wait_for:
        mock_wait_for.side_effect = lambda func, **kwargs: func()
        assert existing_node.check_dns_ready(timeout=10, interval=1, dns_host="custom.host.example.com") is True

    existing_node.remoter.run.assert_called_once_with(
        "getent hosts custom.host.example.com",
        ignore_status=True,
        verbose=False,
    )


def test_check_dns_ready_passes_wait_for_configuration(existing_node):
    """Forward timeout, interval, and log text into wait.wait_for."""
    existing_node.remoter.run.return_value = _fake_result(exit_status=0)

    with patch("sdcm.cluster.wait.wait_for") as mock_wait_for:
        mock_wait_for.side_effect = lambda func, **kwargs: func()
        existing_node.check_dns_ready(timeout=120, interval=10, dns_host="example.com")

    _, kwargs = mock_wait_for.call_args
    assert kwargs["step"] == 10
    assert kwargs["timeout"] == 120
    assert kwargs["text"] == "DNS readiness on test-node"


def test_check_dns_ready_returns_false_after_timeout(existing_node):
    """Return False and capture diagnostics when DNS never becomes ready."""
    existing_node.remoter.run.return_value = _fake_result(exit_status=2)

    with patch("sdcm.cluster.wait.wait_for", side_effect=WaitForTimeoutError("timed out")):
        assert existing_node.check_dns_ready(timeout=60, interval=5) is False

    assert existing_node.remoter.run.call_count == 3


def test_capture_dns_diagnostics_runs_expected_commands(existing_node):
    """Collect the three DNS diagnostic commands in a fixed order."""
    existing_node.remoter.run.return_value = _fake_result(stdout="diagnostic output")

    existing_node._capture_dns_diagnostics()

    assert existing_node.remoter.run.call_args_list == [
        (("cat /etc/resolv.conf",), {"ignore_status": True, "verbose": False}),
        (
            ("resolvectl status 2>/dev/null || systemd-resolve --status 2>/dev/null || true",),
            {"ignore_status": True, "verbose": False},
        ),
        (("nmcli device show 2>/dev/null || true",), {"ignore_status": True, "verbose": False}),
    ]


@pytest.mark.parametrize(
    ("error_text", "expected"),
    [
        pytest.param("Temporary failure resolving 'archive.ubuntu.com'", True, id="temporary_failure"),
        pytest.param("Could not resolve host: mirror.example.com", True, id="could_not_resolve"),
        pytest.param("Name or service not known", True, id="name_not_known"),
        pytest.param("Permission denied", False, id="permission_denied"),
        pytest.param("No such file or directory", False, id="missing_file"),
        pytest.param("Segmentation fault", False, id="segfault"),
        pytest.param("disk full", False, id="disk_full"),
        pytest.param("Failed to fetch http://example.com/package.deb 404 Not Found", False, id="fetch_404"),
    ],
)
def test_is_dns_network_error_matches_expected_patterns(error_text, expected):
    """Recognize only the DNS error strings that should trigger retries."""
    assert _is_dns_network_error(error_text) is expected


@pytest.mark.parametrize(
    ("use_yum", "first_error", "expected_attempts"),
    [
        pytest.param(
            False,
            "Temporary failure resolving 'archive.ubuntu.com'",
            {"scylla_setup": 2, "apt_get": 1, "yum_makecache": 0},
            id="apt_retry",
        ),
        pytest.param(
            True,
            "Could not resolve host: mirror.centos.org",
            {"scylla_setup": 2, "apt_get": 0, "yum_makecache": 1},
            id="yum_retry",
        ),
    ],
)
@patch("sdcm.utils.decorators.time.sleep")
def test_scylla_setup_retries_dns_failures_with_matching_package_refresh(
    mock_sleep,
    existing_node,
    setup_command_runner,
    use_yum,
    first_error,
    expected_attempts,
):
    """Retry scylla_setup only for DNS failures and refresh the matching package cache."""
    attempts = setup_command_runner(first_error=first_error, use_yum=use_yum)

    existing_node.scylla_setup(disks=["/dev/sda"], devname="eth0")

    assert attempts == expected_attempts


@patch("sdcm.utils.decorators.time.sleep")
def test_scylla_setup_raises_non_dns_error_without_retry(mock_sleep, existing_node, setup_command_runner):
    """Propagate non-DNS failures immediately without package refresh."""
    attempts = setup_command_runner(first_error="Permission denied", use_yum=False)

    with pytest.raises(UnexpectedExit):
        existing_node.scylla_setup(disks=["/dev/sda"], devname="eth0")

    assert attempts == {"scylla_setup": 1, "apt_get": 0, "yum_makecache": 0}


@patch("sdcm.utils.decorators.time.sleep")
def test_scylla_setup_raises_after_dns_retries_are_exhausted(mock_sleep, existing_node):
    """Surface the original DNS failure after the retry budget is exhausted."""

    def run(cmd, **kwargs):
        if "scylla_setup --help" in cmd:
            return _fake_result(stdout="--swap-directory")
        if "scylla_setup --nic" in cmd:
            raise _make_unexpected_exit(stderr="Temporary failure resolving 'archive.ubuntu.com'")
        if "sudo apt-get update" in cmd:
            return _fake_result()
        return _fake_result()

    existing_node.remoter.run.side_effect = run

    with pytest.raises(UnexpectedExit):
        existing_node.scylla_setup(disks=["/dev/sda"], devname="eth0")


def test_post_install_raises_when_dns_not_ready():
    """Abort post-install when the node never reaches DNS readiness."""
    node = MagicMock()
    node.detect_disks.return_value = ["/dev/sda"]
    node.check_dns_ready.return_value = False

    with pytest.raises(NodeSetupFailed, match="DNS readiness check failed"):
        BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=True, devname="eth0")


def test_post_install_skips_setup_when_installation_is_not_new():
    """Skip DNS readiness and scylla_setup for existing installations."""
    node = MagicMock()

    BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=False, devname="eth0")

    node.check_dns_ready.assert_not_called()
    node.scylla_setup.assert_not_called()
