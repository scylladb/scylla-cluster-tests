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
from unittest.mock import MagicMock, patch

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.cluster import BaseNode, BaseScyllaCluster, NodeSetupFailed

logging.basicConfig(level=logging.DEBUG)


class FakeResult:
    """Fake result object for remoter.run calls."""

    def __init__(self, stdout="", stderr="", exit_status=0):
        self.stdout = stdout
        self.stderr = stderr
        self.exit_status = exit_status


def make_mock_node():
    """Create a mock BaseNode with a mock remoter for testing."""
    node = MagicMock(spec=BaseNode)
    node.remoter = MagicMock()
    node.log = logging.getLogger("test_node")
    node.check_dns_ready = BaseNode.check_dns_ready.__get__(node, BaseNode)
    node._capture_dns_diagnostics = BaseNode._capture_dns_diagnostics.__get__(node, BaseNode)
    node._is_dns_network_error = BaseNode._is_dns_network_error
    node.DNS_FAILURE_PATTERNS = BaseNode.DNS_FAILURE_PATTERNS
    return node


class TestCheckDnsReady:
    """Tests for BaseNode.check_dns_ready method."""

    def test_dns_ready_succeeds_first_attempt(self):
        node = make_mock_node()
        node.remoter.run.return_value = FakeResult(exit_status=0, stdout="93.184.216.34 archive.ubuntu.com")

        result = node.check_dns_ready(timeout=10, interval=1)

        assert result is True
        node.remoter.run.assert_called_once_with(
            "getent hosts archive.ubuntu.com",
            ignore_status=True,
            verbose=False,
        )

    @patch("sdcm.cluster.time")
    def test_dns_ready_succeeds_after_retry(self, mock_time):
        node = make_mock_node()
        call_count = 0
        time_values = [0, 0, 5, 5, 10]
        mock_time.time.side_effect = time_values
        mock_time.sleep = MagicMock()

        def run_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return FakeResult(exit_status=2, stdout="")
            return FakeResult(exit_status=0, stdout="93.184.216.34 archive.ubuntu.com")

        node.remoter.run.side_effect = run_side_effect

        result = node.check_dns_ready(timeout=120, interval=5)

        assert result is True
        assert call_count == 3

    @patch("sdcm.cluster.time")
    def test_dns_ready_fails_after_timeout(self, mock_time):
        node = make_mock_node()
        # Simulate time progressing past the deadline
        mock_time.time.side_effect = [0, 0, 50, 50, 130]
        mock_time.sleep = MagicMock()
        node.remoter.run.return_value = FakeResult(exit_status=2, stdout="")

        result = node.check_dns_ready(timeout=120, interval=5)

        assert result is False

    def test_dns_ready_custom_host(self):
        node = make_mock_node()
        node.remoter.run.return_value = FakeResult(exit_status=0, stdout="1.2.3.4 custom.host.example.com")

        result = node.check_dns_ready(timeout=10, interval=1, dns_host="custom.host.example.com")

        assert result is True
        node.remoter.run.assert_called_once_with(
            "getent hosts custom.host.example.com",
            ignore_status=True,
            verbose=False,
        )


class TestCaptureDnsDiagnostics:
    """Tests for BaseNode._capture_dns_diagnostics method."""

    def test_captures_diagnostics_commands(self):
        node = make_mock_node()
        node.remoter.run.return_value = FakeResult(stdout="diagnostic output")

        node._capture_dns_diagnostics()

        assert node.remoter.run.call_count == 3
        expected_commands = [
            "cat /etc/resolv.conf",
            "resolvectl status 2>/dev/null || systemd-resolve --status 2>/dev/null || true",
            "nmcli device show 2>/dev/null || true",
        ]
        for cmd in expected_commands:
            node.remoter.run.assert_any_call(cmd, ignore_status=True, verbose=False)


class TestIsDnsNetworkError:
    """Tests for BaseNode._is_dns_network_error static method."""

    @pytest.mark.parametrize("error_text", [
        "Temporary failure resolving 'archive.ubuntu.com'",
        "Could not resolve host: mirror.example.com",
        "Name or service not known",
        "Err:1 http://archive.ubuntu.com/ubuntu focal InRelease",
        "Failed to fetch http://archive.ubuntu.com/ubuntu/dists/focal/InRelease",
    ])
    def test_detects_dns_errors(self, error_text):
        assert BaseNode._is_dns_network_error(error_text) is True

    @pytest.mark.parametrize("error_text", [
        "Permission denied",
        "No such file or directory",
        "Segmentation fault",
        "disk full",
    ])
    def test_ignores_non_dns_errors(self, error_text):
        assert BaseNode._is_dns_network_error(error_text) is False


class TestScyllaSetupRetry:
    """Tests for scylla_setup retry logic on DNS/network errors."""

    def _make_node_for_setup(self):
        """Create a mock node suitable for scylla_setup testing."""
        node = make_mock_node()
        node.scylla_setup = BaseNode.scylla_setup.__get__(node, BaseNode)

        mock_params = MagicMock()
        mock_params.get.side_effect = lambda key: {
            "append_scylla_setup_args": "",
            "unified_package": None,
        }.get(key, "")
        node.parent_cluster = MagicMock()
        node.parent_cluster.params = mock_params
        return node

    @patch("sdcm.cluster.time")
    def test_scylla_setup_retries_on_dns_error(self, mock_time):
        mock_time.sleep = MagicMock()
        node = self._make_node_for_setup()

        call_count = 0
        responses = {
            "scylla_setup --help": FakeResult(stdout="--swap-directory"),
            "apt-get update": FakeResult(stdout=""),
            "cat /proc/mounts": FakeResult(stdout=" /var/lib/scylla "),
        }

        def run_side_effect(cmd, **kwargs):  # noqa: PLR0911
            nonlocal call_count
            if "scylla_setup --nic" in cmd:
                call_count += 1
                if call_count == 1:
                    fake_result = MagicMock()
                    fake_result.stderr = "Temporary failure resolving 'archive.ubuntu.com'"
                    fake_result.stdout = ""
                    raise UnexpectedExit(fake_result)
                return FakeResult(stdout="")
            for key, result in responses.items():
                if key in cmd:
                    return result
            return FakeResult(stdout="")

        node.remoter.run.side_effect = run_side_effect

        node.scylla_setup(disks=["/dev/sda"], devname="eth0")

        assert call_count == 2

    @patch("sdcm.cluster.time")
    def test_scylla_setup_raises_non_dns_error(self, mock_time):
        mock_time.sleep = MagicMock()
        node = self._make_node_for_setup()

        def run_side_effect(cmd, **kwargs):
            if "scylla_setup --help" in cmd:
                return FakeResult(stdout="--swap-directory")
            if "scylla_setup --nic" in cmd:
                fake_result = MagicMock()
                fake_result.stderr = "Permission denied"
                fake_result.stdout = ""
                raise UnexpectedExit(fake_result)
            return FakeResult(stdout="")

        node.remoter.run.side_effect = run_side_effect

        with pytest.raises(UnexpectedExit):
            node.scylla_setup(disks=["/dev/sda"], devname="eth0")

    @patch("sdcm.cluster.time")
    def test_scylla_setup_raises_after_max_retries(self, mock_time):
        mock_time.sleep = MagicMock()
        node = self._make_node_for_setup()

        def run_side_effect(cmd, **kwargs):
            if "scylla_setup --help" in cmd:
                return FakeResult(stdout="--swap-directory")
            if "scylla_setup --nic" in cmd:
                fake_result = MagicMock()
                fake_result.stderr = "Temporary failure resolving 'archive.ubuntu.com'"
                fake_result.stdout = ""
                raise UnexpectedExit(fake_result)
            if "apt-get update" in cmd:
                return FakeResult(stdout="")
            # Diagnostics commands
            return FakeResult(stdout="")

        node.remoter.run.side_effect = run_side_effect

        with pytest.raises(UnexpectedExit):
            node.scylla_setup(disks=["/dev/sda"], devname="eth0")


class TestScyllaPostInstallDnsCheck:
    """Tests for _scylla_post_install DNS readiness gate."""

    @staticmethod
    def _make_post_install_node():
        """Create a plain MagicMock node for _scylla_post_install tests."""
        node = MagicMock()
        node.detect_disks.return_value = ["/dev/sda"]
        return node

    def test_raises_when_dns_not_ready(self):
        node = self._make_post_install_node()
        node.check_dns_ready.return_value = False

        with pytest.raises(NodeSetupFailed, match="DNS readiness check failed"):
            BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=True, devname="eth0")

    def test_proceeds_when_dns_ready(self):
        node = self._make_post_install_node()
        node.check_dns_ready.return_value = True

        BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=True, devname="eth0")

        node.check_dns_ready.assert_called_once()
        node.scylla_setup.assert_called_once_with(["/dev/sda"], "eth0")

    def test_skips_when_not_new_install(self):
        node = self._make_post_install_node()

        BaseScyllaCluster._scylla_post_install(node, new_scylla_installed=False, devname="eth0")

        node.check_dns_ready.assert_not_called()
        node.scylla_setup.assert_not_called()
