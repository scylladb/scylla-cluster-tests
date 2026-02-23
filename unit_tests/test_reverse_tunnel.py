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

import tempfile
from unittest.mock import Mock, patch

import pytest

from sdcm.utils.reverse_tunnel import (
    get_tunnel_manager,
    AutosshTunnelManager,
    NgrokTunnelManager,
    PYNGROK_AVAILABLE,
)


class TestAutosshTunnelManager:
    """Tests for AutosshTunnelManager."""

    @pytest.fixture
    def mock_node(self):
        """Create a mock BaseNode."""
        node = Mock()
        node.name = "test-node"
        node.logdir = tempfile.gettempdir()
        node.ssh_login_info = {
            "hostname": "192.168.1.100",
            "port": "22",
            "user": "centos",
            "key_file": "/path/to/key.pem",
        }
        return node

    def test_get_container_run_args(self, mock_node):
        """Test that AutosshTunnelManager generates correct container args."""
        manager = AutosshTunnelManager(mock_node, "syslog_ng")

        args = manager.get_container_run_args(local_port=5000, remote_port=8000)

        assert args["image"] == "jnovack/autossh:1.2.2"
        assert "test-node" in args["name"]
        assert "192.168.1.100" in args["name"]  # hostname with colons replaced, dots kept
        assert args["environment"]["SSH_HOSTNAME"] == "192.168.1.100"
        assert args["environment"]["SSH_HOSTPORT"] == "22"
        assert args["environment"]["SSH_HOSTUSER"] == "centos"
        assert args["environment"]["SSH_TUNNEL_LOCAL"] == 5000
        assert args["environment"]["SSH_TUNNEL_REMOTE"] == 8000
        assert args["environment"]["SSH_MODE"] == "-R"
        assert args["environment"]["AUTOSSH_GATETIME"] == 0
        assert args["network_mode"] == "host"
        assert args["restart_policy"]["Name"] == "always"

    def test_logfile_property(self, mock_node):
        """Test that logfile property returns correct path."""
        manager = AutosshTunnelManager(mock_node, "vector")

        logfile = manager.logfile

        assert "vector_tunnel.log" in logfile
        assert logfile.startswith(tempfile.gettempdir())


class TestNgrokTunnelManager:
    """Tests for NgrokTunnelManager."""

    @pytest.fixture
    def mock_node(self):
        """Create a mock BaseNode."""
        node = Mock()
        node.name = "test-node"
        node.logdir = tempfile.gettempdir()
        node.ssh_login_info = {
            "hostname": "192.168.1.100",
            "user": "centos",
        }
        return node

    @pytest.fixture
    def mock_tunnel(self):
        """Create a mock ngrok tunnel."""
        tunnel = Mock()
        tunnel.public_url = "https://abc123.ngrok.io"
        return tunnel

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_get_container_run_args_creates_tunnel(self, mock_node, mock_tunnel):
        """Test that NgrokTunnelManager creates a tunnel."""
        with patch('sdcm.utils.reverse_tunnel.ngrok') as mock_ngrok:
            mock_ngrok.connect.return_value = mock_tunnel
            manager = NgrokTunnelManager(mock_node, "ldap")

            args = manager.get_container_run_args(local_port=5001, remote_port=389)

            assert args["image"] == "ngrok-tunnel"
            assert "test-node-ngrok-ldap" in args["name"]
            assert args["environment"]["TUNNEL_TYPE"] == "ngrok"
            assert args["environment"]["NGROK_URL"] == "https://abc123.ngrok.io"
            assert args["environment"]["LOCAL_PORT"] == "5001"
            assert args["environment"]["REMOTE_PORT"] == "389"

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_tunnel_reuse(self, mock_node, mock_tunnel):
        """Test that creating multiple managers for the same port reuses the tunnel."""
        with patch('sdcm.utils.reverse_tunnel.ngrok') as mock_ngrok:
            mock_ngrok.connect.return_value = mock_tunnel
            # Create first manager
            manager1 = NgrokTunnelManager(mock_node, "service1")
            manager1.get_container_run_args(local_port=5000, remote_port=8000)

            # Create second manager with same local port
            manager2 = NgrokTunnelManager(mock_node, "service2")
            manager2.get_container_run_args(local_port=5000, remote_port=9000)

            # ngrok.connect should only be called once
            assert mock_ngrok.connect.call_count == 1

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_get_tunnel_url(self, mock_node, mock_tunnel):
        """Test that get_tunnel_url returns correct URL."""
        with patch('sdcm.utils.reverse_tunnel.ngrok') as mock_ngrok:
            mock_ngrok.connect.return_value = mock_tunnel
            manager = NgrokTunnelManager(mock_node, "test")
            manager.get_container_run_args(local_port=5555, remote_port=8888)

            url = NgrokTunnelManager.get_tunnel_url(5555)

            assert url == "https://abc123.ngrok.io"

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_disconnect_all(self, mock_node, mock_tunnel):
        """Test that disconnect_all closes all tunnels."""
        with patch('sdcm.utils.reverse_tunnel.ngrok') as mock_ngrok:
            mock_ngrok.connect.return_value = mock_tunnel
            # Create tunnels
            manager1 = NgrokTunnelManager(mock_node, "service1")
            manager1.get_container_run_args(local_port=5000, remote_port=8000)

            manager2 = NgrokTunnelManager(mock_node, "service2")
            manager2.get_container_run_args(local_port=6000, remote_port=9000)

            # Disconnect all
            NgrokTunnelManager.disconnect_all()

            # Both tunnels should be disconnected
            assert mock_ngrok.disconnect.call_count == 2
            assert len(NgrokTunnelManager._active_tunnels) == 0


class TestTunnelManagerFactory:
    """Tests for the get_tunnel_manager factory function."""

    @pytest.fixture
    def mock_node(self):
        """Create a mock BaseNode."""
        node = Mock()
        node.name = "test-node"
        node.logdir = tempfile.gettempdir()
        node.ssh_login_info = {
            "hostname": "192.168.1.100",
            "user": "centos",
            "key_file": "/path/to/key.pem",
        }
        return node

    def test_get_autossh_manager(self, mock_node):
        """Test that factory returns AutosshTunnelManager for 'autossh'."""
        manager = get_tunnel_manager(mock_node, "test", "autossh")

        assert isinstance(manager, AutosshTunnelManager)
        assert manager.service_name == "test"

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_get_ngrok_manager(self, mock_node):
        """Test that factory returns NgrokTunnelManager for 'ngrok'."""
        manager = get_tunnel_manager(mock_node, "test", "ngrok")

        assert isinstance(manager, NgrokTunnelManager)
        assert manager.service_name == "test"

    def test_invalid_tunnel_mode(self, mock_node):
        """Test that factory raises ValueError for invalid tunnel mode."""
        with pytest.raises(ValueError, match="Unknown tunnel mode"):
            get_tunnel_manager(mock_node, "test", "invalid")

    @pytest.mark.skipif(not PYNGROK_AVAILABLE, reason="pyngrok not installed")
    def test_case_insensitive(self, mock_node):
        """Test that tunnel mode is case-insensitive."""
        manager1 = get_tunnel_manager(mock_node, "test", "AUTOSSH")
        manager2 = get_tunnel_manager(mock_node, "test", "NgRoK")

        assert isinstance(manager1, AutosshTunnelManager)
        assert isinstance(manager2, NgrokTunnelManager)
