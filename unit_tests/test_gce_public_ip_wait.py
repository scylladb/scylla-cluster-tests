"""Unit tests for GCE public IP waiting behavior."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from google.cloud import compute_v1

from sdcm.cluster_gce import GCENode


class TestGCEPublicIPWait:
    """Test that GCE nodes conditionally wait for public IPs based on configuration."""

    def _create_mock_node_with_params(self, params):
        """Helper to create a mock GCENode with given params.
        
        This avoids patching __init__ by creating a properly initialized mock.
        """
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        mock_instance.machine_type = "projects/test-project/zones/us-east1-b/machineTypes/n2-standard-2"
        
        mock_service = Mock(spec=compute_v1.InstancesClient)
        mock_cluster = Mock()
        mock_cluster.params = params
        
        # Create a mock node with necessary attributes
        node = Mock(spec=GCENode)
        node.params = params
        node._instance = mock_instance
        node._gce_service = mock_service
        node.project = "test-project"
        node.zone = "us-east1-b"
        node.log = Mock()
        
        # Bind the actual methods from GCENode to the mock
        node._needs_public_ip = lambda: GCENode._needs_public_ip(node)
        node.init = lambda: GCENode.init(node)
        node._wait_public_ip = Mock()
        node._wait_private_ip = Mock()
        
        return node

    def test_needs_public_ip_when_intra_node_comm_public(self):
        """Test that _needs_public_ip returns True when intra_node_comm_public=True."""
        params = {"intra_node_comm_public": True, "ip_ssh_connections": "private"}
        node = self._create_mock_node_with_params(params)
        
        assert node._needs_public_ip() is True

    def test_needs_public_ip_when_ssh_connection_type_public(self):
        """Test that _needs_public_ip returns True when ssh_connection_ip_type='public'."""
        params = {"intra_node_comm_public": False, "ip_ssh_connections": "public"}
        
        with patch('sdcm.cluster_gce.ssh_connection_ip_type', return_value='public'):
            node = self._create_mock_node_with_params(params)
            assert node._needs_public_ip() is True

    def test_needs_public_ip_false_for_private_connections(self):
        """Test that _needs_public_ip returns False for private connections (default)."""
        params = {"intra_node_comm_public": False, "ip_ssh_connections": "private"}
        
        with patch('sdcm.cluster_gce.ssh_connection_ip_type', return_value='private'):
            node = self._create_mock_node_with_params(params)
            assert node._needs_public_ip() is False

    def test_init_waits_for_public_ip_when_needed(self):
        """Test that init() calls _wait_public_ip when public IP is needed."""
        params = {"intra_node_comm_public": True, "ip_ssh_connections": "private"}
        node = self._create_mock_node_with_params(params)
        
        with patch('time.sleep'):  # Skip the 10-second sleep
            node.init()
        
        assert node._wait_public_ip.called, "_wait_public_ip should be called when needed"
        assert not node._wait_private_ip.called, "_wait_private_ip should not be called"

    def test_init_waits_for_private_ip_when_public_not_needed(self):
        """Test that init() calls _wait_private_ip when public IP is not needed."""
        params = {"intra_node_comm_public": False, "ip_ssh_connections": "private"}
        
        with patch('sdcm.cluster_gce.ssh_connection_ip_type', return_value='private'):
            node = self._create_mock_node_with_params(params)
            
            with patch('time.sleep'):  # Skip the 10-second sleep
                node.init()
            
            assert not node._wait_public_ip.called, "_wait_public_ip should not be called"
            assert node._wait_private_ip.called, "_wait_private_ip should be called when public IP not needed"

    def test_wait_public_ip_timeout_with_no_ip(self):
        """Test that _wait_public_ip times out when no public IP is assigned."""
        params = {"intra_node_comm_public": True}
        node = self._create_mock_node_with_params(params)
        
        # Restore the real _wait_public_ip method for this test
        node._wait_public_ip = lambda: GCENode._wait_public_ip(node)
        
        # Mock _refresh_instance_state to always return no public IPs
        node._refresh_instance_state = Mock(return_value=([], ["10.0.0.1"]))
        
        # Should raise AssertionError after retries
        with pytest.raises(AssertionError) as exc_info:
            node._wait_public_ip()
        
        assert "has no public IP address" in str(exc_info.value)

    def test_wait_public_ip_success_with_ip_assigned(self):
        """Test that _wait_public_ip succeeds when public IP is assigned."""
        params = {"intra_node_comm_public": True}
        node = self._create_mock_node_with_params(params)
        
        # Restore the real _wait_public_ip method for this test
        node._wait_public_ip = lambda: GCENode._wait_public_ip(node)
        
        # Mock _refresh_instance_state to return a public IP
        node._refresh_instance_state = Mock(return_value=(["34.123.45.67"], ["10.0.0.1"]))
        
        # Should complete without raising
        node._wait_public_ip()
        
        # Verify debug log was called
        assert node.log.debug.called

