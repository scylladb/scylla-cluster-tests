"""Unit tests for GCE public IP waiting behavior."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from google.cloud import compute_v1

from sdcm.cluster_gce import GCENode
from sdcm.test_config import TestConfig


class TestGCEPublicIPWait:
    """Test that GCE nodes conditionally wait for public IPs based on configuration."""

    def test_wait_public_ip_when_intra_node_comm_public(self):
        """Test that GCENode waits for public IP when intra_node_comm_public=True."""
        # Create mock GCE instance
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        mock_instance.machine_type = "projects/test-project/zones/us-east1-b/machineTypes/n2-standard-2"
        
        # Create mock GCE service
        mock_service = Mock(spec=compute_v1.InstancesClient)
        
        # Create mock params with intra_node_comm_public=True
        mock_params = {
            "intra_node_comm_public": True,
            "user_credentials_path": "/tmp/test.pem",
        }
        
        # Create mock parent cluster
        mock_cluster = Mock()
        mock_cluster.params = mock_params
        
        # Patch methods that would be called during init
        with patch.object(GCENode, '_wait_public_ip') as mock_wait_public, \
             patch.object(GCENode, '_wait_private_ip') as mock_wait_private, \
             patch.object(GCENode, '__init__', lambda self, *args, **kwargs: None):
            
            node = GCENode(
                gce_instance=mock_instance,
                gce_service=mock_service,
                credentials=Mock(key_file="/tmp/test.pem"),
                parent_cluster=mock_cluster,
                gce_project="test-project",
            )
            node.params = mock_params
            node.log = Mock()
            
            # Manually set required attributes
            node._instance = mock_instance
            
            # Call init to test the logic
            GCENode.init(node)
            
            # Verify that _wait_public_ip was called and _wait_private_ip was not
            assert mock_wait_public.called, "_wait_public_ip should be called when intra_node_comm_public=True"
            assert not mock_wait_private.called, "_wait_private_ip should not be called when intra_node_comm_public=True"

    def test_wait_public_ip_when_ssh_connection_type_public(self):
        """Test that GCENode waits for public IP when ssh_connection_ip_type returns 'public'."""
        # Create mock GCE instance
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        mock_instance.machine_type = "projects/test-project/zones/us-east1-b/machineTypes/n2-standard-2"
        
        # Create mock GCE service
        mock_service = Mock(spec=compute_v1.InstancesClient)
        
        # Create mock params with ip_ssh_connections='public'
        mock_params = {
            "intra_node_comm_public": False,
            "ip_ssh_connections": "public",
            "user_credentials_path": "/tmp/test.pem",
        }
        
        # Create mock parent cluster
        mock_cluster = Mock()
        mock_cluster.params = mock_params
        
        # Patch methods that would be called during init
        with patch.object(GCENode, '_wait_public_ip') as mock_wait_public, \
             patch.object(GCENode, '_wait_private_ip') as mock_wait_private, \
             patch('sdcm.cluster_gce.ssh_connection_ip_type', return_value='public'), \
             patch.object(GCENode, '__init__', lambda self, *args, **kwargs: None):
            
            node = GCENode(
                gce_instance=mock_instance,
                gce_service=mock_service,
                credentials=Mock(key_file="/tmp/test.pem"),
                parent_cluster=mock_cluster,
                gce_project="test-project",
            )
            node.params = mock_params
            node.log = Mock()
            node._instance = mock_instance
            
            # Call init to test the logic
            GCENode.init(node)
            
            # Verify that _wait_public_ip was called and _wait_private_ip was not
            assert mock_wait_public.called, "_wait_public_ip should be called when ssh_connection_ip_type='public'"
            assert not mock_wait_private.called, "_wait_private_ip should not be called when ssh_connection_ip_type='public'"

    def test_wait_private_ip_when_using_private_connections(self):
        """Test that GCENode waits for private IP when using private connections (default)."""
        # Create mock GCE instance
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        mock_instance.machine_type = "projects/test-project/zones/us-east1-b/machineTypes/n2-standard-2"
        
        # Create mock GCE service
        mock_service = Mock(spec=compute_v1.InstancesClient)
        
        # Create mock params with default settings (private connections)
        mock_params = {
            "intra_node_comm_public": False,
            "ip_ssh_connections": "private",
            "user_credentials_path": "/tmp/test.pem",
        }
        
        # Create mock parent cluster
        mock_cluster = Mock()
        mock_cluster.params = mock_params
        
        # Patch methods that would be called during init
        with patch.object(GCENode, '_wait_public_ip') as mock_wait_public, \
             patch.object(GCENode, '_wait_private_ip') as mock_wait_private, \
             patch('sdcm.cluster_gce.ssh_connection_ip_type', return_value='private'), \
             patch.object(GCENode, '__init__', lambda self, *args, **kwargs: None):
            
            node = GCENode(
                gce_instance=mock_instance,
                gce_service=mock_service,
                credentials=Mock(key_file="/tmp/test.pem"),
                parent_cluster=mock_cluster,
                gce_project="test-project",
            )
            node.params = mock_params
            node.log = Mock()
            node._instance = mock_instance
            
            # Call init to test the logic
            GCENode.init(node)
            
            # Verify that _wait_private_ip was called and _wait_public_ip was not
            assert not mock_wait_public.called, "_wait_public_ip should not be called when using private connections"
            assert mock_wait_private.called, "_wait_private_ip should be called when using private connections"

    def test_wait_public_ip_timeout(self):
        """Test that _wait_public_ip times out after 15 minutes when no public IP is assigned."""
        # Create mock GCE instance without public IP
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        mock_instance.network_interfaces = []
        
        # Create mock GCE service
        mock_service = Mock(spec=compute_v1.InstancesClient)
        mock_service.get.return_value = mock_instance
        
        # Create node with mocked dependencies
        with patch.object(GCENode, '__init__', lambda self, *args, **kwargs: None):
            node = GCENode(
                gce_instance=mock_instance,
                gce_service=mock_service,
                credentials=Mock(key_file="/tmp/test.pem"),
                parent_cluster=Mock(),
                gce_project="test-project",
            )
            node._instance = mock_instance
            node._gce_service = mock_service
            node.project = "test-project"
            node.zone = "us-east1-b"
            node.log = Mock()
            
            # Mock _refresh_instance_state to return empty public IPs
            with patch.object(node, '_refresh_instance_state', return_value=([], ["10.0.0.1"])):
                # The method should raise AssertionError after retries are exhausted
                with pytest.raises(AssertionError) as exc_info:
                    node._wait_public_ip()
                
                assert "has no public IP address" in str(exc_info.value)

    def test_wait_public_ip_success(self):
        """Test that _wait_public_ip succeeds when public IP is assigned."""
        # Create mock GCE instance with public IP
        mock_instance = Mock(spec=compute_v1.Instance)
        mock_instance.name = "test-instance"
        mock_instance.zone = "projects/test-project/zones/us-east1-b"
        
        # Create mock GCE service
        mock_service = Mock(spec=compute_v1.InstancesClient)
        
        # Create node with mocked dependencies
        with patch.object(GCENode, '__init__', lambda self, *args, **kwargs: None):
            node = GCENode(
                gce_instance=mock_instance,
                gce_service=mock_service,
                credentials=Mock(key_file="/tmp/test.pem"),
                parent_cluster=Mock(),
                gce_project="test-project",
            )
            node._instance = mock_instance
            node._gce_service = mock_service
            node.project = "test-project"
            node.zone = "us-east1-b"
            node.log = Mock()
            
            # Mock _refresh_instance_state to return a public IP
            with patch.object(node, '_refresh_instance_state', return_value=(["34.123.45.67"], ["10.0.0.1"])):
                # Should complete without raising an exception
                node._wait_public_ip()
                
                # Verify log was called
                assert node.log.debug.called
