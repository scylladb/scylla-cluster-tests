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

"""Unit tests for Docker backend Scylla Manager support"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from sdcm.cluster_docker import DockerManagerNode, ManagerSetDocker, ScyllaDockerCluster
from sdcm.test_config import TestConfig


@pytest.fixture
def mock_params():
    """Mock parameters for testing"""
    return {
        "mgmt_docker_image": "scylladb/scylla-manager:3.8.0",
        "user_prefix": "test",
        "docker_network": "bridge",
        "use_mgmt": True,
    }


@pytest.fixture
def mock_container_manager():
    """Mock ContainerManager for testing"""
    with patch("sdcm.cluster_docker.ContainerManager") as mock_cm:
        mock_container = Mock()
        mock_container.labels = {"NodeIndex": "1"}
        mock_container.name = "test-manager-node-1"
        mock_cm.get_container.return_value = mock_container
        mock_cm.run_container.return_value = None
        mock_cm.wait_for_status.return_value = None
        mock_cm.is_running.return_value = True
        yield mock_cm


class TestDockerManagerNode:
    """Tests for DockerManagerNode class"""

    def test_node_container_image_tag(self, mock_params):
        """Test that manager node uses mgmt_docker_image"""
        with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_cluster:
            mock_cluster.params = mock_params
            node = DockerManagerNode(
                parent_cluster=mock_cluster,
                node_prefix="manager-node",
                node_index=1,
            )
            node.parent_cluster.params = mock_params

            assert node.node_container_image_tag == "scylladb/scylla-manager:3.8.0"

    def test_node_container_image_tag_missing(self):
        """Test that missing mgmt_docker_image raises ValueError"""
        with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_cluster:
            mock_cluster.params = {"mgmt_docker_image": None}
            node = DockerManagerNode(
                parent_cluster=mock_cluster,
                node_prefix="manager-node",
                node_index=1,
            )
            node.parent_cluster.params = {"mgmt_docker_image": None}

            with pytest.raises(ValueError, match="mgmt_docker_image parameter is required"):
                _ = node.node_container_image_tag

    def test_node_type(self):
        """Test that node_type property returns 'manager'"""
        with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_cluster:
            node = DockerManagerNode(
                parent_cluster=mock_cluster,
                node_prefix="manager-node",
                node_index=1,
            )
            assert node.node_type == "manager"

    def test_node_container_run_args(self, mock_params):
        """Test that container run args are configured correctly"""
        with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_cluster:
            mock_cluster.params = mock_params
            node = DockerManagerNode(
                parent_cluster=mock_cluster,
                node_prefix="manager-node",
                node_index=1,
            )
            node.parent_cluster.params = mock_params
            node.name = "test-manager-node-1"

            run_args = node.node_container_run_args()

            assert run_args["name"] == "test-manager-node-1"
            assert run_args["image"] == "scylladb/scylla-manager:3.8.0"
            assert run_args["network"] == "bridge"
            assert "/var/run/docker.sock" in run_args["volumes"]


class TestManagerSetDocker:
    """Tests for ManagerSetDocker class"""

    def test_init_with_mgmt_docker_image(self, mock_params):
        """Test ManagerSetDocker initialization with mgmt_docker_image"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__") as mock_init:
            mock_init.return_value = None

            manager_set = ManagerSetDocker(
                user_prefix="test",
                n_nodes=1,
                params=mock_params,
            )

            # Verify that __init__ was called with correct parameters
            mock_init.assert_called_once()
            call_kwargs = mock_init.call_args[1]
            assert call_kwargs["docker_image"] == "scylladb/scylla-manager"
            assert call_kwargs["docker_image_tag"] == "3.8.0"
            assert call_kwargs["node_type"] == "manager"

    def test_init_without_mgmt_docker_image(self):
        """Test ManagerSetDocker initialization fails without mgmt_docker_image"""
        params = {"mgmt_docker_image": None}

        with pytest.raises(ValueError, match="mgmt_docker_image parameter is required"):
            ManagerSetDocker(
                user_prefix="test",
                n_nodes=1,
                params=params,
            )

    def test_init_with_image_no_tag(self):
        """Test ManagerSetDocker initialization with image but no tag"""
        params = {
            "mgmt_docker_image": "scylladb/scylla-manager",
            "user_prefix": "test",
            "docker_network": "bridge",
        }

        with patch("sdcm.cluster_docker.DockerCluster.__init__") as mock_init:
            mock_init.return_value = None

            ManagerSetDocker(
                user_prefix="test",
                n_nodes=1,
                params=params,
            )

            # Verify that default tag is 'latest'
            call_kwargs = mock_init.call_args[1]
            assert call_kwargs["docker_image"] == "scylladb/scylla-manager"
            assert call_kwargs["docker_image_tag"] == "latest"

    def test_create_node(self, mock_params, mock_container_manager):
        """Test _create_node creates DockerManagerNode"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            with patch("sdcm.cluster_docker.DockerManagerNode") as mock_node_class:
                mock_node = Mock()
                mock_node.init = Mock()
                mock_node_class.return_value = mock_node

                manager_set = ManagerSetDocker(
                    user_prefix="test",
                    n_nodes=1,
                    params=mock_params,
                )
                manager_set.logdir = "/tmp/test"
                manager_set.node_prefix = "test-manager-node"
                manager_set.node_container_user = "scylla-test"
                manager_set.node_container_key_file = None

                node = manager_set._create_node(node_index=1)

                # Verify node was created and initialized
                assert node == mock_node
                mock_node.init.assert_called_once()
                mock_container_manager.run_container.assert_called_once()
                mock_container_manager.wait_for_status.assert_called_once()


class TestScyllaDockerClusterManager:
    """Tests for manager integration in ScyllaDockerCluster"""

    def test_init_manager_node_when_enabled(self, mock_params):
        """Test _init_manager_node creates manager when use_mgmt is True"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_manager_set_class:
                mock_manager_set = Mock()
                mock_manager_node = Mock()
                mock_manager_node.name = "test-manager-node-1"
                mock_manager_set.nodes = [mock_manager_node]
                mock_manager_set.add_nodes = Mock()
                mock_manager_set_class.return_value = mock_manager_set

                cluster = ScyllaDockerCluster(params=mock_params)
                cluster.params = mock_params

                cluster._init_manager_node()

                # Verify manager set was created and nodes were added
                mock_manager_set_class.assert_called_once()
                mock_manager_set.add_nodes.assert_called_once_with(count=1)
                assert cluster.manager_node == mock_manager_node

    def test_init_manager_node_when_disabled(self):
        """Test _init_manager_node skips creation when use_mgmt is False"""
        params = {
            "use_mgmt": False,
            "user_prefix": "test",
        }

        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            with patch("sdcm.cluster_docker.ManagerSetDocker") as mock_manager_set_class:
                cluster = ScyllaDockerCluster(params=params)
                cluster.params = params

                cluster._init_manager_node()

                # Verify manager set was not created
                mock_manager_set_class.assert_not_called()
                assert cluster.manager_node is None

    def test_scylla_manager_node_property_with_manager(self, mock_params):
        """Test scylla_manager_node property returns manager_node when available"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            cluster = ScyllaDockerCluster(params=mock_params)
            mock_manager_node = Mock()
            cluster.manager_node = mock_manager_node

            assert cluster.scylla_manager_node == mock_manager_node

    def test_scylla_manager_node_property_fallback(self, mock_params):
        """Test scylla_manager_node property falls back to monitors when no manager"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            cluster = ScyllaDockerCluster(params=mock_params)
            cluster.manager_node = None

            # Mock test_config and monitors
            mock_tester = Mock()
            mock_monitor_node = Mock()
            mock_tester.monitors.nodes = [mock_monitor_node]

            with patch.object(cluster, "test_config") as mock_test_config:
                mock_test_config.tester_obj.return_value = mock_tester

                assert cluster.scylla_manager_node == mock_monitor_node

    def test_destroy_with_manager(self, mock_params):
        """Test destroy method cleans up manager node"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            with patch("sdcm.cluster_docker.DockerCluster.destroy") as mock_super_destroy:
                cluster = ScyllaDockerCluster(params=mock_params)
                mock_manager_node = Mock()
                cluster.manager_node = mock_manager_node
                cluster.vector_store_cluster = None

                cluster.destroy()

                # Verify manager node was destroyed
                mock_manager_node.destroy.assert_called_once()
                mock_super_destroy.assert_called_once()

    def test_destroy_without_manager(self, mock_params):
        """Test destroy method works when no manager node exists"""
        with patch("sdcm.cluster_docker.DockerCluster.__init__"):
            with patch("sdcm.cluster_docker.DockerCluster.destroy") as mock_super_destroy:
                cluster = ScyllaDockerCluster(params=mock_params)
                cluster.manager_node = None
                cluster.vector_store_cluster = None

                cluster.destroy()

                # Verify destroy still works
                mock_super_destroy.assert_called_once()
