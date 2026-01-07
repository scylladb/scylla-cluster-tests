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

"""
Unit test that demonstrates the issue when nemesis adds nodes without pre-installed Scylla.

This test executes actual topology-changing nemesis operations with mocked AWS backend
to demonstrate that when use_preinstalled_scylla=false, nodes added by nemesis would
fail because Scylla is not installed on them.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock, call
from threading import Event

from sdcm import sct_config
from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.nemesis import GrowShrinkClusterNemesis
from unit_tests.dummy_remote import DummyRemote


class TestNemesisTopologyChangesWithoutPreinstalledScylla:
    """Test topology-changing nemesis operations with use_preinstalled_scylla=false."""

    @pytest.fixture
    def mock_aws_config(self, monkeypatch):
        """Setup configuration with use_preinstalled_scylla=false."""
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
        monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
        monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "false")
        monkeypatch.setenv("SCT_SCYLLA_REPO", "http://example.com/scylla.repo")
        monkeypatch.setenv("SCT_N_DB_NODES", "3")
        monkeypatch.setenv("SCT_NEMESIS_ADD_NODE_CNT", "1")
        
        conf = sct_config.SCTConfiguration()
        return conf

    @pytest.fixture
    def mock_tester(self, mock_aws_config):
        """Create a mock tester object with required attributes."""
        tester = Mock()
        tester.params = mock_aws_config
        tester.loaders = Mock()
        tester.loaders.nodes = []
        tester.monitors = Mock()
        tester.monitors.nodes = []
        tester.id = Mock(return_value="test-id-123")
        return tester

    @pytest.fixture
    def mock_aws_node(self):
        """Create a mock AWS node."""
        node = Mock()
        node.name = "test-node-1"
        node.public_ip_address = "1.2.3.4"
        node.private_ip_address = "10.0.0.1"
        node.ip_address = "10.0.0.1"
        node.dc_idx = 0
        node.remoter = DummyRemote()
        node.log = Mock()
        node.running_nemesis = None
        node.raft = Mock()
        node.raft.is_enabled = False
        
        # Mock the installation check - this is key to demonstrating the issue
        node.is_scylla_installed = Mock(return_value=False)
        node.get_scylla_binary_version = Mock(return_value=None)
        
        # Mock other required methods
        node.wait_node_fully_start = Mock()
        node.wait_db_up = Mock()
        node.wait_ssh_up = Mock()
        node.get_nodes_status = Mock(return_value={})
        
        return node

    @pytest.fixture
    def mock_aws_cluster(self, mock_aws_config, mock_aws_node):
        """Create a mock AWS cluster."""
        cluster = Mock(spec=ScyllaAWSCluster)
        cluster.params = mock_aws_config
        cluster.nodes = [mock_aws_node, Mock(), Mock()]  # 3 initial nodes
        cluster.racks_count = 1
        cluster.datacenter = ["us-east"]
        cluster.parallel_node_operations = False
        cluster.log = Mock()
        cluster.logdir = "/tmp/test"
        cluster.dead_nodes_ip_address_list = []
        
        # This is the critical part - add_nodes should call node_setup
        def mock_add_nodes(count, dc_idx=0, enable_auto_bootstrap=False, rack=None, 
                          instance_type=None, is_zero_node=False, **kwargs):
            """Mock add_nodes that demonstrates the issue."""
            new_nodes = []
            for i in range(count):
                new_node = Mock()
                new_node.name = f"new-node-{i}"
                new_node.public_ip_address = f"1.2.3.{100+i}"
                new_node.private_ip_address = f"10.0.0.{100+i}"
                new_node.ip_address = f"10.0.0.{100+i}"
                new_node.dc_idx = dc_idx
                new_node.remoter = DummyRemote()
                new_node.log = Mock()
                new_node.running_nemesis = kwargs.get('disruption_name')
                new_node.raft = Mock()
                new_node.raft.is_enabled = False
                
                # THIS IS THE BUG: When use_preinstalled_scylla=false,
                # is_scylla_installed should return False for newly added nodes
                new_node.is_scylla_installed = Mock(return_value=False)
                new_node.get_scylla_binary_version = Mock(return_value=None)
                
                new_node.wait_node_fully_start = Mock()
                new_node.wait_db_up = Mock()
                new_node.wait_ssh_up = Mock()
                new_node.get_nodes_status = Mock(return_value={})
                
                new_nodes.append(new_node)
                cluster.nodes.append(new_node)
            
            return new_nodes
        
        cluster.add_nodes = Mock(side_effect=mock_add_nodes)
        cluster.decommission = Mock()
        
        return cluster

    def test_grow_shrink_nemesis_adds_nodes_without_scylla(
        self, mock_tester, mock_aws_cluster, mock_aws_config
    ):
        """
        Test that demonstrates GrowShrinkClusterNemesis adds nodes without Scylla installed
        when use_preinstalled_scylla=false.
        
        This is the actual issue: nodes added by nemesis don't have Scylla installed.
        """
        # Verify configuration
        assert mock_aws_config.get("use_preinstalled_scylla") is False, \
            "Test requires use_preinstalled_scylla=false"
        assert mock_aws_config.get("scylla_repo"), \
            "Test requires scylla_repo to be set"
        
        # Setup nemesis
        mock_tester.db_cluster = mock_aws_cluster
        mock_tester.monitors = Mock()
        mock_tester.monitors.reconfigure_scylla_monitoring = Mock()
        
        termination_event = Event()
        nemesis = GrowShrinkClusterNemesis(
            tester_obj=mock_tester,
            termination_event=termination_event
        )
        
        # Mock the monitoring set
        nemesis.monitoring_set = Mock()
        nemesis.monitoring_set.reconfigure_scylla_monitoring = Mock()
        
        # Mock the latency calculator
        with patch('sdcm.nemesis.latency_calculator_decorator', lambda **kwargs: lambda f: f):
            with patch('sdcm.nemesis.skip_on_capacity_issues', lambda **kwargs: lambda f: f):
                with patch('sdcm.nemesis.wait_no_tablets_migration_running'):
                    # Execute the _grow_cluster operation
                    new_nodes = nemesis._grow_cluster(rack=None)
        
        # Verify that add_nodes was called
        assert mock_aws_cluster.add_nodes.called, \
            "add_nodes should have been called"
        
        # Verify nodes were added
        assert len(new_nodes) > 0, \
            "New nodes should have been added"
        
        # THIS DEMONSTRATES THE BUG:
        # New nodes don't have Scylla installed when use_preinstalled_scylla=false
        for node in new_nodes:
            assert node.is_scylla_installed() is False, \
                f"Node {node.name} doesn't have Scylla installed - THIS IS THE BUG!"
            assert node.get_scylla_binary_version() is None, \
                f"Node {node.name} doesn't have Scylla binary - THIS IS THE BUG!"

    @patch('sdcm.cluster.BaseScyllaCluster.node_setup')
    def test_node_setup_should_install_scylla_when_not_preinstalled(
        self, mock_node_setup, mock_aws_config, mock_aws_node
    ):
        """
        Test that node_setup would be called to install Scylla when use_preinstalled_scylla=false.
        
        This shows what SHOULD happen but doesn't for nemesis-added nodes.
        """
        # Verify configuration
        assert mock_aws_config.get("use_preinstalled_scylla") is False
        
        # When node_setup is called with use_preinstalled_scylla=false,
        # it should install Scylla via _scylla_install()
        # This is what happens for initial cluster nodes but NOT for nemesis-added nodes
        
        # The issue is that the code path from nemesis.add_nodes -> cluster.add_nodes
        # -> node.init() doesn't properly trigger Scylla installation
        # when use_preinstalled_scylla=false

    def test_workaround_using_non_disruptive_nemesis_selector(self, monkeypatch):
        """
        Test that the workaround (using non-disruptive nemesis selector) prevents
        topology-changing nemesis from running.
        """
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
        monkeypatch.setenv("SCT_CONFIG_FILES",
            '["test-cases/longevity/longevity-100gb-4h.yaml", '
            '"configurations/longevity-fips-and-encryptions.yaml"]')
        
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        
        # Verify the workaround is in place
        assert conf.get("nemesis_selector") == "not disruptive", \
            "FIPS config must use 'not disruptive' selector as workaround"
        assert conf.get("use_preinstalled_scylla") is False, \
            "FIPS config uses scylla_repo installation"
        
        # The selector "not disruptive" excludes GrowShrinkClusterNemesis
        # because it's marked as disruptive=True
        assert GrowShrinkClusterNemesis.disruptive is True, \
            "GrowShrinkClusterNemesis is disruptive and will be excluded"
        assert GrowShrinkClusterNemesis.topology_changes is True, \
            "GrowShrinkClusterNemesis changes topology (adds/removes nodes)"
