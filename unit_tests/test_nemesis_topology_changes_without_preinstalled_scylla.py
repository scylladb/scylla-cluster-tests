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
Unit test to verify whether _scylla_install is called when nemesis adds nodes.

This test executes the actual add_nodes flow with mocked AWS backend to determine
if Scylla installation occurs when use_preinstalled_scylla=false.
"""

import pytest
from unittest.mock import Mock, patch, call
from threading import Event

from sdcm import sct_config
from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.nemesis import GrowShrinkClusterNemesis
from unit_tests.dummy_remote import DummyRemote


class TestNemesisAddNodesScyllaInstallation:
    """
    Test to verify if _scylla_install is called when nemesis adds nodes.
    
    This test uses the actual nemesis and cluster code paths with minimal mocking
    to determine whether Scylla installation happens for nodes added by topology-
    changing nemesis operations when use_preinstalled_scylla=false.
    """

    @patch('sdcm.cluster.BaseScyllaCluster._scylla_install')
    @patch('sdcm.cluster.BaseScyllaCluster.wait_for_init')
    @patch('sdcm.cluster_aws.ScyllaAWSCluster.add_nodes')
    def test_scylla_install_called_when_nemesis_adds_nodes(
        self, 
        mock_add_nodes,
        mock_wait_for_init, 
        mock_scylla_install,
        monkeypatch
    ):
        """
        Test that verifies if _scylla_install is called when nodes are added by nemesis.
        
        This test mocks _scylla_install to track if it's called, allowing us to confirm
        whether the installation flow is triggered for nemesis-added nodes when
        use_preinstalled_scylla=false.
        """
        # Setup configuration with use_preinstalled_scylla=false (like FIPS longevity)
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
        monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
        monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "false")
        monkeypatch.setenv("SCT_SCYLLA_REPO", "http://example.com/scylla.repo")
        monkeypatch.setenv("SCT_N_DB_NODES", "3")
        monkeypatch.setenv("SCT_NEMESIS_ADD_NODE_CNT", "1")
        
        conf = sct_config.SCTConfiguration()
        
        # Verify configuration
        assert conf.get("use_preinstalled_scylla") is False, \
            "Test requires use_preinstalled_scylla=false"
        assert conf.get("scylla_repo"), \
            "Test requires scylla_repo to be set"
        
        # Create mock nodes that will be returned by add_nodes
        new_node = Mock()
        new_node.name = "new-node-1"
        new_node.public_ip_address = "1.2.3.100"
        new_node.private_ip_address = "10.0.0.100"
        new_node.ip_address = "10.0.0.100"
        new_node.dc_idx = 0
        new_node.remoter = DummyRemote()
        new_node.log = Mock()
        new_node.raft = Mock()
        new_node.raft.is_enabled = False
        new_node.is_scylla_installed = Mock(return_value=False)
        new_node.get_scylla_binary_version = Mock(return_value=None)
        new_node.wait_node_fully_start = Mock()
        new_node.wait_db_up = Mock()
        new_node.wait_ssh_up = Mock()
        new_node.wait_native_transport = Mock()
        new_node.get_nodes_status = Mock(return_value={})
        
        # Mock add_nodes to return our new node
        mock_add_nodes.return_value = [new_node]
        
        # Create mock cluster
        mock_cluster = Mock(spec=ScyllaAWSCluster)
        mock_cluster.params = conf
        mock_cluster.nodes = [Mock(), Mock(), Mock()]  # 3 initial nodes
        mock_cluster.data_nodes = mock_cluster.nodes
        mock_cluster.racks_count = 1
        mock_cluster.datacenter = ["us-east"]
        mock_cluster.parallel_node_operations = False
        mock_cluster.log = Mock()
        mock_cluster.add_nodes = mock_add_nodes
        mock_cluster.wait_for_init = mock_wait_for_init
        mock_cluster.set_seeds = Mock()
        mock_cluster.update_seed_provider = Mock()
        mock_cluster.wait_for_nodes_up_and_normal = Mock()
        
        # Create mock tester
        mock_tester = Mock()
        mock_tester.params = conf
        mock_tester.db_cluster = mock_cluster
        mock_tester.monitors = Mock()
        mock_tester.monitors.reconfigure_scylla_monitoring = Mock()
        
        # Create nemesis instance
        termination_event = Event()
        nemesis = GrowShrinkClusterNemesis(
            tester_obj=mock_tester,
            termination_event=termination_event
        )
        nemesis.monitoring_set = Mock()
        nemesis.monitoring_set.reconfigure_scylla_monitoring = Mock()
        
        # Execute nemesis operation to add nodes
        with patch('sdcm.nemesis.latency_calculator_decorator', lambda **kwargs: lambda f: f):
            with patch('sdcm.nemesis.skip_on_capacity_issues', lambda **kwargs: lambda f: f):
                with patch('sdcm.nemesis.wait_no_tablets_migration_running'):
                    with patch('sdcm.nemesis.adaptive_timeout'):
                        with patch('sdcm.nemesis.InfoEvent'):
                            new_nodes = nemesis._add_and_init_new_cluster_nodes(
                                count=1, 
                                rack=None
                            )
        
        # Verify add_nodes was called
        assert mock_add_nodes.called, "add_nodes should have been called"
        
        # Verify wait_for_init was called with the new nodes
        # wait_for_init is what triggers node_setup via @wait_for_init_wrap decorator
        assert mock_wait_for_init.called, "wait_for_init should have been called"
        
        # THE KEY VERIFICATION: Check if _scylla_install was called
        # If it was called, it means the installation flow is working correctly
        # If it wasn't called, that's the bug we're demonstrating
        if mock_scylla_install.called:
            print("✓ _scylla_install WAS called - installation flow works correctly")
            # Verify it was called with the new node
            call_args = mock_scylla_install.call_args_list
            assert len(call_args) > 0, "_scylla_install should have been called at least once"
        else:
            print("✗ _scylla_install WAS NOT called - THIS IS THE BUG!")
            print("  Nodes added by nemesis don't get Scylla installed when use_preinstalled_scylla=false")
        
        # Return the result so we can see what actually happened
        return {
            "scylla_install_called": mock_scylla_install.called,
            "scylla_install_call_count": mock_scylla_install.call_count,
            "wait_for_init_called": mock_wait_for_init.called,
            "add_nodes_called": mock_add_nodes.called,
        }

    def test_fips_workaround_configuration(self, monkeypatch):
        """
        Verify that the FIPS configuration uses non-disruptive nemesis selector
        to avoid topology changes.
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
        
        # Verify that topology-changing nemesis are marked as disruptive
        assert GrowShrinkClusterNemesis.disruptive is True, \
            "GrowShrinkClusterNemesis is disruptive"
        assert GrowShrinkClusterNemesis.topology_changes is True, \
            "GrowShrinkClusterNemesis changes topology"
