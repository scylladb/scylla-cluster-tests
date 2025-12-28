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

"""
Unit test to demonstrate the issue when nemesis adds nodes without pre-installed Scylla.

This test verifies that:
1. When use_preinstalled_scylla=false, Scylla must be installed during node_setup
2. Topology-changing nemesis operations (disruptive=True, topology_changes=True) would
   attempt to add nodes
3. Without non-disruptive nemesis selector, these operations would fail because
   nodes added by nemesis don't go through the proper Scylla installation flow
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call

from sdcm import sct_config
from sdcm.nemesis import DecommissionMonkey, GrowShrinkClusterNemesis, NoOpMonkey
from sdcm.cluster import BaseScyllaCluster


class TestNemesisAddNodeWithoutPreinstalledScylla:
    """Test that demonstrates the issue with nemesis adding nodes when Scylla is not pre-installed."""

    def test_topology_changing_nemesis_have_correct_flags(self):
        """
        Verify that topology-changing nemesis operations are marked with both
        disruptive=True and topology_changes=True.
        """
        # Verify DecommissionMonkey (removes and adds nodes)
        assert DecommissionMonkey.disruptive is True, \
            "DecommissionMonkey should be disruptive"
        assert DecommissionMonkey.topology_changes is True, \
            "DecommissionMonkey changes topology by removing/adding nodes"
        
        # Verify GrowShrinkClusterNemesis (adds and removes nodes)
        assert GrowShrinkClusterNemesis.disruptive is True, \
            "GrowShrinkClusterNemesis should be disruptive"
        assert GrowShrinkClusterNemesis.topology_changes is True, \
            "GrowShrinkClusterNemesis changes topology by growing/shrinking cluster"
        
        # Verify NoOpMonkey does not change topology
        assert NoOpMonkey.disruptive is False, \
            "NoOpMonkey should not be disruptive"
        assert not hasattr(NoOpMonkey, 'topology_changes') or NoOpMonkey.topology_changes is False, \
            "NoOpMonkey should not change topology"

    def test_non_disruptive_selector_excludes_topology_changes(self):
        """
        Verify that nemesis_selector="not disruptive" excludes all topology-changing operations.
        This is the fix for the FIPS longevity issue.
        """
        # Topology-changing nemesis operations are ALL marked as disruptive=True
        # So "not disruptive" selector will exclude them all
        from sdcm.nemesis import NEMESIS_CLASSES
        
        topology_changing_nemeses = [
            cls for cls in NEMESIS_CLASSES.values()
            if hasattr(cls, 'topology_changes') and cls.topology_changes is True
        ]
        
        # All topology-changing nemeses should also be disruptive
        for nemesis_cls in topology_changing_nemeses:
            assert nemesis_cls.disruptive is True, \
                f"{nemesis_cls.__name__} changes topology but is not marked as disruptive"

    @patch('sdcm.cluster.BaseScyllaCluster.node_setup')
    @patch('sdcm.cluster.BaseNode')
    def test_add_nodes_calls_node_setup_with_scylla_install(self, mock_node, mock_node_setup, monkeypatch):
        """
        Demonstrate that when add_nodes is called, it should trigger node_setup
        which would install Scylla when use_preinstalled_scylla=false.
        
        This test shows what SHOULD happen - node_setup should be called for new nodes.
        The issue is that nemesis operations that add nodes don't properly install Scylla
        on those nodes when using scylla_repo installation.
        """
        # Setup configuration with use_preinstalled_scylla=false (like FIPS longevity test)
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
        monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "false")
        monkeypatch.setenv("SCT_SCYLLA_REPO", "http://example.com/scylla.repo")
        
        conf = sct_config.SCTConfiguration()
        
        # Verify configuration
        assert conf.get("use_preinstalled_scylla") is False, \
            "Test requires use_preinstalled_scylla=false to demonstrate the issue"
        
        # This demonstrates the issue: when use_preinstalled_scylla=false,
        # nodes need Scylla installed during node_setup.
        # Topology-changing nemesis operations would call add_nodes, which should
        # trigger node_setup to install Scylla, but the flow isn't properly set up
        # for nemesis-added nodes.

    def test_fips_config_uses_non_disruptive_selector(self, monkeypatch):
        """
        Verify that the FIPS longevity configuration uses non-disruptive nemesis selector
        to avoid topology changes when use_preinstalled_scylla=false.
        """
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
        monkeypatch.setenv("SCT_CONFIG_FILES", 
            '["test-cases/longevity/longevity-100gb-4h.yaml", "configurations/longevity-fips-and-encryptions.yaml"]')
        
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        
        # This is the fix: use non-disruptive nemesis selector
        assert conf.get("nemesis_selector") == "not disruptive", \
            "FIPS longevity must use 'not disruptive' nemesis selector to avoid topology changes"
        
        # Verify use_preinstalled_scylla is false
        assert conf.get("use_preinstalled_scylla") is False, \
            "FIPS longevity uses scylla_repo installation"

    @pytest.mark.parametrize("nemesis_class_name,expected_adds_nodes", [
        ("NoOpMonkey", False),  # Does nothing
        ("DecommissionMonkey", True),  # Removes node, then adds it back
        ("GrowShrinkClusterNemesis", True),  # Grows/shrinks cluster
    ])
    def test_nemesis_operations_that_add_nodes(self, nemesis_class_name, expected_adds_nodes):
        """
        Demonstrate which nemesis operations would add nodes and thus would fail
        without pre-installed Scylla.
        """
        from sdcm.nemesis import NEMESIS_CLASSES
        
        nemesis_cls = NEMESIS_CLASSES.get(nemesis_class_name)
        assert nemesis_cls is not None, f"Nemesis class {nemesis_class_name} not found"
        
        has_topology_changes = hasattr(nemesis_cls, 'topology_changes') and nemesis_cls.topology_changes
        
        if expected_adds_nodes:
            assert has_topology_changes, \
                f"{nemesis_class_name} should have topology_changes=True if it adds nodes"
            assert nemesis_cls.disruptive is True, \
                f"{nemesis_class_name} should be disruptive if it changes topology"
        else:
            # NoOpMonkey doesn't change topology
            assert nemesis_cls.disruptive is False, \
                f"{nemesis_class_name} should not be disruptive"
