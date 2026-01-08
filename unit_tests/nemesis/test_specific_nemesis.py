"""This module tests specific nemesis and is heavily dependent on the implementation"""

import logging
from unittest.mock import MagicMock, Mock

import pytest

from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from sdcm.nemesis import CategoricalMonkey, MemoryStressMonkey
from unit_tests.nemesis.fake_cluster import FakeTester
from unit_tests.nemesis.test_sisyphus import TestNemesisClass

LOGGER = logging.getLogger(__name__)


class FakeCategoricalMonkey(CategoricalMonkey):
    runs = []

    def __new__(cls, *_, **__):
        return object.__new__(cls)

    def __init__(self, tester_obj, termination_event, dist: dict, default_weight: float = 1):
        setattr(CategoricalMonkey, "disrupt_m1", FakeCategoricalMonkey.disrupt_m1)
        setattr(CategoricalMonkey, "disrupt_m2", FakeCategoricalMonkey.disrupt_m2)
        super().__init__(tester_obj, termination_event, dist, default_weight=default_weight)

    def disrupt_m1(self):
        self.runs.append(1)

    def disrupt_m2(self):
        self.runs.append(2)

    def get_runs(self):
        return self.runs


@pytest.mark.parametrize(
    "parent, result",
    [
        (LocalMinimalScyllaPodCluster, True),
        (GkeScyllaPodCluster, True),
        (EksScyllaPodCluster, True),
        (ScyllaGCECluster, False),
        (ScyllaAWSCluster, False),
        (ScyllaDockerCluster, False),
    ],
)
def test_is_it_on_kubernetes(parent, result):
    """Tests is_it_on_kubernetes on different Cluster types"""

    class FakeClass(parent):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    params = {"nemesis_interval": 10, "nemesis_filter_seeds": 1}
    nemesis = TestNemesisClass(FakeTester(db_cluster=FakeClass(), params=params), None)
    assert nemesis._is_it_on_kubernetes() == result


def test_categorical_monkey():
    tester = FakeTester()

    nemesis = FakeCategoricalMonkey(tester, None, {"m1": 1}, default_weight=0)
    nemesis._random_disrupt()

    nemesis = FakeCategoricalMonkey(tester, None, {"m2": 1}, default_weight=0)
    nemesis._random_disrupt()

    assert nemesis.runs == [1, 2]

    nemesis = FakeCategoricalMonkey(tester, None, {"m1": 1, "m2": 1}, default_weight=0)
    nemesis._random_disrupt()

    assert nemesis.runs in ([1, 2, 1], [1, 2, 2])


def test_memory_stress_cgroup_pinning():
    """Test that memory stress nemesis pins stress-ng to Scylla's cgroup"""
    
    # Create mock tester and cluster
    tester = FakeTester()
    
    # Create mock target node with mocked remoter
    mock_node = Mock()
    mock_node.distro.is_rhel_like = False
    mock_node.distro.is_ubuntu = True
    mock_node.name = "test-node"
    
    # Mock remoter responses
    mock_remoter = MagicMock()
    mock_node.remoter = mock_remoter
    
    # Setup mock responses for commands
    # Response for finding Scylla PID
    pid_result = Mock()
    pid_result.ok = True
    pid_result.stdout = "12345"
    
    # Response for getting Scylla memory usage (100GB = 104857600 KB)
    memory_result = Mock()
    memory_result.ok = True
    memory_result.stdout = "104857600"  # 100GB in KB
    
    # Response for getting Scylla slice
    slice_result = Mock()
    slice_result.ok = True
    slice_result.stdout = "scylla.slice"
    
    # Configure mock to return different results based on command
    def mock_run(cmd, ignore_status=False):
        if "ps -C scylla" in cmd:
            return pid_result
        elif "awk '/VmRSS/" in cmd:
            return memory_result
        elif "systemctl show scylla-server" in cmd:
            return slice_result
        else:
            # This is the actual stress-ng command
            result = Mock()
            result.ok = True
            return result
    
    mock_remoter.run.side_effect = mock_run
    mock_remoter.sudo.return_value = Mock(ok=True)
    
    # Create mock cluster with nodes
    mock_cluster = Mock()
    mock_cluster.nodes = [mock_node]
    tester.db_cluster = mock_cluster
    
    # Create MemoryStressMonkey instance
    nemesis = MemoryStressMonkey(tester, None)
    nemesis.target_node = mock_node
    nemesis.cluster = mock_cluster
    
    # Execute the nemesis
    nemesis.disrupt()
    
    # Verify the commands were called correctly
    # Should call ps to get PID
    assert any("ps -C scylla" in str(call) for call in mock_remoter.run.call_args_list)
    
    # Should call awk to get memory usage
    assert any("VmRSS" in str(call) for call in mock_remoter.run.call_args_list)
    
    # Should call systemctl to get slice
    assert any("systemctl show scylla-server" in str(call) for call in mock_remoter.run.call_args_list)
    
    # Verify stress-ng was run with systemd-run and correct slice
    stress_calls = [call for call in mock_remoter.run.call_args_list 
                   if "stress-ng" in str(call) and "systemd-run" in str(call)]
    assert len(stress_calls) > 0, "stress-ng should be called with systemd-run"
    
    # Get the stress-ng command
    stress_cmd = str(stress_calls[0])
    
    # Verify it uses systemd-run with --slice
    assert "systemd-run" in stress_cmd
    assert "--slice=scylla.slice" in stress_cmd or "--slice" in stress_cmd
    
    # Verify memory amount is calculated correctly (90% of 104857600 KB)
    expected_memory_kb = int(104857600 * 0.9)
    assert f"{expected_memory_kb}k" in stress_cmd or str(expected_memory_kb) in stress_cmd


def test_memory_stress_memory_calculation():
    """Test that memory stress calculates correct amount based on Scylla usage"""
    
    test_cases = [
        # (scylla_memory_kb, expected_stress_kb)
        (100000, 90000),      # 90% of 100MB
        (1048576, 943718),    # 90% of 1GB
        (10485760, 9437184),  # 90% of 10GB
    ]
    
    for scylla_memory_kb, expected_stress_kb in test_cases:
        # Create mock tester
        tester = FakeTester()
        
        # Create mock target node
        mock_node = Mock()
        mock_node.distro.is_ubuntu = True
        mock_node.name = "test-node"
        
        mock_remoter = MagicMock()
        mock_node.remoter = mock_remoter
        
        # Setup mock responses
        pid_result = Mock(ok=True, stdout="12345")
        memory_result = Mock(ok=True, stdout=str(scylla_memory_kb))
        slice_result = Mock(ok=True, stdout="system.slice")
        
        def mock_run(cmd, ignore_status=False):
            if "ps -C scylla" in cmd:
                return pid_result
            elif "VmRSS" in cmd:
                return memory_result
            elif "systemctl show" in cmd:
                return slice_result
            return Mock(ok=True)
        
        mock_remoter.run.side_effect = mock_run
        mock_remoter.sudo.return_value = Mock(ok=True)
        
        mock_cluster = Mock()
        mock_cluster.nodes = [mock_node]
        tester.db_cluster = mock_cluster
        
        # Execute nemesis
        nemesis = MemoryStressMonkey(tester, None)
        nemesis.target_node = mock_node
        nemesis.cluster = mock_cluster
        nemesis.disrupt()
        
        # Verify correct memory amount in stress-ng command
        stress_calls = [call for call in mock_remoter.run.call_args_list 
                       if "stress-ng" in str(call)]
        assert len(stress_calls) > 0
        
        stress_cmd = str(stress_calls[0])
        assert f"{expected_stress_kb}k" in stress_cmd or str(expected_stress_kb) in stress_cmd

