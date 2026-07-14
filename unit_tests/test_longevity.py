import math
import threading
import unittest
from unittest.mock import MagicMock, patch

import pytest

from longevity_test import LongevityTest
from sdcm.cluster import NoMonitorSet
from sdcm.sct_events import events_processes
from unit_tests.test_utils_common import DummyDbCluster, DummyNode
from unit_tests.test_cluster import DummyDbCluster


LongevityTest.__test__ = False


@pytest.fixture(scope="function", autouse=True)
def fixture_mock_calls():
    with unittest.mock.patch("sdcm.tester.validate_raft_on_nodes"), unittest.mock.patch("sdcm.tester.time.sleep"):
        yield

    # clear the events processes registry after each test, so next test would be ableto start it fresh
    events_processes._EVENTS_PROCESSES = None


@pytest.mark.sct_config(files="test-cases/features/elasticity/longevity-elasticity-many-small-tables.yaml")
class DummyLongevityTest(LongevityTest):
    __test__ = True
    test_custom_time = None
    test_batch_custom_time = None

    @pytest.fixture(autouse=True)
    def fixture_params(self, params):
        self.params = params
        self.params["cluster_health_check"] = False
        self.params["n_monitor_nodes"] = 0
        self.params["nemesis_interval"] = 1
        self.timeout_thread = None
        self.k8s_clusters = []

    def _init_params(self):
        pass

    def save_email_data(self):
        pass

    def argus_finalize_test_run(self):
        pass

    def start_argus_heartbeat_thread(self):
        # prevent from heartbeat thread to start
        # because it can be left running after the test
        # and break other tests
        return threading.Event()

    def _pre_create_templated_user_schema(self, *args, **kwargs):
        pass

    def init_resources(self):
        node = DummyNode(
            name="test_node", parent_cluster=None, ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519")
        )
        node.parent_cluster = DummyDbCluster([node], params=self.params)
        node.parent_cluster.nemesis_termination_event = threading.Event()
        node.parent_cluster.nemesis = []
        node.parent_cluster.nemesis_threads = []
        self.db_cluster = node.parent_cluster
        self.monitors = NoMonitorSet()
        self.timeout_thread = self._init_test_timeout_thread()

    def init_nodes(self, db_cluster):
        pass

    def argus_collect_manager_version(self):
        pass

    def argus_get_scylla_version(self):
        pass

    def argus_collect_packages(self):
        pass

    def _run_all_stress_cmds(self, stress_queue, params):
        for _ in range(len(params["stress_cmd"])):
            m = MagicMock()
            m.parse_results.return_value = ([], {})
            stress_queue.append(m)


# ---------------------------------------------------------------------------
# effective_disk_size_bytes tests
# ---------------------------------------------------------------------------


def make_longevity_test_with_df_output(monkeypatch, df_available_per_node, effective_compression_ratio=1.0):
    """Return a LongevityTest instance whose nodes return controlled df output."""

    class FakeLongevityTest(LongevityTest):
        def __init__(self):
            pass  # skip full ClusterTester.__init__

    instance = FakeLongevityTest()
    instance.params = {"effective_compression_ratio": effective_compression_ratio}

    nodes = []
    for available_bytes in df_available_per_node:
        node = MagicMock()
        node.remoter.run.return_value.stdout = (
            f"Filesystem        1B-blocks       Used  Available Use% Mounted on\n"
            f"/dev/nvme0n1p1  1000000000  100000000  {available_bytes}  10% /var/lib/scylla\n"
        )
        nodes.append(node)

    instance.db_cluster = MagicMock()
    instance.db_cluster.nodes = nodes
    return instance


@pytest.mark.parametrize(
    "df_available_per_node, compression_ratio, expected",
    [
        ([1_000_000_000], 1.0, 1_000_000_000),
        ([1_000_000_000], 0.5, 2_000_000_000),
        ([1_000_000_000], 0.68, math.floor(1_000_000_000 / 0.68)),
        ([600_000_000, 1_000_000_000, 800_000_000], 1.0, 800_000_000),  # average of three nodes
        ([600_000_000, 1_000_000_000, 800_000_000], 0.5, 1_600_000_000),
    ],
)
def test_effective_disk_size_bytes(monkeypatch, df_available_per_node, compression_ratio, expected):
    instance = make_longevity_test_with_df_output(monkeypatch, df_available_per_node, compression_ratio)
    with patch("longevity_test.ParallelObject") as mock_parallel:
        mock_parallel.return_value.run.return_value = [MagicMock(result=b) for b in df_available_per_node]
        assert instance.effective_disk_size_bytes == expected
