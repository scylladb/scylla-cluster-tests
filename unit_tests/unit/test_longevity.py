import threading
import unittest
from unittest.mock import MagicMock, patch

import pytest

from longevity_test import LongevityTest
from sdcm.cluster import NoMonitorSet
from sdcm.sct_events.base import SctEvent
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode
from unit_tests.lib.fake_events import make_fake_events


LongevityTest.__test__ = False


@pytest.fixture(scope="function", autouse=True)
def fixture_mock_calls():
    with unittest.mock.patch("sdcm.tester.validate_raft_on_nodes"), unittest.mock.patch("sdcm.tester.time.sleep"):
        yield


@pytest.mark.sct_config(files="unit_tests/test_configs/longevity-elasticity-unit-test.yaml")
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
        self.kafka_cluster = None

    @pytest.fixture(autouse=True, name="event_system")
    def fixture_event_system(self, setup_logging):
        with make_fake_events() as device:
            self._fake_device = device
            self.events_processes_registry = SctEvent._events_processes_registry
            yield

    def _init_params(self):
        pass

    def init_argus_run(self):
        pass

    def collect_relocatable_package(self):
        pass

    def collect_grafana_screenshots(self):
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

    def create_templated_user_stress_params(self, idx, cs_profile):
        fake_profile_path = f"/tmp/mock_profile_table{idx}.yaml"
        return [
            {
                "stress_cmd": f"cassandra-stress user profile={fake_profile_path} 'ops(insert=1)' cl=QUORUM n=1495501 -rate threads=1",
                "profile": fake_profile_path,
            },
            {
                "stress_cmd": f"cassandra-stress user profile={fake_profile_path} 'ops(read1=1)' cl=QUORUM n=747748 -rate threads=1",
                "profile": fake_profile_path,
            },
        ]

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


def make_longevity_test_with_df_output(
    monkeypatch,
    df_available_per_node,
    effective_compression_ratio=1.0,
    dc_indices=None,
    configured_n_db_nodes=None,
):
    """Return a LongevityTest instance whose nodes return controlled df output."""

    class FakeLongevityTest(LongevityTest):
        def __init__(self):
            pass  # skip full ClusterTester.__init__

    instance = FakeLongevityTest()
    instance.params = {"effective_compression_ratio": effective_compression_ratio}
    if configured_n_db_nodes is not None:
        instance.params["n_db_nodes"] = configured_n_db_nodes

    if dc_indices is None:
        dc_indices = [0] * len(df_available_per_node)

    nodes = []
    for available_bytes, dc_idx in zip(df_available_per_node, dc_indices):
        node = MagicMock()
        node.dc_idx = dc_idx
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
        ([111], 1.0, 100),
        ([56], 0.5, 101),
        ([748_001], 0.68, 1_000_001),
        ([67, 111, 89], 1.0, 80),  # average of three nodes
        ([660_001, 1_100_001, 880_001], 0.5, 1_600_001),
    ],
)
def test_effective_disk_size_bytes(monkeypatch, df_available_per_node, compression_ratio, expected):
    instance = make_longevity_test_with_df_output(monkeypatch, df_available_per_node, compression_ratio)
    with patch("longevity_test.ParallelObject") as mock_parallel:
        mock_parallel.return_value.run.return_value = [MagicMock(result=b) for b in df_available_per_node]
        assert instance.effective_disk_size_bytes == expected


def test_longevity_whitelist_allows_effective_disk_size_bytes(monkeypatch):
    instance = make_longevity_test_with_df_output(monkeypatch, [111])
    with patch("longevity_test.ParallelObject") as mock_parallel:
        mock_parallel.return_value.run.return_value = [MagicMock(result=111)]
        rendered = instance.render_stress_cmd("cassandra-stress write n={{ effective_disk_size_bytes }}")
    assert rendered == "cassandra-stress write n=100"


def test_longevity_whitelist_allows_db_node_count_per_dc(monkeypatch):
    instance = make_longevity_test_with_df_output(
        monkeypatch,
        [600_000_000, 1_000_000_000, 800_000_000],
        dc_indices=[0, 0, 2],
        configured_n_db_nodes=[2, 0, 1],
    )
    rendered = instance.render_stress_cmd("cassandra-stress write n={{ db_node_count_per_dc }}")
    assert instance.db_node_count_per_dc == 2
    assert rendered == "cassandra-stress write n=2"


def test_db_node_count_per_dc_falls_back_to_runtime_nodes_when_config_missing(monkeypatch):
    instance = make_longevity_test_with_df_output(
        monkeypatch,
        [600_000_000, 1_000_000_000, 800_000_000],
        dc_indices=[0, 0, 2],
    )
    assert instance.db_node_count_per_dc == 2


def test_longevity_stress_template_context_can_use_whitelisted_builtins(monkeypatch):
    instance = make_longevity_test_with_df_output(
        monkeypatch,
        [660_001, 1_100_001],
        dc_indices=[0, 0],
        configured_n_db_nodes=[2],
    )
    instance.params["stress_template_context"] = {
        "rows_total": "{{ ((effective_disk_size_bytes * db_node_count_per_dc) // 1000) | int }}",
        "rows_per_cmd": "{{ rows_total // 4 }}",
    }
    with patch("longevity_test.ParallelObject") as mock_parallel:
        mock_parallel.return_value.run.return_value = [MagicMock(result=660_001), MagicMock(result=1_100_001)]
        rendered = instance.render_stress_cmd("cassandra-stress write n={{ rows_per_cmd }}")
    assert rendered == "cassandra-stress write n=400"


def test_longevity_rejects_context_key_conflicting_with_builtin(monkeypatch):
    instance = make_longevity_test_with_df_output(monkeypatch, [1_000_000_000], configured_n_db_nodes=[1])
    instance.params["stress_template_context"] = {"db_node_count_per_dc": 99}
    with pytest.raises(RuntimeError, match="conflicts with a built-in stress template variable"):
        instance.render_stress_cmd("cassandra-stress write n={{ db_node_count_per_dc }}")


def test_longevity_whitelist_blocks_other_owner_attributes(monkeypatch):
    instance = make_longevity_test_with_df_output(monkeypatch, [1_000_000_000])
    instance.some_other_template_value = 17
    with pytest.raises(RuntimeError, match="unknown Jinja variable"):
        instance.render_stress_cmd("cassandra-stress write n={{ some_other_template_value }}")
