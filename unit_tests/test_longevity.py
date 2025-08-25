import threading
import unittest
from unittest.mock import MagicMock

import pytest

from longevity_test import LongevityTest
from sdcm.cluster import NoMonitorSet
from sdcm.sct_events import events_processes
from unit_tests.test_utils_common import DummyDbCluster, DummyNode
from unit_tests.test_cluster import DummyDbCluster


LongevityTest.__test__ = False


@pytest.fixture(scope="function", autouse=True)
def fixture_mock_calls():
    with unittest.mock.patch("sdcm.tester.validate_raft_on_nodes"):
        yield

    # clear the events processes registry after each test, so next test would be ableto start it fresh
    events_processes._EVENTS_PROCESSES = None


@pytest.mark.sct_config(files='test-cases/features/elasticity/longevity-elasticity-many-small-tables.yaml')
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
        node = DummyNode(name="test_node", parent_cluster=None, ssh_login_info=dict(key_file="~/.ssh/scylla-test"))
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
        for _ in range(len(params['stress_cmd'])):
            m = MagicMock()
            m.parse_results.return_value = ([], {})
            stress_queue.append(m)
