from longevity_test import LongevityTest
from unit_tests.test_utils_common import DummyDbCluster, DummyNode
from unit_tests.test_cluster import DummyDbCluster

import pytest
import threading
from unittest.mock import MagicMock

LongevityTest.__test__ = False
LongevityTest = pytest.mark.skip(reason="we don't need to run those tests")(LongevityTest)


@pytest.mark.sct_config(files='test-cases/scale/longevity-5000-tables.yaml')
def test_test_user_batch_custom_time(params):

    class DummyLongevityTest(LongevityTest):
        def _init_params(self):
            self.params = params
            # NOTE: running this test we get a nemesis trigerred,
            # and if health checks are enabled then the nemesis lock gets held
            # while it's checks are running. It may run for more than 10 minutes.
            # Additional problem is that it is not controlled by this test,
            # thread just runs as a side-car even after finish of this test.
            # In this case any further unit test which runs a nemesis
            # will stumble upon a held lock.
            # One of such tests is following:
            # - test_nemesis.py::test_list_nemesis_of_added_disrupt_methods
            # So, disable health checks here.
            self.params["cluster_health_check"] = False

        def start_argus_heartbeat_thread(self):
            # prevent from heartbeat thread to start
            # because it can be left running after the test
            # and break other tests
            return threading.Event()

        def _pre_create_templated_user_schema(self, *args, **kwargs):
            pass

        def _run_all_stress_cmds(self, stress_queue, params):
            for _ in range(len(params['stress_cmd'])):
                m = MagicMock()
                m.verify_results.return_value = ('', [])
                stress_queue.append(m)

    test = DummyLongevityTest()
    node = DummyNode(name='test_node',
                     parent_cluster=None,
                     ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
    node.parent_cluster = DummyDbCluster([node], params=params)
    node.parent_cluster.nemesis_termination_event = threading.Event()
    node.parent_cluster.nemesis = []
    node.parent_cluster.nemesis_threads = []
    test.db_cluster = node.parent_cluster
    test.monitors = MagicMock()
    test.test_user_batch_custom_time()
