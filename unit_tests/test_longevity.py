
def test_test_user_batch_custom_time(params, pytester):
    """
    Test to verify that the longevity test for user batch with custom time runs successfully with 5000 tables case

    test is using pytester plugin, so all the pytest fixtures logic would be operational
    """

    # remove HOME and USERPROFILE environment variables, so we can get actual user configuration
    # the test depends on some part of the user configuration, for running the test locally
    pytester._monkeypatch.delenv('HOME')
    pytester._monkeypatch.delenv('USERPROFILE')

    pytester.makeconftest(
        """
        import os
        import unittest.mock

        import pytest

        from sdcm import sct_config
        from sdcm.sct_events import events_processes

        @pytest.fixture(scope="function", autouse=True)
        def fixture_mock_calls():
            with unittest.mock.patch("sdcm.tester.validate_raft_on_nodes"):
                yield

            # clear the events processes registry after each test, so next test would be ableto start it fresh
            events_processes._EVENTS_PROCESSES = None

        # TODO: find a way we can pick those fixture from something external to this generated file
        @pytest.fixture(scope="function", name="params")
        def fixture_params(request: pytest.FixtureRequest):
            if sct_config_marker := request.node.get_closest_marker("sct_config"):
                config_files = sct_config_marker.kwargs.get('files')
                os.environ['SCT_CONFIG_FILES'] = config_files

            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            params = sct_config.SCTConfiguration()

            yield params

            for k in os.environ:
                if k.startswith('SCT_'):
                    del os.environ[k]
        """
    )

    # create a temporary pytest test file, mocking enough of LongevityTest methods,
    # so we can try the logic of stress commands execution
    pytester.makepyfile(
        """
        import threading
        from unittest.mock import MagicMock

        import pytest

        from longevity_test import LongevityTest
        from sdcm.cluster import NoMonitorSet
        from unit_tests.test_utils_common import DummyDbCluster, DummyNode
        from unit_tests.test_cluster import DummyDbCluster

        LongevityTest.__test__ = False

        @pytest.mark.sct_config(files='test-cases/scale/longevity-5000-tables.yaml')
        class DummyLongevityTest(LongevityTest):
            __test__ = True

            @pytest.fixture(autouse=True)
            def fixture_params(self, params):
                self.params = params
                self.params["cluster_health_check"] = False
                self.params["n_monitor_nodes"] = 0
                self.timeout_thread = None
                self.k8s_clusters = []

            def _init_params(self):
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
    """
    )

    # run the test with pytest
    result = pytester.runpytest(
        "test_test_user_batch_custom_time.py::DummyLongevityTest::test_user_batch_custom_time")

    result.assert_outcomes(passed=1)
