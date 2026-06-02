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
# Copyright (c) 2020 ScyllaDB
import json
import logging
import time
import types
import unittest.mock
from unittest.mock import MagicMock, patch

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.test_config import TestConfig
from sdcm.tester import ClusterTester, silence
from sdcm.utils.common import get_post_behavior_actions
from unit_tests.lib.fake_events import make_fake_events
from unit_tests.lib.fake_tester import ClusterTesterForTests, FakeSCTConfiguration


class SubtestAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
        with self.subTest("SUBTEST1"):
            # This wont create error in events
            raise ValueError("Subtest1 failed")
        with self.subTest("SUBTEST2"):
            raise ValueError("Subtest2 failed")
        raise ValueError("Main test also failed")

    @silence()
    def save_email_data(self):
        raise ValueError()


class CriticalErrorNotCaughtTest(ClusterTesterForTests):
    def test(self):
        try:
            ClusterHealthValidatorEvent.NodeStatus(
                node="node-1",
                message="Failed by some reason",
                error="Reason to fail",
                severity=Severity.CRITICAL,
            ).publish()
            end_time = time.time() + 0.2
            while time.time() < end_time:
                time.sleep(0.1)
        except Exception:  # noqa: BLE001
            pass


class SubtestAssertAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
        with self.subTest("SUBTEST1"):
            # This wont create error in events
            assert False, "Subtest1 failed"
        with self.subTest("SUBTEST2"):
            assert False, "Subtest2 failed"
        assert False, "Main test also failed"

    @silence()
    def save_email_data(self):
        raise ValueError()


class TeardownFailsTest(ClusterTesterForTests):
    def test(self):
        pass

    @silence()
    def save_email_data(self):
        raise ValueError()


class SetupFailsTest(ClusterTesterForTests):
    def prepare_kms_host(self):
        raise RuntimeError("prepare_kms_host failed")

    def test(self):
        pass


class TestErrorTest(ClusterTesterForTests):
    def test(self):
        TestFrameworkEvent(
            source=self.__class__.__name__, source_method="test", message="Something went wrong"
        ).publish()


class SuccessTest(ClusterTesterForTests):
    def test(self):
        pass


class SubtestsSuccessTest(ClusterTesterForTests):
    def test(self):
        with self.subTest("SUBTEST1"):
            pass
        with self.subTest("SUBTEST2"):
            pass


@pytest.mark.parametrize(
    "test_class, results, outcomes",
    [
        pytest.param(
            TeardownFailsTest,
            {"passed": 1},
            ["EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}", "TEST_STATUS: FAILED", "ERROR 0: save_email_data (silenced)"],
            id="TeardownFailsTest",
        ),
        pytest.param(
            SubtestsSuccessTest,
            {"passed": 1, "subtests": 2},
            ["EVENT_SUMMARY: {'NORMAL': 2}", "TEST_STATUS: SUCCESS"],
            id="SubtestsSuccessTest",
        ),
        pytest.param(
            SuccessTest, {"passed": 1}, ["EVENT_SUMMARY: {'NORMAL': 2}", "TEST_STATUS: SUCCESS"], id="SuccessTest"
        ),
        pytest.param(
            TestErrorTest,
            {"passed": 1},
            ["EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}", "TEST_STATUS: FAILED", "ERROR 0: Something went wrong"],
            id="TestErrorTest",
        ),
        pytest.param(
            SetupFailsTest,
            {"failed": 1},
            ["EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}", "TEST_STATUS: FAILED", "ERROR 0: prepare_kms_host failed"],
            id="SetupFailsTest",
        ),
        pytest.param(
            CriticalErrorNotCaughtTest,
            {"passed": 1},
            [
                "EVENT_SUMMARY: {'NORMAL': 2, 'CRITICAL': 1}",
                "TEST_STATUS: FAILED",
                "CRITICAL 0: Reason to fail",
            ],
            id="CriticalErrorNotCaughtTest",
        ),
        pytest.param(
            SubtestAndTeardownFailsTest,
            {"failed": 3},
            [
                "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
                "TEST_STATUS: FAILED",
                "ERROR 0: save_email_data (silenced)",
                "E           ValueError: Subtest1 failed",
                "E           ValueError: Subtest2 failed",
                "E       ValueError: Main test also failed",
            ],
            id="SubtestAndTeardownFailsTest",
        ),
        pytest.param(
            SubtestAssertAndTeardownFailsTest,
            {"failed": 3},
            [
                "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
                "TEST_STATUS: FAILED",
                "ERROR 0: save_email_data (silenced)",
                "E           AssertionError: Subtest1 failed",
                "E           AssertionError: Subtest2 failed",
                "E       AssertionError: Main test also failed",
            ],
            id="SubtestAssertAndTeardownFailsTest",
        ),
    ],
)
def test_tester_subclass(pytester, test_class, results, outcomes):
    # pytester sets HOME to its temp path; create a mock SSH key file
    # so the ExistingFile validator for user_credentials_path succeeds.
    ssh_dir = pytester.path / ".ssh"
    ssh_dir.mkdir()
    (ssh_dir / "scylla_test_id_ed25519").write_text("mock-key")

    # Create a pytest file with the test class. We cannot just use the class directly
    # because it would not be collected. If we made it collectable it would be run as part of standard test run as well.
    # Which we do not want, as it is intended only to be run with pytester
    pytester.makepyfile(f"""
        from unit_tests.conftest import *
        from unit_tests.unit.test_tester import {test_class.__name__}
        {test_class.__name__}.__test__ = True
    """)

    # cause of https://github.com/pytest-dev/pytest/issues/13905
    # we need to run with extra flag -q, so subtest summary output is shown
    result = pytester.runpytest_inprocess("-q")
    summary = result.parseoutcomes()
    for status, count in results.items():
        assert status in summary, f"Status '{status}' not found in results"
        assert summary[status] == count, f"Status '{status}' count mismatch: expected {count}, got {summary[status]}"
    output = result.stdout.str().splitlines()
    for outcome in outcomes:
        assert outcome in output


class TestGatherFailureStatistics:
    """Tests for the gather_failure_statistics functionality."""

    def test_gather_failure_statistics_no_db_cluster(self, tmp_path):
        """Test that gather_failure_statistics handles missing db_cluster gracefully."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_no_cluster")
        tester.db_cluster = None
        tester.logdir = str(tmp_path)

        # Should not raise an exception
        tester.gather_failure_statistics()

    def test_gather_failure_statistics_with_nodes(self, tmp_path):
        """Test that gather_failure_statistics collects nodetool outputs."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_with_nodes")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with nodes
        mock_node = MagicMock()
        mock_node.name = "test_node_1"
        mock_node._is_node_ready_run_scylla_commands.return_value = True

        # Mock nodetool commands
        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "Test nodetool output"
        mock_result.stderr = ""
        mock_node.run_nodetool.return_value = mock_result

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]
        tester.params = FakeSCTConfiguration()

        # Run the function
        tester.gather_failure_statistics()

        # Verify nodetool commands were called
        assert mock_node.run_nodetool.called
        nodetool_calls = [call[1]["sub_cmd"] for call in mock_node.run_nodetool.call_args_list]
        assert "status" in nodetool_calls
        assert "gossipinfo" in nodetool_calls
        assert "compactionstats" in nodetool_calls

        # Verify files were created
        assert (tmp_path / "nodetool_status_failure.log").exists()
        assert (tmp_path / "nodetool_gossipinfo_failure_test_node_1.log").exists()
        assert (tmp_path / "nodetool_compactionstats_failure_test_node_1.log").exists()

    def test_gather_failure_statistics_node_not_ready(self, tmp_path):
        """Test that gather_failure_statistics handles nodes that are not ready."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_node_not_ready")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with a node that's not ready
        mock_node = MagicMock()
        mock_node.name = "test_node_not_ready"
        mock_node._is_node_ready_run_scylla_commands.return_value = False

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]
        tester.params = FakeSCTConfiguration()

        # Run the function
        tester.gather_failure_statistics()

        # Verify nodetool was not called on the not-ready node
        assert not mock_node.run_nodetool.called

    def test_gather_failure_statistics_with_multiple_nodes(self, tmp_path):
        """Test that gather_failure_statistics collects from multiple nodes."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_multiple_nodes")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with multiple nodes
        mock_nodes = []
        for i in range(3):
            mock_node = MagicMock()
            mock_node.name = f"test_node_{i}"
            mock_node._is_node_ready_run_scylla_commands.return_value = True

            mock_result = MagicMock()
            mock_result.ok = True
            mock_result.stdout = f"Test output from node {i}"
            mock_result.stderr = ""
            mock_node.run_nodetool.return_value = mock_result

            mock_nodes.append(mock_node)

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = mock_nodes
        tester.params = FakeSCTConfiguration()

        # Run the function
        tester.gather_failure_statistics()

        # Verify all nodes were queried for gossipinfo and compactionstats
        for i, node in enumerate(mock_nodes):
            assert node.run_nodetool.called
            # Each node should have gossipinfo and compactionstats calls
            nodetool_calls = [call[1]["sub_cmd"] for call in node.run_nodetool.call_args_list]
            assert "gossipinfo" in nodetool_calls
            assert "compactionstats" in nodetool_calls

    def test_gather_failure_statistics_nodetool_failure(self, tmp_path):
        """Test that gather_failure_statistics handles nodetool command failures."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_nodetool_failure")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with a node where nodetool fails
        mock_node = MagicMock()
        mock_node.name = "test_node_fail"
        mock_node._is_node_ready_run_scylla_commands.return_value = True

        # Mock failed nodetool command
        mock_result = MagicMock()
        mock_result.ok = False
        mock_result.stdout = ""
        mock_result.stderr = "Command failed"
        mock_node.run_nodetool.return_value = mock_result

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]
        tester.params = FakeSCTConfiguration()

        # Should not raise an exception
        tester.gather_failure_statistics()

        # Verify it still attempted to call nodetool
        assert mock_node.run_nodetool.called

    def test_save_nodetool_output_in_file(self, tmp_path):
        """Test save_nodetool_output_in_file method."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_save_nodetool")
        tester.logdir = str(tmp_path)

        # Mock node
        mock_node = MagicMock()
        mock_node.name = "test_node"

        # Mock successful nodetool command
        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "Test nodetool status output"
        mock_result.stderr = ""
        mock_node.run_nodetool.return_value = mock_result

        # Save output
        tester.save_nodetool_output_in_file(node=mock_node, sub_cmd="status", log_file="test_nodetool_output.log")

        # Verify file was created with correct content
        output_file = tmp_path / "test_nodetool_output.log"
        assert output_file.exists()
        assert output_file.read_text() == "Test nodetool status output"

    def test_teardown_calls_gather_failure_statistics_on_failure(self, tmp_path):
        """Test that tearDown calls gather_failure_statistics when test fails."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_teardown_failure")
        tester.logdir = str(tmp_path)
        tester.monitors = MagicMock()
        tester.kafka_cluster = None  # Required by tearDown

        # Mock db_cluster
        mock_node = MagicMock()
        mock_node.name = "test_node"
        mock_node._is_node_ready_run_scylla_commands.return_value = True

        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "Test output"
        mock_node.run_nodetool.return_value = mock_result

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]
        tester.params = FakeSCTConfiguration()
        tester.start_time = time.time()  # Required by tearDown

        # Setup fake events before calling tearDown
        with make_fake_events():
            tester.events_processes_registry = SctEvent._events_processes_registry

            # Create an error event to make test status FAILED
            TestFrameworkEvent(
                source="test", source_method="test", exception=Exception("Test error"), severity=Severity.ERROR
            ).publish()

            # Mock gather_failure_statistics to verify it's called
            tester.gather_failure_statistics = MagicMock()

            # Run tearDown
            tester.tearDown()

            # Verify gather_failure_statistics was called
            assert tester.gather_failure_statistics.called


class TestSaveSchema:
    """Tests for the save_schema functionality."""

    def test_save_schema_no_db_cluster(self, tmp_path):
        """Test that save_schema handles missing db_cluster gracefully."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_no_cluster")
        tester.db_cluster = None
        tester.logdir = str(tmp_path)

        # Should not raise an exception
        tester.save_schema()

    def test_save_schema_saves_all_tables(self, tmp_path):
        """Test that save_schema saves all expected tables and uploads compaction_history to S3."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_save_schema")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with a node
        mock_node = MagicMock()
        mock_node.name = "test_node_1"
        mock_node._is_node_ready_run_scylla_commands.return_value = True

        # Mock run_cqlsh to return sample data
        mock_result = MagicMock()
        mock_result.stdout = "Sample output"
        mock_node.run_cqlsh.return_value = mock_result

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]

        # Mock the upload_system_table_to_s3 function and argus_collect_logs
        with (
            patch("sdcm.tester.upload_system_table_to_s3") as mock_upload,
            patch.object(tester, "argus_collect_logs") as mock_argus_logs,
        ):
            mock_upload.return_value = (
                "https://s3.amazonaws.com/test-bucket/test.jsonl.tar.gz",
                "system_compaction_history-20260524_092654-node-1.jsonl.tar.gz",
            )

            # Run save_schema
            tester.save_schema()

            # Verify upload_system_table_to_s3 was called for compaction_history
            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            assert call_args[1]["node"] == mock_node
            assert call_args[1]["table_name"] == "system.compaction_history"

            # Verify argus_collect_logs was called with the S3 link
            mock_argus_logs.assert_called_once_with(
                {
                    "system_compaction_history-20260524_092654-node-1.jsonl.tar.gz": "https://s3.amazonaws.com/test-bucket/test.jsonl.tar.gz"
                }
            )

        # Verify run_cqlsh was called with expected commands (but NOT for compaction_history)
        cqlsh_calls = [call[0][0] for call in mock_node.run_cqlsh.call_args_list]
        assert "desc schema" in cqlsh_calls
        assert "select JSON * from system_schema.tables" in cqlsh_calls
        assert "select JSON * from system.truncated" in cqlsh_calls
        assert "select JSON * from system.tablets" in cqlsh_calls
        assert "select JSON * from system.compaction_history" not in cqlsh_calls  # Now uploaded directly to S3
        assert "desc schema with internals" in cqlsh_calls

        # Verify files were created (except compaction_history which goes to S3)
        assert (tmp_path / "schema.log").exists()
        assert (tmp_path / "system_schema_tables.log").exists()
        assert (tmp_path / "system_truncated.log").exists()
        assert (tmp_path / "system_tablets.log").exists()
        assert not (tmp_path / "system_compaction_history.log").exists()  # Not saved locally anymore
        assert (tmp_path / "schema_with_internals.log").exists()

    def test_save_schema_node_not_ready(self, tmp_path):
        """Test that save_schema handles nodes that are not ready."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_node_not_ready")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with a node that's not ready
        mock_node = MagicMock()
        mock_node.name = "test_node_not_ready"
        mock_node._is_node_ready_run_scylla_commands.return_value = False

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = [mock_node]

        # Run save_schema
        tester.save_schema()

        # Verify run_cqlsh was not called on the not-ready node
        assert not mock_node.run_cqlsh.called

    def test_save_schema_only_uses_first_live_node(self, tmp_path):
        """Test that save_schema only queries the first live node."""
        tester = ClusterTesterForTests()
        tester._init_logging(tmp_path / "test_first_node")
        tester.logdir = str(tmp_path)

        # Mock db_cluster with multiple nodes
        mock_nodes = []
        for i in range(3):
            mock_node = MagicMock()
            mock_node.name = f"test_node_{i}"
            mock_node._is_node_ready_run_scylla_commands.return_value = True

            mock_result = MagicMock()
            mock_result.stdout = f"Output from node {i}"
            mock_node.run_cqlsh.return_value = mock_result

            mock_nodes.append(mock_node)

        tester.db_cluster = MagicMock()
        tester.db_cluster.nodes = mock_nodes

        # Run save_schema
        tester.save_schema()

        # Verify only the first node was queried
        assert mock_nodes[0].run_cqlsh.called
        assert not mock_nodes[1].run_cqlsh.called
        assert not mock_nodes[2].run_cqlsh.called


class TestEmrCleanResources:
    """Tests for EMR cluster conditional cleanup in clean_resources."""

    @pytest.fixture(autouse=True)
    def _reset_test_config(self):
        """Reset EMR keep-alive flag between tests."""
        original = TestConfig.KEEP_ALIVE_EMR_CLUSTER
        yield
        TestConfig.KEEP_ALIVE_EMR_CLUSTER = original

    @staticmethod
    def _make_params(emr_action):
        mock_params = MagicMock()
        mock_params.get.side_effect = lambda key, default=None: {
            "execute_post_behavior": True,
            "post_behavior_db_nodes": "keep",
            "post_behavior_loader_nodes": "keep",
            "post_behavior_monitor_nodes": "keep",
            "post_behavior_vector_store_nodes": "keep",
            "post_behavior_k8s_cluster": "keep",
            "post_behavior_emr_cluster": emr_action,
            "db_type": "scylla",
        }.get(key, default)
        return mock_params

    @staticmethod
    def _make_tester(mock_params, emr_cluster=None):
        tester = MagicMock(spec=ClusterTester)
        tester.params = mock_params
        tester.test_config = TestConfig
        tester.log = logging.getLogger("test_emr")
        tester.logdir = "/tmp/test"
        tester.db_cluster = tester.loaders = tester.monitors = None
        tester.emr_cluster = emr_cluster
        return tester

    @staticmethod
    def _run_clean_resources(tester, critical_events):
        actions = get_post_behavior_actions(tester.params)
        with (
            unittest.mock.patch("sdcm.tester.get_testrun_status", return_value=critical_events),
            unittest.mock.patch("sdcm.tester.get_post_behavior_actions", return_value=actions),
        ):
            ClusterTester.clean_resources(tester)

    @pytest.mark.parametrize(
        "action, critical_events, expect_terminated",
        [
            pytest.param("destroy", False, True, id="destroy-no-critical"),
            pytest.param("destroy", True, True, id="destroy-with-critical"),
            pytest.param("keep-on-failure", False, True, id="keep-on-failure-no-critical"),
            pytest.param("keep-on-failure", True, False, id="keep-on-failure-with-critical"),
            pytest.param("keep", False, False, id="keep-no-critical"),
            pytest.param("keep", True, False, id="keep-with-critical"),
        ],
    )
    def test_emr_clean_resources_conditional_termination(self, action, critical_events, expect_terminated):
        """Test that clean_resources terminates EMR cluster based on post-behavior action and critical events."""
        mock_emr = MagicMock()
        mock_emr.cluster_id = "j-TEST123"
        tester = self._make_tester(self._make_params(action), emr_cluster=mock_emr)

        self._run_clean_resources(tester, critical_events)

        if expect_terminated:
            mock_emr.terminate_emr_cluster.assert_called_once()
        else:
            mock_emr.terminate_emr_cluster.assert_not_called()

    def test_emr_clean_resources_keeps_on_failure_sets_keep_alive(self):
        """Test that keep-on-failure with critical events sets KEEP_ALIVE_EMR_CLUSTER flag."""
        mock_emr = MagicMock()
        mock_emr.cluster_id = "j-KEEPME"
        tester = self._make_tester(self._make_params("keep-on-failure"), emr_cluster=mock_emr)

        self._run_clean_resources(tester, critical_events=True)

        mock_emr.terminate_emr_cluster.assert_not_called()
        assert TestConfig.KEEP_ALIVE_EMR_CLUSTER is True

    def test_emr_clean_resources_no_emr_cluster(self):
        """Test that clean_resources handles None emr_cluster gracefully."""
        tester = self._make_tester(self._make_params("destroy"))
        self._run_clean_resources(tester, critical_events=False)


# --- Tests for ClusterTester.init_argus_run() Argus config submission ---


@pytest.fixture()
def tester_with_argus(monkeypatch):
    """Create a minimal ClusterTester-like object with mocked Argus client and real SCTConfiguration.

    Uses MagicMock as `self` because ClusterTester.__init__ has heavy side effects
    (events system, monitoring, cloud provisioning) that cannot be trivially mocked.
    We bind only the method under test to preserve real logic while isolating I/O.

    The bound init_argus_run already wraps all external I/O functions (git, network)
    so tests can simply call ``tester.init_argus_run()`` without extra patching.
    """
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", "{'decommission': 5, 'new_node': 5}")

    params = SCTConfiguration()

    mock_argus_client = MagicMock()
    mock_argus_client.run_id = "test-run-id"

    mock_test_config = MagicMock()
    mock_test_config.argus_client.return_value = mock_argus_client

    # MagicMock as self: ClusterTester.__init__ has heavy side effects;
    # we bind only the method under test.
    tester = MagicMock()
    tester.params = params
    tester.test_config = mock_test_config
    tester.log = logging.getLogger("test_init_argus_run")

    monkeypatch.setattr(
        "sdcm.tester.get_git_status_info",
        lambda: {
            "branch.oid": "abc123",
            "upstream.url": "git@github.com:test",
            "branch.upstream": "origin/main",
        },
    )
    monkeypatch.setattr("sdcm.tester.get_git_commit_id", lambda: "abc123")
    monkeypatch.setattr("sdcm.tester.get_job_name", lambda: "test-job")
    monkeypatch.setattr("sdcm.tester.get_job_url", lambda: "http://jenkins/job/1")
    monkeypatch.setattr("sdcm.tester.get_username", lambda: "test-user")
    monkeypatch.setattr("sdcm.tester.get_sct_runner_ip", lambda: "1.2.3.4")
    monkeypatch.setattr("sdcm.tester.get_my_ip", lambda: "10.0.0.1")

    tester.init_argus_run = types.MethodType(ClusterTester.init_argus_run, tester)

    return tester, mock_argus_client


def test_init_argus_run_pydantic_root_model_serializes_valid_json(tester_with_argus):
    """Regression: json.dumps previously raised ValueError (circular reference) with AdaptiveTimeoutMultipliers."""
    tester, mock_argus_client = tester_with_argus

    tester.init_argus_run()

    content = mock_argus_client.sct_submit_config.call_args.kwargs["content"]
    parsed = json.loads(content)
    assert isinstance(parsed, dict), f"Expected dict, got {type(parsed).__name__}"
    assert parsed["adaptive_timeout_multipliers"] == {"decommission": 5.0, "new_node": 5.0}, (
        f"adaptive_timeout_multipliers not serialized correctly: {parsed.get('adaptive_timeout_multipliers')}"
    )


def test_init_argus_run_config_contains_essential_fields(tester_with_argus):
    """Verify essential SCT config fields are present in the submitted JSON."""
    tester, mock_argus_client = tester_with_argus

    tester.init_argus_run()

    content = mock_argus_client.sct_submit_config.call_args.kwargs["content"]
    parsed = json.loads(content)
    assert parsed["cluster_backend"] == "aws"
    assert "instance_type_db" in parsed


def test_init_argus_run_config_excludes_internal_fields(tester_with_argus):
    """Fields marked with exclude=True must not appear in the submitted config."""
    tester, mock_argus_client = tester_with_argus

    tester.init_argus_run()

    content = mock_argus_client.sct_submit_config.call_args.kwargs["content"]
    parsed = json.loads(content)
    assert "multi_region_params" not in parsed
    assert "regions_data" not in parsed
    assert "target_db_image_ids" not in parsed
