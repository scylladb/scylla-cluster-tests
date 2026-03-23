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

"""ClusterTesterForTests: shared ClusterTester stub for unit tests.

Extracted from test_tester.py to break cross-test imports.
Provides a fully wired-up ClusterTester subclass that uses in-memory fake
events (via make_fake_events) and stubs out all infrastructure calls so tests
can run without real clusters, cloud credentials, or Argus.
"""

import logging
import re
import threading
import unittest
from unittest.mock import MagicMock

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.base import SctEvent
from sdcm.sct_events.file_logger import get_events_grouped_by_category
from sdcm.tester import ClusterTester, silence, TestResultEvent
from sdcm.utils.action_logger import get_action_logger

from unit_tests.lib.fake_events import make_fake_events


class FakeSCTConfiguration(SCTConfiguration):
    def _load_environment_variables(self):
        return {
            "config_files": ["test-cases/PR-provision-test-docker.yaml"],
            "cluster_backend": "docker",
            "run_commit_log_check_thread": False,
        }


ClusterTester.__test__ = False


class ClusterTesterForTests(ClusterTester):
    k8s_clusters = None

    def init_argus_run(self):
        self.argus_heartbeat_stop_signal = threading.Event()

    def _init_params(self):
        self.params = FakeSCTConfiguration()

    @pytest.fixture(autouse=True, name="setup_logging")
    def fixture_setup_logging(self, tmp_path):
        self._init_logging(tmp_path / self.__class__.__name__)
        self.kafka_cluster = None

    @pytest.fixture(scope="function", autouse=True)
    def fixture_mock_issues(self):
        """
        In Pytester the boto credentials are not set, so we mock the issue details, so we dont need them.
        It is not important for the test anyway
        """
        with unittest.mock.patch("sdcm.utils.issues.SkipPerIssues.get_issue_details") as mock_issue_details:
            mock_issue_details.return_value = None
            yield

    def _init_logging(self, logdir):
        self.log = logging.getLogger(self.__class__.__name__)
        self.actions_log = get_action_logger("tester")
        self.logdir = logdir

    def init_resources(self, loader_info=None, db_info=None, monitor_info=None):
        pass

    def _init_localhost(self):
        return None

    def argus_finalize_test_run(self):
        pass

    @property
    def elasticsearch(self):
        return None

    @silence()
    def save_email_data(self):
        pass

    def argus_collect_manager_version(self):
        pass

    def _db_post_validation(self):
        pass

    def argus_get_scylla_version(self):
        pass

    def start_argus_heartbeat_thread(self):
        # prevent from heartbeat thread to start
        # because it can be left running after the test
        # and break other tests
        return threading.Event()

    def tearDown(self):
        self.monitors = MagicMock()
        super().tearDown()

    @property
    def final_event(self) -> TestResultEvent:
        return self._get_test_result_event()

    @property
    def event_summary(self) -> dict:
        return self.get_event_summary()

    @property
    def events(self) -> dict:
        return get_events_grouped_by_category(_registry=self.events_processes_registry)

    @pytest.fixture(autouse=True, name="event_system")
    def fixture_event_system(self, setup_logging):
        with make_fake_events() as device:
            self._fake_device = device
            self.events_processes_registry = SctEvent._events_processes_registry
            yield

    @pytest.fixture(autouse=True, name="print_output")
    def fixture_print_output(self, event_system, capsys):
        # Parse error report to output only useful message
        filter_message = re.compile(r"((?<=message=)|(?<=exception=)|(?<=error=)).*(?=( failed|$))")
        yield

        with capsys.disabled():
            print(f"\nEVENT_SUMMARY: {self.event_summary}")
            print(f"TEST_STATUS: {self.final_event.test_status}")
            for i, error in enumerate(self.events["ERROR"]):
                print(f"ERROR {i}: {filter_message.search(error).group(0)}")
            for i, error in enumerate(self.events["CRITICAL"]):
                print(f"CRITICAL {i}: {filter_message.search(error).group(0)}")

    def finalize_teardown(self):
        pass
