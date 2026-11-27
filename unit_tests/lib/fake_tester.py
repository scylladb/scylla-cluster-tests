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
from sdcm.tester import ClusterTester, TestResultEvent, silence
from sdcm.utils.action_logger import get_action_logger
from unit_tests.lib.fake_events import make_fake_events


class FakeSCTConfiguration(SCTConfiguration):
    """SCTConfiguration loading the docker provision test-case in place of the SCT_* environment."""

    def _load_environment_variables(self):
        return {
            "config_files": ["test-cases/PR-provision-test-docker.yaml"],
            "cluster_backend": "docker",
            "run_commit_log_check_thread": False,
        }


ClusterTester.__test__ = False


class ClusterTesterForTests(ClusterTester):
    """ClusterTester with fake events and stubbed Argus, provisioning and collection steps."""

    k8s_clusters = None

    def init_argus_run(self):
        """Create only the Argus heartbeat stop signal, leaving Argus disconnected."""
        self.argus_heartbeat_stop_signal = threading.Event()

    def _init_params(self):
        self.params = FakeSCTConfiguration()

    @pytest.fixture(autouse=True, name="setup_logging")
    def fixture_setup_logging(self, tmp_path):
        """Point the tester logs at a class-named dir under the test's tmp_path and clear the Kafka cluster."""
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
        """No-op: tests run without provisioned resources."""

    def _init_localhost(self):
        return None

    def argus_finalize_test_run(self):
        """No-op: the Argus run is left as is."""

    @property
    def elasticsearch(self):
        """Return None in place of an Elasticsearch client."""
        return None

    @silence()
    def collect_relocatable_package(self):
        """No-op: there is no relocatable package to collect."""

    @silence()
    def collect_grafana_screenshots(self):
        """No-op: there is no Grafana to screenshot."""

    def argus_collect_manager_version(self):
        """No-op: the manager version is left unreported."""

    def _db_post_validation(self):
        pass

    def argus_get_scylla_version(self):
        """No-op: the Scylla version is left unreported."""

    def start_argus_heartbeat_thread(self):
        """Return an Event in place of starting the Argus heartbeat thread, so no thread outlives the test."""
        # prevent from heartbeat thread to start
        # because it can be left running after the test
        # and break other tests
        return threading.Event()

    def tearDown(self):
        """Replace monitors with a MagicMock and run the regular ClusterTester teardown."""
        self.monitors = MagicMock()
        super().tearDown()

    @property
    def final_event(self) -> TestResultEvent:
        """Return the test result event built from the events of the run."""
        return self._get_test_result_event()

    @property
    def event_summary(self) -> dict:
        """Return the event count per severity."""
        return self.get_event_summary()

    @property
    def events(self) -> dict:
        """Return the formatted events of the run grouped by severity."""
        return get_events_grouped_by_category(_registry=self.events_processes_registry)

    @pytest.fixture(autouse=True, name="event_system")
    def fixture_event_system(self, setup_logging):
        """Run the test with a fresh FakeEventsDevice wired into the events registry."""
        with make_fake_events() as device:
            self._fake_device = device
            self.events_processes_registry = SctEvent._events_processes_registry
            yield

    @pytest.fixture(autouse=True, name="print_output")
    def fixture_print_output(self, event_system, capsys):
        """Print the event summary, test status and short ERROR/CRITICAL messages after the test."""
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
        """No-op: teardown has no extra finalization."""
