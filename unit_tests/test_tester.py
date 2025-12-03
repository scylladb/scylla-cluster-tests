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

import os
import shutil
import logging
import tempfile
import time
import unittest.mock
import threading
from time import sleep
from unittest.mock import MagicMock

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.tester import ClusterTester, silence, TestResultEvent
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.action_logger import get_action_logger
from sdcm.utils.log import MultilineMessagesFormatter, configure_logging, JSONLFormatter
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.sct_events.file_logger import get_events_grouped_by_category
from sdcm.sct_events.events_processes import EventsProcessesRegistry


class FakeSCTConfiguration(SCTConfiguration):
    def _load_environment_variables(self):
        return {
            "config_files": ["test-cases/PR-provision-test-docker.yaml"],
            "cluster_backend": "docker",
            "run_commit_log_check_thread": False,
        }


class ClusterTesterForTests(ClusterTester):
    _sct_log = None
    _final_event = None
    _events = None
    _event_summary = None
    _get_event_summary_cached = None
    _get_events_grouped_by_category_cached = None
    _unittest_final_event = False

    def __init__(self, *args):
        self.logdir = tempfile.mkdtemp()
        self.events_processes_registry = EventsProcessesRegistry(log_dir=self.logdir)
        self.events_processes_registry_patcher = \
            unittest.mock.patch("sdcm.sct_events.base.SctEvent._events_processes_registry",
                                self.events_processes_registry)
        self.events_processes_registry_patcher.start()
        self.actions_log = get_action_logger('tester')
        configure_logging(
            formatters={
                'default': {
                    '()': MultilineMessagesFormatter,
                    'format': '%(message)s'
                },
                'action_logger': {
                    '()': JSONLFormatter,
                    'format': '%(message)s'
                }
            },
            variables={'log_dir': self.logdir}
        )
        super().__init__(*args)

    def init_argus_run(self):
        pass

    def _init_params(self):
<<<<<<< HEAD
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        self.params = FakeSCTConfiguration()

    @pytest.fixture(autouse=True, name='setup_logging')
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
=======
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
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        self.log = logging.getLogger(self.__class__.__name__)
<<<<<<< HEAD
        self.params = FakeSCTConfiguration()
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        self.actions_log = get_action_logger('tester')
        self.logdir = logdir
=======
        self.actions_log = get_action_logger("tester")
        self.logdir = logdir
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    def init_resources(self, loader_info=None, db_info=None, monitor_info=None):
        pass

    def _init_localhost(self):
        return None

    def _init_logging(self):
        pass

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

    def start_argus_heartbeat_thread(self):
        # prevent from heartbeat thread to start
        # because it can be left running after the test
        # and break other tests
        return threading.Event()

    def tearDown(self):
        self.monitors = MagicMock()
        super().tearDown()
        self._validate_results()
        self.events_processes_registry_patcher.stop()

    def _validate_results(self):
        self.result._excinfo = []
        final_event = self.final_event
        unittest_final_event = self.unittest_final_event
        self._remove_errors_from_unittest_results(self._outcome)
        events_by_category = self.events
        sleep(0.3)
        # cache files info before deleting the folder
        sct_log = self.sct_log
        event_summary = self.event_summary
        shutil.rmtree(self.logdir)
        for event_category, total_events in event_summary.items():
            assert len(events_by_category[event_category]) == total_events, \
                f"{event_category}: Contains ({len(events_by_category[event_category])}) while " \
                f"({total_events}) expected:\n{''.join(events_by_category[event_category])}"
            assert final_event.events[event_category][0] == events_by_category[event_category][-1]
        if final_event.test_status == 'SUCCESS':
            assert unittest_final_event is None
            assert str(final_event) in sct_log
        else:
            assert isinstance(unittest_final_event, TestResultEvent)
            assert str(final_event) not in sct_log

    @property
    def final_event(self) -> TestResultEvent:
        if self._final_event:
            return self._final_event
        self._final_event = self.unittest_final_event
        if self._final_event:
            return self._final_event
        self._final_event = self._get_test_result_event()
        return self._final_event

    @property
    def unittest_final_event(self) -> TestResultEvent:
        if self._unittest_final_event is not False:
            return self._unittest_final_event
        final_event = self._get_unittest_final_event()
        self._unittest_final_event = final_event
        return final_event

    def _get_unittest_final_event(self) -> TestResultEvent:
        errors = self._outcome.errors.copy()
        for error in errors:
            if error and error[1] and error[1][1] and isinstance(error[1][1], TestResultEvent):
                return error[1][1]
        return None

    @property
    def sct_log(self):
        if self._sct_log:
            return self._sct_log
        with open(os.path.join(self.logdir, 'sct.log'), encoding="utf-8") as log_file:
            output = log_file.read()
        self._sct_log = output
        return output

    @property
    def event_summary(self) -> dict:
        if self._event_summary:
            return self._event_summary
        self._event_summary = self.get_event_summary()
        return self._event_summary

    @property
    def events(self) -> dict:
        if self._events:
            return self._events
        self._events = get_events_grouped_by_category(_registry=self.events_processes_registry)
        return self._events

    def stop_event_device(self):
        time.sleep(0.5)
        super().stop_event_device()

    def save_schema(self):
        pass

<<<<<<< HEAD
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
    @pytest.fixture(autouse=True, name='event_system')
    def fixture_event_system(self, setup_logging):
        self.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
        yield
        self.teardown_events_processes()

    @pytest.fixture(autouse=True, name="print_output")
    def fixture_print_output(self, event_system, capsys):
        # Parse error report to output only useful message
        filter_message = re.compile(r"((?<=message=)|(?<=exception=)|(?<=error=)).*(?=( failed|$))")
        yield

        with capsys.disabled():
            print(f"\nEVENT_SUMMARY: {self.event_summary}")
            print(f"TEST_STATUS: {self.final_event.test_status}")
            for i, error in enumerate(self.events['ERROR']):
                print(f"ERROR {i}: {filter_message.search(error).group(0)}")
            for i, error in enumerate(self.events['CRITICAL']):
                print(f"CRITICAL {i}: {filter_message.search(error).group(0)}")

    def finalize_teardown(self):
        pass

=======
    @pytest.fixture(autouse=True, name="event_system")
    def fixture_event_system(self, setup_logging):
        self.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
        yield
        self.teardown_events_processes()

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

>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

class SubtestAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
<<<<<<< HEAD
        with self.subTest('SUBTEST1'):
            raise ValueError('Subtest1 failed')
        with self.subTest('SUBTEST2'):
            raise ValueError('Subtest2 failed')
        raise ValueError('Main test also failed')
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        with self.subTest('SUBTEST1'):
            # This wont create error in events
            raise ValueError('Subtest1 failed')
        with self.subTest('SUBTEST2'):
            raise ValueError('Subtest2 failed')
        raise ValueError('Main test also failed')
=======
        with self.subTest("SUBTEST1"):
            # This wont create error in events
            raise ValueError("Subtest1 failed")
        with self.subTest("SUBTEST2"):
            raise ValueError("Subtest2 failed")
        raise ValueError("Main test also failed")
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    @silence()
    def save_email_data(self):
        raise ValueError()

    def _validate_results(self):
        super()._validate_results()
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 2}
        assert 'Subtest1 failed' in self.events['ERROR'][0]
        assert 'save_email_data' in self.events['ERROR'][1]
        assert self.final_event.test_status == 'FAILED'


class CriticalErrorNotCaughtTest(ClusterTesterForTests):
<<<<<<< HEAD
    @staticmethod
    def test():
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)

    def test(self):
=======
    def test(self):
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
        try:
            ClusterHealthValidatorEvent.NodeStatus(
                node="node-1",
                message="Failed by some reason",
                error="Reason to fail",
                severity=Severity.CRITICAL,
            ).publish()
            end_time = time.time() + 2
            while time.time() < end_time:
                time.sleep(0.1)
        except Exception:  # noqa: BLE001
            pass

    def _validate_results(self):
        super()._validate_results()
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert len(self.events['CRITICAL']) == 1
        assert 'ClusterHealthValidatorEvent' in self.events['CRITICAL'][0]
        assert self.final_event.test_status == 'FAILED'


class SubtestAssertAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
<<<<<<< HEAD
        with self.subTest('SUBTEST1'):
            assert False, 'Subtest1 failed'
        with self.subTest('SUBTEST2'):
            assert False, 'Subtest2 failed'
        assert False, 'Main test also failed'
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)
        with self.subTest('SUBTEST1'):
            # This wont create error in events
            assert False, 'Subtest1 failed'
        with self.subTest('SUBTEST2'):
            assert False, 'Subtest2 failed'
        assert False, 'Main test also failed'
=======
        with self.subTest("SUBTEST1"):
            # This wont create error in events
            assert False, "Subtest1 failed"
        with self.subTest("SUBTEST2"):
            assert False, "Subtest2 failed"
        assert False, "Main test also failed"
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)

    @silence()
    def save_email_data(self):
        raise ValueError()

    def _validate_results(self):
        super()._validate_results()
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 2}
        assert 'Subtest1 failed' in self.events['ERROR'][0]
        assert 'save_email_data' in self.events['ERROR'][1]
        assert self.final_event.test_status == 'FAILED'


class TeardownFailsTest(ClusterTesterForTests):
    def test(self):
        pass

    @silence()
    def save_email_data(self):
        raise ValueError()

    def _validate_results(self):
        super()._validate_results()
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert 'save_email_data' in self.final_event.events['ERROR'][0]
        assert self.final_event.test_status == 'FAILED'


class SetupFailsTest(ClusterTesterForTests):
<<<<<<< HEAD
    def __init__(self, *args):
        super().__init__(*args)
        self.addCleanup(self._validate_results)

||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)

=======
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
    def prepare_kms_host(self):
        raise RuntimeError("prepare_kms_host failed")

    def test(self):
        pass

    def tearDown(self):
        self.monitors = MagicMock()
        ClusterTester.tearDown(self)

    def _validate_results(self):
        super()._validate_results()
        self._remove_errors_from_unittest_results(self._outcome)
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert 'prepare_kms_host failed' in self.final_event.events['ERROR'][0]
        assert self.final_event.test_status == 'FAILED'


class TestErrorTest(ClusterTesterForTests):
    def test(self):
        TestFrameworkEvent(
            source=self.__class__.__name__, source_method="test", message="Something went wrong"
        ).publish()

    def _validate_results(self):
        super()._validate_results()
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert self.final_event.test_status == 'FAILED'


class SuccessTest(ClusterTesterForTests):
    def test(self):
        pass

    def _validate_results(self):
        super()._validate_results()
        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'


class SubtestsSuccessTest(ClusterTesterForTests):
    def test(self):
        with self.subTest("SUBTEST1"):
            pass
        with self.subTest("SUBTEST2"):
            pass

<<<<<<< HEAD
    def _validate_results(self):
        super()._validate_results()
        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff)

@pytest.mark.parametrize("test_class, results, outcomes", [
    pytest.param(TeardownFailsTest, {"passed": 1}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
        "TEST_STATUS: FAILED",
        "ERROR 0: save_email_data (silenced)"
    ], id="TeardownFailsTest"),
    pytest.param(SubtestsSuccessTest, {"passed": 1, "subtests": 2}, [
        "EVENT_SUMMARY: {'NORMAL': 2}",
        "TEST_STATUS: SUCCESS"
    ], id="SubtestsSuccessTest"),
    pytest.param(SuccessTest, {"passed": 1}, [
        "EVENT_SUMMARY: {'NORMAL': 2}",
        "TEST_STATUS: SUCCESS"
    ], id="SuccessTest"),
    pytest.param(TestErrorTest, {"passed": 1}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
        "TEST_STATUS: FAILED",
        "ERROR 0: Something went wrong"
    ], id="TestErrorTest"),
    pytest.param(SetupFailsTest, {"failed": 1}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
        "TEST_STATUS: FAILED",
        "ERROR 0: prepare_kms_host failed"
    ], id="SetupFailsTest"),
    pytest.param(CriticalErrorNotCaughtTest, {"passed": 1}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'CRITICAL': 1}",
        "TEST_STATUS: FAILED",
        "CRITICAL 0: Reason to fail",
    ], id="CriticalErrorNotCaughtTest"),
    pytest.param(SubtestAndTeardownFailsTest, {"failed": 3}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
        "TEST_STATUS: FAILED",
        "ERROR 0: save_email_data (silenced)",
        "E           ValueError: Subtest1 failed",
        "E           ValueError: Subtest2 failed",
        "E       ValueError: Main test also failed"
    ], id="SubtestAndTeardownFailsTest"),
    pytest.param(SubtestAssertAndTeardownFailsTest, {"failed": 3}, [
        "EVENT_SUMMARY: {'NORMAL': 2, 'ERROR': 1}",
        "TEST_STATUS: FAILED",
        "ERROR 0: save_email_data (silenced)",
        "E           AssertionError: Subtest1 failed",
        "E           AssertionError: Subtest2 failed",
        "E       AssertionError: Main test also failed",
    ], id="SubtestAssertAndTeardownFailsTest"),
])
def test_tester_subclass(pytester, test_class, results, outcomes):
    # Create a pytest file with the test class. We cannot just use the class directly
    # because it would not be collected. If we made it collectable it would be run as part of standard test run as well.
    # Which we do not want, as it is intended only to be run with pytester
    pytester.makepyfile(f"""
        from unit_tests.conftest import *
        from unit_tests.test_tester import {test_class.__name__}
        {test_class.__name__}.__test__ = True
    """)
    # cause of https://github.com/pytest-dev/pytest/issues/13905
    # we need to run with extra flag -q, so subtest summary output is shown
    result = pytester.runpytest_inprocess('-q')
    summary = result.parseoutcomes()
    for status, count in results.items():
        assert status in summary, f"Status '{status}' not found in results"
        assert summary[status] == count, f"Status '{status}' count mismatch: expected {count}, got {summary[status]}"
    output = result.stdout.str().splitlines()
    for outcome in outcomes:
        assert outcome in output
=======

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
    # Create a pytest file with the test class. We cannot just use the class directly
    # because it would not be collected. If we made it collectable it would be run as part of standard test run as well.
    # Which we do not want, as it is intended only to be run with pytester
    pytester.makepyfile(f"""
        from unit_tests.conftest import *
        from unit_tests.test_tester import {test_class.__name__}
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
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff)
