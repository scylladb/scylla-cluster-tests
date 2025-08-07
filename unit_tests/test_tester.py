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

import time
import threading
from unittest.mock import MagicMock
import logging
import pytest

from sdcm.utils.decorators import retrying
from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.tester import ClusterTester, silence, TestResultEvent
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.sct_events.file_logger import get_events_grouped_by_category
from sdcm.utils.action_logger import get_action_logger
from unit_tests.lib.events_utils import EventsUtilsMixin


class FakeSCTConfiguration(SCTConfiguration):
    def _load_environment_variables(self):
        return {
            'config_files': ['test-cases/PR-provision-test-docker.yaml'],
            'cluster_backend': 'docker',
            'run_commit_log_check_thread': False
        }


ClusterTester.__test__ = False


class ClusterTesterForTests(ClusterTester, EventsUtilsMixin):
    __test__ = True

    k8s_clusters = None
    argus_heartbeat_stop_signal = threading.Event()

    def init_argus_run(self):
        self.argus_heartbeat_stop_signal = threading.Event()

    def _init_params(self):
        self.params = FakeSCTConfiguration()

    @pytest.fixture(autouse=True, name='setup_logging')
    def fixture_setup_logging(self, tmp_path):
        self._init_logging(tmp_path / self.__class__.__name__)

    def _init_logging(self, logdir):
        self.log = logging.getLogger(self.__class__.__name__)
        self.actions_log = get_action_logger('tester')
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

    def save_schema(self):
        pass

    @pytest.fixture(autouse=True, name='event_system')
    def fixture_event_system(self, setup_logging):
        self.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
        yield
        self.teardown_events_processes()


@pytest.mark.xfail(reason="this test fails if run after `test_test_user_batch_custom_time`", strict=False)
class SubtestAndTeardownFailsTest(ClusterTesterForTests):

    def test(self):
        with self.subTest('SUBTEST1'):
            raise ValueError('Subtest1 failed')
        with self.subTest('SUBTEST2'):
            raise ValueError('Subtest2 failed')
        raise ValueError('Main test also failed')

    @silence()
    def save_email_data(self):
        raise ValueError()

    @pytest.fixture(scope='function')
    def validate(self, event_system):
        yield
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.

        @retrying(n=5, sleep_time=1, allowed_exceptions=(AssertionError,))
        def wait_for_summary():
            assert self.event_summary == {"NORMAL": 2, "ERROR": 3}

        wait_for_summary()
        assert 'Subtest1 failed' in self.events['ERROR'][1]
        assert 'save_email_data' in self.events['ERROR'][0]
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


class CriticalErrorNotCaughtTest(ClusterTesterForTests):

    @pytest.mark.override_pass
    def test(self):
        try:
            ClusterHealthValidatorEvent.NodeStatus(
                node='node-1',
                message='Failed by some reason',
                error='Reason to fail',
                severity=Severity.CRITICAL,
            ).publish()
            end_time = time.time() + 2
            while time.time() < end_time:
                time.sleep(0.1)
        except Exception:  # noqa: BLE001
            pass

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert len(self.events['CRITICAL']) == 1
        assert 'ClusterHealthValidatorEvent' in self.events['CRITICAL'][0]
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


@pytest.mark.xfail(reason="this test fails if run after `test_test_user_batch_custom_time`", strict=False)
class SubtestAssertAndTeardownFailsTest(ClusterTesterForTests):

    def test(self):
        with self.subTest('SUBTEST1'):
            assert False, 'Subtest1 failed'
        with self.subTest('SUBTEST2'):
            assert False, 'Subtest2 failed'
        assert False, 'Main test also failed'

    @silence()
    def save_email_data(self):
        raise ValueError()

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        @retrying(n=5, sleep_time=1, allowed_exceptions=(AssertionError,))
        def wait_for_summary():
            assert self.event_summary == {'NORMAL': 2, 'ERROR': 3}
        wait_for_summary()
        assert "save_email_data" in self.events["ERROR"][0]
        assert 'Subtest1 failed' in self.events['ERROR'][1]
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


class TeardownFailsTest(ClusterTesterForTests):

    def test(self):
        pass

    @silence()
    def save_email_data(self):
        raise ValueError()

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert 'save_email_data' in self.final_event.events['ERROR'][0]
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


class SetupFailsTest(ClusterTesterForTests):

    def prepare_kms_host(self):
        raise RuntimeError('prepare_kms_host failed')

    @pytest.mark.override_pass
    def test(self):
        pass

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert 'prepare_kms_host failed' in self.final_event.events['ERROR'][0]
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


class TestErrorTest(ClusterTesterForTests):

    def test(self):
        TestFrameworkEvent(
            source=self.__class__.__name__,
            source_method='test',
            message="Something went wrong"
        ).publish()

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert self.final_event.test_status == 'FAILED'

    def finalize_teardown(self):
        pass


class SuccessTest(ClusterTesterForTests):

    def test(self):
        pass

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'


class SubtestsSuccessTest(ClusterTesterForTests):

    def test(self):
        with self.subTest('SUBTEST1'):
            pass
        with self.subTest('SUBTEST2'):
            pass

    @pytest.fixture(autouse=True)
    def validate(self, event_system):
        yield

        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'
