from time import sleep
import os
import shutil
import logging

from sdcm.tester import ClusterTester, silence, TestResultEvent
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.log import MultilineMessagesFormatter, configure_logging
from sdcm.utils.common import generate_random_string


class FakeSCTConfiguration(SCTConfiguration):
    def _load_environment_variables(self):
        return {
            'config_files': ['test-cases/PR-provision-test-docker.yaml'],
            'cluster_backend': 'docker',
            'store_results_in_elasticsearch': False
        }


class ClusterTesterForTests(ClusterTester):
    _sct_log = None
    _final_event = None
    _events = None
    _event_summary = None
    _get_event_summary_cached = None
    _get_events_grouped_by_category_cached = None

    def __init__(self, *args):
        self.logdir = os.path.join('/tmp', generate_random_string(10))
        os.mkdir(self.logdir)
        configure_logging(
            formatters={
                'default': {
                    '()': MultilineMessagesFormatter,
                    'format': '%(message)s'
                }
            },
            variables={'log_dir': self.logdir}
        )
        super().__init__(*args)

    def _init_params(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.params = FakeSCTConfiguration()

    def init_resources(self, loader_info=None, db_info=None, monitor_info=None):
        pass

    def _init_localhost(self):
        return None

    def _init_logging(self):
        pass

    @staticmethod
    def update_certificates():
        pass

    @staticmethod
    def _create_es_connection():
        return None

    @silence()
    def send_email(self):
        pass

    def tearDown(self):
        super().tearDown()
        final_event = self.final_event
        self._remove_errors_from_unittest_results(self._outcome)
        events_by_category = self.events
        sleep(0.3)
        # cache files info before deleting the folder
        sct_log = self.sct_log  # pylint: disable=pointless-statement
        event_summary = self.event_summary  # pylint: disable=pointless-statement
        shutil.rmtree(self.logdir)
        for event_category, total_events in event_summary.items():
            assert len(events_by_category[event_category]) == total_events, \
                f"{event_category}: Contains ({len(events_by_category[event_category])}) while ({total_events}) expected:\n{''.join(events_by_category[event_category])}"
            assert final_event.events[event_category][0] == events_by_category[event_category][-1]
        assert str(final_event._head) in sct_log  # pylint: disable=protected-access
        assert str(final_event._ending) in sct_log  # pylint: disable=protected-access
        assert str(final_event.test_status) in sct_log
        if final_event.test_status == 'SUCCESS':
            assert self.unittest_final_event is None
        assert str(final_event) in self.sct_log

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
        errors = self._outcome.errors.copy()
        for error in errors:
            if error and error[1] and error[1][1] and isinstance(error[1][1], TestResultEvent):
                return error[1][1]
        return None

    @property
    def sct_log(self):
        if self._sct_log:
            return self._sct_log
        with open(os.path.join(self.logdir, 'sct.log'), 'r') as log_file:
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
        self._events = self.get_events_grouped_by_category()
        return self._events


class SubtestAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
        with self.subTest('SUBTEST1'):
            raise ValueError('Subtest1 failed')
        with self.subTest('SUBTEST2'):
            raise ValueError('Subtest2 failed')
        raise ValueError('Main test also failed')

    @silence()
    def send_email(self):
        raise ValueError()

    def tearDown(self):
        super().tearDown()
        assert self.final_event.test_status == 'FAILED'
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 2}
        assert 'Subtest1 failed' in self.events['ERROR'][0]
        assert 'send_email' in self.events['ERROR'][1]


class SubtestAssertAndTeardownFailsTest(ClusterTesterForTests):
    def test(self):
        with self.subTest('SUBTEST1'):
            assert False, 'Subtest1 failed'
        with self.subTest('SUBTEST2'):
            assert False, 'Subtest2 failed'
        assert False, 'Main test also failed'

    @silence()
    def send_email(self):
        raise ValueError()

    def tearDown(self):
        super().tearDown()
        assert self.final_event.test_status == 'FAILED'
        # While running from pycharm and from hydra run-test exception inside subTest won't stop the test,
        #  under hydra unit_test it stops running it and you don't see exception from next subtest.
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 2}
        assert 'Subtest1 failed' in self.events['ERROR'][0]
        assert 'send_email' in self.events['ERROR'][1]


class TeardownFailsTest(ClusterTesterForTests):
    def test(self):
        pass

    @silence()
    def send_email(self):
        raise ValueError()

    def tearDown(self):
        super().tearDown()
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert self.final_event.test_status == 'FAILED'
        assert 'send_email' in self.final_event.events['ERROR'][0]


class SetupFailsTest(ClusterTesterForTests):
    def __init__(self, *args):
        super().__init__(*args)
        self.addCleanup(self._tear_down)

    def update_certificates(self):
        raise RuntimeError('update_certificates failed')

    def test(self):
        pass

    def _tear_down(self):
        self._remove_errors_from_unittest_results(self._outcome)
        assert self.event_summary == {'NORMAL': 2, 'ERROR': 1}
        assert self.final_event.test_status == 'FAILED'
        assert 'update_certificates failed' in self.final_event.events['ERROR'][0]


class SuccessTest(ClusterTesterForTests):
    def test(self):
        pass

    def tearDown(self):
        super().tearDown()
        assert self.unittest_final_event is None
        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'


class SubtestsSuccessTest(ClusterTesterForTests):
    def test(self):
        with self.subTest('SUBTEST1'):
            pass
        with self.subTest('SUBTEST2'):
            pass

    def tearDown(self):
        super().tearDown()
        assert self.unittest_final_event is None
        assert self.event_summary == {'NORMAL': 2}
        assert self.final_event.test_status == 'SUCCESS'
