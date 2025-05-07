import unittest
import os
import time
import tempfile
from abc import abstractmethod

from sdcm.cluster import BaseNode
from sdcm.coredump import CoredumpExportSystemdThread, CoreDumpInfo, CoredumpExportFileThread, CoredumpThreadBase
from unit_tests.lib.data_pickle import Pickler
from unit_tests.lib.mock_remoter import MockRemoter


class FakeNode(BaseNode):

    def __init__(self, remoter, logdir):
        self.remoter = remoter
        os.makedirs(logdir, exist_ok=True)
        self.logdir = logdir

    def wait_ssh_up(self, verbose=False, timeout=60):
        return True


class CoredumpExportSystemdTestThread(CoredumpExportSystemdThread):
    lookup_period = 0

    def __init__(self, node: 'BaseNode', max_core_upload_limit: int):
        self.got_cores = []
        super().__init__(node, max_core_upload_limit)

    def publish_event(self, core_info: CoreDumpInfo):
        pass

    def _localize_results(self):
        output = {'exception': self.exception}
        for group_name in ['found', 'in_progress', 'completed', 'uploaded']:
            group = getattr(self, group_name)
            group_data = []
            for data in group:
                group_data.append(data)
            output[group_name] = group_data
        return output

    def get_results(self) -> dict:
        return Pickler.to_data(self._localize_results())

    @staticmethod
    def load_expected_results(filepath: str) -> dict:
        return Pickler.load_data_from_file(filepath)

    def save_results(self, filepath):
        return Pickler.save_to_file(filepath, self._localize_results())


class CoredumpExportFileTestThread(CoredumpExportFileThread):
    lookup_period = 0
    checkup_time_core_to_complete = 0

    def __init__(self, node: 'BaseNode', max_core_upload_limit: int, coredump_directories=None):
        self.got_cores = []
        super().__init__(node, max_core_upload_limit, coredump_directories)

    def publish_event(self, core_info: CoreDumpInfo):
        pass

    def _localize_results(self):
        output = {}
        for group_name in ['found', 'in_progress', 'completed', 'uploaded']:
            group = getattr(self, group_name)
            group_data = []
            for data in group:
                group_data.append(data)
            output[group_name] = group_data
        return output

    def get_results(self) -> dict:
        return Pickler.to_data(self._localize_results())

    @staticmethod
    def load_expected_results(filepath: str) -> dict:
        return Pickler.load_data_from_file(filepath)

    def save_results(self, filepath):
        return Pickler.save_to_file(filepath, self._localize_results())


class CoredumpExportTestBase(unittest.TestCase):
    maxDiff = None
    test_data_folder: str = None

    @abstractmethod
    def _init_target_coredump_class(self, test_name: str) -> CoredumpThreadBase:
        pass

    def _run_coredump_with_fake_remoter(self, test_name: str):
        coredump_thread = self._init_target_coredump_class(test_name)
        coredump_thread.start()
        time.sleep(1)
        coredump_thread.stop()
        coredump_thread.join(20)
        self.assertFalse(coredump_thread.is_alive(), 'CoredumpExportThread thread did not stop in 20 seconds')
        results = coredump_thread.get_results()
        expected_results = coredump_thread.load_expected_results(
            os.path.join(os.path.dirname(__file__), 'test_data', 'test_coredump', self.test_data_folder,
                         test_name + '_results.json')
        )
        for coredump_status, expected_coredump_list in expected_results.items():
            result_coredump_list = results[coredump_status]
            try:
                self.assertEqual(expected_coredump_list, result_coredump_list)
            except Exception as exc:  # noqa: BLE001
                raise AssertionError(
                    f'Got unexpected results for {coredump_status}: {str(result_coredump_list)}\n{str(exc)}') from exc


class CoredumpExportExceptionTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = 'systemd'

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportSystemdTestThread:
        coredump_thread = CoredumpExportSystemdTestThread(
            FakeNode(
                MockRemoter(
                    responses=os.path.join(
                        os.path.dirname(__file__), 'test_data', 'test_coredump', self.test_data_folder,
                        test_name + '_remoter.json'
                    )
                ),
                tempfile.mkdtemp()
            ),
            5
        )
        coredump_thread.max_coredump_thread_exceptions = 2
        return coredump_thread

    def test_with_exceptions_limit_reached(self):
        self._run_coredump_with_fake_remoter('exceptions_limit_reached_test')

    def test_with_exceptions_limit_not_reached(self):
        self._run_coredump_with_fake_remoter('exceptions_limit_not_reached_test')


class CoredumpExportSystemdTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = 'systemd'

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportSystemdTestThread:
        return CoredumpExportSystemdTestThread(
            FakeNode(
                MockRemoter(
                    responses=os.path.join(
                        os.path.dirname(__file__), 'test_data', 'test_coredump', self.test_data_folder,
                        test_name + '_remoter.json'
                    )
                ),
                tempfile.mkdtemp()
            ),
            5
        )

    def test_success_test(self):
        self._run_coredump_with_fake_remoter('success_test')

    def test_success_test_systemd_248(self):
        self._run_coredump_with_fake_remoter('success_test_systemd_248')

    def test_fail_upload_test(self):
        self._run_coredump_with_fake_remoter('fail_upload_test')

    def test_fail_get_list_test(self):
        self._run_coredump_with_fake_remoter('fail_get_list_test')


class CoredumpExportFileTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = 'filebased'

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportFileTestThread:
        return CoredumpExportFileTestThread(
            FakeNode(
                MockRemoter(
                    responses=os.path.join(
                        os.path.dirname(__file__), 'test_data', 'test_coredump', self.test_data_folder,
                        test_name + '_remoter.json'
                    )
                ),
                tempfile.mkdtemp()
            ),
            5,
            coredump_directories=['/var/lib/scylla/coredumps']
        )

    def test_success_test(self):
        self._run_coredump_with_fake_remoter('success_test')

    def test_fail_upload_test(self):
        self._run_coredump_with_fake_remoter('fail_upload_test')

    def test_fail_get_list_test(self):
        self._run_coredump_with_fake_remoter('fail_get_list_test')
