import unittest
import os
import time
import tempfile
from sdcm.coredump import CoredumpExportThread, CoreDumpInfo
from unit_tests.lib.data_pickle import Pickler
from unit_tests.lib.mock_remoter import MockRemoter


class FakeNode:
    def __init__(self, remoter, logdir):
        self.remoter = remoter
        os.makedirs(logdir, exist_ok=True)
        self.logdir = logdir

    def is_ubuntu14(self):
        return False

    def wait_ssh_up(self, verbose=False):
        return True


class CoredumpExportTestThread(CoredumpExportThread):
    lookup_period = 0

    def __init__(self, node: 'BaseNode', max_core_upload_limit: int):
        self.got_cores = []
        super().__init__(node, max_core_upload_limit)

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

    def load_expected_results(self, filepath: str) -> dict:
        return Pickler.load_data_from_file(filepath)

    def save_results(self, filepath):
        return Pickler.save_to_file(filepath, self._localize_results())


class CoredumpTest(unittest.TestCase):
    maxDiff = None

    def _run_coredump_with_fake_remoter(self, test_name: str):
        th = CoredumpExportTestThread(
            FakeNode(
                MockRemoter(
                    responses=os.path.join(
                        os.path.dirname(__file__), 'test_data', 'test_coredump', test_name + '_remoter.json'
                    )
                ),
                tempfile.mkdtemp()
            ),
            5
        )
        th.start()
        time.sleep(1)
        th.stop()
        th.join(20)
        self.assertFalse(th.is_alive(), 'CoredumpExportThread thread did not stop in 20 seconds')
        results = th.get_results()
        expected_results = th.load_expected_results(
            os.path.join(os.path.dirname(__file__), 'test_data', 'test_coredump', test_name + '_results.json')
        )
        for coredump_status, expected_coredump_list in expected_results.items():
            result_coredump_list = results[coredump_status]
            self.assertEqual(
                len(expected_coredump_list),
                len(result_coredump_list),
                f'got unexpected results for {coredump_status}: {str(result_coredump_list)}'
            )
            self.assertEqual(expected_coredump_list, result_coredump_list)

    def test_success_test(self):
        self._run_coredump_with_fake_remoter('success_test')

    def test_fail_upload_test(self):
        self._run_coredump_with_fake_remoter('fail_upload_test')

    def test_fail_get_list_test(self):
        self._run_coredump_with_fake_remoter('fail_get_list_test')
