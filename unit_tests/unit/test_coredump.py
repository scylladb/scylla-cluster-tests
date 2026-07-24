import time
from pathlib import Path

import pytest

from sdcm.cluster import BaseNode
from sdcm.coredump import CoredumpExportSystemdThread, CoreDumpInfo, CoredumpExportFileThread, CoredumpThreadBase
from unit_tests.lib.data_pickle import Pickler
from unit_tests.lib.mock_remoter import MockRemoter


class FakeNode(BaseNode):
    def __init__(self, remoter, logdir):
        self.remoter = remoter
        Path(logdir).mkdir(parents=True, exist_ok=True)
        self.logdir = logdir

    def wait_ssh_up(self, verbose=False, timeout=60):
        return True


class CoredumpExportSystemdTestThread(CoredumpExportSystemdThread):
    lookup_period = 0

    def __init__(self, node: "BaseNode", max_core_upload_limit: int):
        self.got_cores = []
        super().__init__(node, max_core_upload_limit)

    def publish_event(self, core_info: CoreDumpInfo):
        pass

    def _localize_results(self):
        output = {"exception": self.exception}
        for group_name in ["found", "in_progress", "completed", "uploaded"]:
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

    def __init__(self, node: "BaseNode", max_core_upload_limit: int, coredump_directories=None):
        self.got_cores = []
        super().__init__(node, max_core_upload_limit, coredump_directories)

    def publish_event(self, core_info: CoreDumpInfo):
        pass

    def _localize_results(self):
        output = {}
        for group_name in ["found", "in_progress", "completed", "uploaded"]:
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


@pytest.fixture
def systemd_coredump_thread_factory(test_data_dir, tmp_path):
    def make_thread(test_name: str) -> CoredumpExportSystemdTestThread:
        return CoredumpExportSystemdTestThread(
            FakeNode(
                MockRemoter(responses=str(test_data_dir / "test_coredump" / "systemd" / (test_name + "_remoter.json"))),
                str(tmp_path / "logdir"),
            ),
            5,
        )

    return make_thread


@pytest.fixture
def systemd_exception_coredump_thread_factory(systemd_coredump_thread_factory):
    def make_thread(test_name: str) -> CoredumpExportSystemdTestThread:
        coredump_thread = systemd_coredump_thread_factory(test_name)
        coredump_thread.max_coredump_thread_exceptions = 2
        return coredump_thread

    return make_thread


@pytest.fixture
def file_coredump_thread_factory(test_data_dir, tmp_path):
    def make_thread(test_name: str) -> CoredumpExportFileTestThread:
        return CoredumpExportFileTestThread(
            FakeNode(
                MockRemoter(
                    responses=str(test_data_dir / "test_coredump" / "filebased" / (test_name + "_remoter.json"))
                ),
                str(tmp_path / "logdir"),
            ),
            5,
            coredump_directories=["/var/lib/scylla/coredumps"],
        )

    return make_thread


def run_coredump_export_case(
    test_data_dir: Path, test_data_folder: str, coredump_thread: CoredumpThreadBase, test_name: str
):
    coredump_thread.start()
    time.sleep(1)
    coredump_thread.stop()
    coredump_thread.join(20)
    assert not coredump_thread.is_alive(), "CoredumpExportThread thread did not stop in 20 seconds"
    results = coredump_thread.get_results()
    expected_results = coredump_thread.load_expected_results(
        str(test_data_dir / "test_coredump" / test_data_folder / (test_name + "_results.json"))
    )
    for coredump_status, expected_coredump_list in expected_results.items():
        result_coredump_list = results[coredump_status]
        try:
            assert expected_coredump_list == result_coredump_list
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"Got unexpected results for {coredump_status}: {result_coredump_list!s}\n{exc!s}"
            ) from exc


@pytest.mark.usefixtures("events")
@pytest.mark.parametrize(
    "test_name",
    [
        pytest.param("exceptions_limit_reached_test", id="exceptions-limit-reached"),
        pytest.param("exceptions_limit_not_reached_test", id="exceptions-limit-not-reached"),
    ],
)
def test_coredump_export_systemd_exception_case(test_data_dir, systemd_exception_coredump_thread_factory, test_name):
    coredump_thread = systemd_exception_coredump_thread_factory(test_name)
    run_coredump_export_case(test_data_dir, "systemd", coredump_thread, test_name)


@pytest.mark.usefixtures("events")
@pytest.mark.parametrize(
    "test_name",
    [
        pytest.param("success_test", id="success"),
        pytest.param("success_test_systemd_248", id="success-systemd-248"),
        pytest.param("fail_upload_test", id="fail-upload"),
        pytest.param("fail_get_list_test", id="fail-get-list"),
    ],
)
def test_coredump_export_systemd_case(test_data_dir, systemd_coredump_thread_factory, test_name):
    coredump_thread = systemd_coredump_thread_factory(test_name)
    run_coredump_export_case(test_data_dir, "systemd", coredump_thread, test_name)


@pytest.mark.usefixtures("events")
@pytest.mark.parametrize(
    "test_name",
    [
        pytest.param("success_test", id="success"),
        pytest.param("fail_upload_test", id="fail-upload"),
        pytest.param("fail_get_list_test", id="fail-get-list"),
    ],
)
def test_coredump_export_file_case(test_data_dir, file_coredump_thread_factory, test_name):
    coredump_thread = file_coredump_thread_factory(test_name)
    run_coredump_export_case(test_data_dir, "filebased", coredump_thread, test_name)
