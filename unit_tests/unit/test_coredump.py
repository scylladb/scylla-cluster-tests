import time
from abc import abstractmethod
from pathlib import Path
from unittest.mock import MagicMock

import google.api_core.exceptions
import pytest
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from click.testing import CliRunner

import sct
from sdcm.cluster import BaseNode
from sdcm.coredump import CoredumpExportSystemdThread, CoreDumpInfo, CoredumpExportFileThread, CoredumpThreadBase
from sdcm.utils import gce_utils
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


@pytest.fixture(autouse=True)
def fake_signed_upload_url(monkeypatch):
    monkeypatch.setattr(
        "sdcm.coredump.sign_coredump_upload_url",
        lambda blob_name: f"https://storage.googleapis.com/scylla-sct-coredumps/{blob_name}?X-Goog-Signature=fake",
    )


@pytest.mark.usefixtures("events")
class CoredumpExportTestBase:
    maxDiff = None
    test_data_folder: str = None

    @pytest.fixture(autouse=True)
    def inject_test_data_dir(self, test_data_dir):
        self.test_data_dir = test_data_dir

    @pytest.fixture(autouse=True)
    def inject_tmp_path(self, tmp_path):
        self._tmp_path = tmp_path

    @abstractmethod
    def _init_target_coredump_class(self, test_name: str) -> CoredumpThreadBase:
        pass

    def _run_coredump_with_fake_remoter(self, test_name: str):
        coredump_thread = self._init_target_coredump_class(test_name)
        coredump_thread.start()
        time.sleep(1)
        coredump_thread.stop()
        coredump_thread.join(20)
        assert not coredump_thread.is_alive(), "CoredumpExportThread thread did not stop in 20 seconds"
        results = coredump_thread.get_results()
        expected_results = coredump_thread.load_expected_results(
            str(self.test_data_dir / "test_coredump" / self.test_data_folder / (test_name + "_results.json"))
        )
        for coredump_status, expected_coredump_list in expected_results.items():
            result_coredump_list = results[coredump_status]
            try:
                assert expected_coredump_list == result_coredump_list
            except Exception as exc:  # noqa: BLE001
                raise AssertionError(
                    f"Got unexpected results for {coredump_status}: {str(result_coredump_list)}\n{str(exc)}"
                ) from exc


class CoredumpExportExceptionTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = "systemd"

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportSystemdTestThread:
        coredump_thread = CoredumpExportSystemdTestThread(
            FakeNode(
                MockRemoter(
                    responses=str(
                        self.test_data_dir / "test_coredump" / self.test_data_folder / (test_name + "_remoter.json")
                    )
                ),
                str(self._tmp_path / "logdir"),
            ),
            5,
        )
        coredump_thread.max_coredump_thread_exceptions = 2
        return coredump_thread

    def test_with_exceptions_limit_reached(self):
        self._run_coredump_with_fake_remoter("exceptions_limit_reached_test")

    def test_with_exceptions_limit_not_reached(self):
        self._run_coredump_with_fake_remoter("exceptions_limit_not_reached_test")


class CoredumpExportSystemdTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = "systemd"

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportSystemdTestThread:
        return CoredumpExportSystemdTestThread(
            FakeNode(
                MockRemoter(
                    responses=str(
                        self.test_data_dir / "test_coredump" / self.test_data_folder / (test_name + "_remoter.json")
                    )
                ),
                str(self._tmp_path / "logdir"),
            ),
            5,
        )

    def test_success_test(self):
        self._run_coredump_with_fake_remoter("success_test")

    def test_success_test_systemd_248(self):
        self._run_coredump_with_fake_remoter("success_test_systemd_248")

    def test_fail_upload_test(self):
        self._run_coredump_with_fake_remoter("fail_upload_test")

    def test_fail_get_list_test(self):
        self._run_coredump_with_fake_remoter("fail_get_list_test")


class CoredumpExportFileTest(CoredumpExportTestBase):
    maxDiff = None
    test_data_folder = "filebased"

    def _init_target_coredump_class(self, test_name: str) -> CoredumpExportFileTestThread:
        return CoredumpExportFileTestThread(
            FakeNode(
                MockRemoter(
                    responses=str(
                        self.test_data_dir / "test_coredump" / self.test_data_folder / (test_name + "_remoter.json")
                    )
                ),
                str(self._tmp_path / "logdir"),
            ),
            5,
            coredump_directories=["/var/lib/scylla/coredumps"],
        )

    def test_success_test(self):
        self._run_coredump_with_fake_remoter("success_test")

    def test_fail_upload_test(self):
        self._run_coredump_with_fake_remoter("fail_upload_test")

    def test_fail_get_list_test(self):
        self._run_coredump_with_fake_remoter("fail_get_list_test")


def _upload_thread() -> CoredumpExportSystemdThread:
    thread = CoredumpExportSystemdThread(node=MagicMock(), max_core_upload_limit=1)
    thread.sent_config = []
    thread.node.remoter.send_files.side_effect = lambda src, dst: thread.sent_config.append(Path(src).read_text())
    return thread


def _core(thread) -> CoreDumpInfo:
    return CoreDumpInfo(pid="1", node=thread.node, corefile="/var/lib/systemd/coredump/core.scylla.1.zst")


@pytest.mark.parametrize(
    "error",
    [
        NoCredentialsError(),
        PartialCredentialsError(provider="env", cred_var="AWS_SECRET_ACCESS_KEY"),
        ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetSecretValue"),
        ClientError({"Error": {"Code": "InternalServiceError"}}, "GetSecretValue"),
    ],
    ids=["no-credentials", "partial-credentials", "other-account", "keystore-error"],
)
def test_upload_without_signed_url_falls_back_to_public_bucket(monkeypatch, error):
    def sign(blob_name):
        raise error

    monkeypatch.setattr("sdcm.coredump.sign_coredump_upload_url", sign)
    thread = _upload_thread()
    core = _core(thread)

    thread._upload_coredump(core)

    assert thread.sent_config == ['url = "https://upload.scylladb.com/core.scylla.1./core.scylla.1.zst"\n']
    assert core.download_url == "https://storage.cloud.google.com/upload.scylladb.com/core.scylla.1./core.scylla.1.zst"
    assert core.download_instructions.startswith("gsutil cp gs://upload.scylladb.com/core.scylla.1./")


def test_signed_url_stays_out_of_the_command():
    thread = _upload_thread()

    thread._upload_coredump(_core(thread))

    assert thread.sent_config == [
        'url = "https://storage.googleapis.com/scylla-sct-coredumps/core.scylla.1./core.scylla.1.zst'
        '?X-Goog-Signature=fake"\n'
    ]
    cmd = thread.node.remoter.run.call_args.args[0]
    assert "X-Goog-Signature" not in cmd
    assert "--config /tmp/core.scylla.1.zst.curl;" in cmd
    assert cmd.endswith("rm -f /tmp/core.scylla.1.zst.curl; exit $rc")


def test_signed_url_ignores_gce_endpoint_override(monkeypatch):
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:5000")
    monkeypatch.setattr(gce_utils, "KeyStore", lambda: MagicMock(get_json=lambda name: {"project_id": "p"}))
    monkeypatch.setattr(gce_utils.service_account.Credentials, "from_service_account_info", MagicMock())
    client = MagicMock()
    monkeypatch.setattr(gce_utils.storage, "Client", client)

    gce_utils.sign_coredump_upload_url("x/y.zst")

    assert "client_options" not in client.call_args.kwargs


def test_configure_coredump_bucket_rerun_does_not_duplicate(monkeypatch):
    policy = MagicMock(bindings=[{"role": "roles/storage.admin", "members": {"user:owner@scylladb.com"}}])
    bucket = MagicMock(location="US-EAST1")
    bucket.get_iam_policy.return_value = policy
    client = MagicMock()
    client.create_bucket.side_effect = google.api_core.exceptions.Conflict("exists")
    client.get_bucket.return_value = bucket
    monkeypatch.setattr(gce_utils, "_coredump_storage_client", lambda: (client, {"client_email": "sct@p.iam"}))

    gce_utils.configure_coredump_bucket(region="us-east1", coldline_after_days=14, retention_days=90)
    gce_utils.configure_coredump_bucket(region="us-east1", coldline_after_days=14, retention_days=90)

    assert policy.bindings == [
        {"role": "roles/storage.admin", "members": {"user:owner@scylladb.com"}},
        {"role": "roles/storage.objectViewer", "members": {"domain:scylladb.com"}},
        {"role": "roles/storage.objectCreator", "members": {"serviceAccount:sct@p.iam"}},
    ]
    assert bucket.clear_lifecycle_rules.call_count == 2
    bucket.add_lifecycle_set_storage_class_rule.assert_called_with(
        "COLDLINE", age=14, matches_storage_class=["STANDARD"]
    )
    bucket.add_lifecycle_delete_rule.assert_called_with(age=90)
    assert bucket.storage_class == "STANDARD"
    assert bucket.iam_configuration.uniform_bucket_level_access_enabled is True
    assert bucket.iam_configuration.public_access_prevention == "enforced"


@pytest.mark.parametrize(
    "args",
    [
        ["--coredump-retention-days", "7"],
        ["--coredump-coldline-after-days", "0"],
        ["--coredump-coldline-after-days", "90"],
    ],
    ids=["retention-deletes-cores", "coldline-zero", "coldline-not-below-retention"],
)
def test_prepare_global_resources_rejects_unsafe_retention(monkeypatch, args):
    configure = MagicMock()
    monkeypatch.setattr(sct, "configure_coredump_bucket", configure)

    result = CliRunner().invoke(sct.prepare_global_resources, args)

    assert result.exit_code == 2
    configure.assert_not_called()
