"""Native minicloud process on macOS: preflight, start, reuse and stop."""

import os
from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError
from sdcm.utils.minicloud.manager import MinicloudManager


@pytest.fixture
def native_config(tmp_path):
    return MinicloudConfig(
        native=True,
        port=5050,
        lock_guest_memory=False,
        state_dir=str(tmp_path / "state"),
        log_file=str(tmp_path / "minicloud.log"),
    )


def test_preflight_skips_kvm_and_docker_checks(native_config):
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=True),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_preflight_fails_without_binary(native_config):
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None),
    ):
        with pytest.raises(MinicloudError, match="'minicloud' is not on PATH"):
            manager.preflight_check(skip_aws_creds=True)


def test_preflight_fails_without_tund_socket(native_config):
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=False),
    ):
        with pytest.raises(MinicloudError, match="minicloud-tund.sock"):
            manager.preflight_check(skip_aws_creds=True)


def test_preflight_rejects_guest_memory_locking(native_config):
    native_config.lock_guest_memory = True
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=True),
    ):
        with pytest.raises(MinicloudError, match="cannot lock guest memory"):
            manager.preflight_check(skip_aws_creds=True)


def test_preflight_accepts_running_endpoint(native_config):
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_start_launches_binary_and_writes_pid(native_config):
    manager = MinicloudManager(config=native_config)
    process = MagicMock(pid=4242)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch.object(MinicloudManager, "_wait_for_health"),
        patch.object(MinicloudManager, "_start_log_streaming"),
        patch.object(MinicloudManager, "_setup_host_networking") as networking,
        patch("sdcm.utils.minicloud.manager.subprocess.Popen", return_value=process) as popen,
    ):
        manager.start()

    cmd = popen.call_args[0][0]
    assert cmd[0] == "minicloud"
    assert cmd[cmd.index("--port") + 1] == "5050"
    assert cmd[cmd.index("--cache-dir") + 1] == native_config.state_dir
    assert "--intercept-s3" in cmd
    assert "--lock-guest-memory" not in cmd
    assert popen.call_args.kwargs["start_new_session"] is True
    assert popen.call_args.kwargs["env"]["AWS_REGION"] == native_config.region
    assert open(native_config.pid_file).read() == "4242"
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5050"
    networking.assert_not_called()


def test_start_reuses_healthy_endpoint(native_config):
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=True),
        patch.object(MinicloudManager, "_start_log_streaming") as streaming,
        patch("sdcm.utils.minicloud.manager.subprocess.Popen") as popen,
        patch("sdcm.utils.minicloud.manager.subprocess.run") as run,
    ):
        manager.start()

    popen.assert_not_called()
    run.assert_not_called()
    streaming.assert_called_once()
    assert os.environ["SCT_MINICLOUD_ENDPOINT_URL"] == "http://localhost:5050"


def test_stop_terminates_process_and_removes_pid_file(native_config):
    os.makedirs(native_config.state_dir)
    with open(native_config.pid_file, "w") as fh:
        fh.write("4242")
    manager = MinicloudManager(config=native_config)
    alive = iter([4242, 4242, None, None])
    with (
        patch.object(MinicloudManager, "_native_pid", side_effect=lambda: next(alive)),
        patch("sdcm.utils.minicloud.manager.os.kill") as kill,
        patch("sdcm.utils.minicloud.manager.subprocess.run") as run,
    ):
        manager.stop()

    kill.assert_called_once()
    assert kill.call_args[0][0] == 4242
    run.assert_not_called()
    assert not os.path.exists(native_config.pid_file)


def test_is_running_reads_pid(native_config):
    manager = MinicloudManager(config=native_config)
    with patch.object(MinicloudManager, "_native_pid", return_value=4242):
        assert manager.is_running
    with patch.object(MinicloudManager, "_native_pid", return_value=None):
        assert not manager.is_running
