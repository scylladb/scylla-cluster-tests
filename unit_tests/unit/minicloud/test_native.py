"""Native minicloud process on macOS: preflight, start, reuse and stop."""

import os
import signal
import subprocess
from unittest.mock import ANY, MagicMock, patch

import pytest

from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError
from sdcm.utils.minicloud.manager import MinicloudManager


@pytest.fixture
def native_config(tmp_path):
    """Native minicloud config with state and log files under tmp_path."""
    return MinicloudConfig(
        native=True,
        port=5050,
        lock_guest_memory=False,
        state_dir=str(tmp_path / "state"),
        log_file=str(tmp_path / "minicloud.log"),
    )


def write_pid(config, pid):
    """Write pid to the config's pid file, creating the state dir first."""
    os.makedirs(config.state_dir, exist_ok=True)
    with open(config.pid_file, "w") as fh:
        fh.write(str(pid))


def test_preflight_skips_kvm_and_docker_checks(native_config):
    """preflight_check on native skips the KVM and Docker checks when the binary and socket exist."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=True),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_preflight_fails_without_binary(native_config):
    """preflight_check fails when minicloud is not on PATH."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None),
        pytest.raises(MinicloudError, match="'minicloud' is not on PATH"),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_preflight_fails_without_tund_socket(native_config):
    """preflight_check fails when the minicloud-tund.sock socket is missing."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=False),
        pytest.raises(MinicloudError, match="minicloud-tund.sock"),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_preflight_rejects_guest_memory_locking(native_config):
    """preflight_check rejects lock_guest_memory, which the native binary cannot honour."""
    native_config.lock_guest_memory = True
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=True),
        pytest.raises(MinicloudError, match="cannot lock guest memory"),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_preflight_accepts_running_endpoint(native_config):
    """preflight_check passes without the binary when the endpoint already answers health probes."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value=None),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_start_launches_binary_and_writes_pid(native_config):
    """Start launches the native binary with the expected flags and env, and records its pid."""
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
    """Start reuses a healthy endpoint of a known native process without relaunching it."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=True),
        patch.object(MinicloudManager, "_native_pid", return_value=4242),
        patch.object(MinicloudManager, "_container_sizing_gaps", return_value=[]),
        patch.object(MinicloudManager, "_start_log_streaming") as streaming,
        patch("sdcm.utils.minicloud.manager.subprocess.Popen") as popen,
        patch("sdcm.utils.minicloud.manager.subprocess.run") as run,
    ):
        manager.start()

    popen.assert_not_called()
    run.assert_not_called()
    streaming.assert_called_once_with()
    assert os.environ["SCT_MINICLOUD_ENDPOINT_URL"] == "http://localhost:5050"


def test_stop_terminates_process_and_removes_pid_file(native_config):
    """Stop sends SIGTERM to the native process and removes the pid file."""
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

    kill.assert_called_once_with(4242, signal.SIGTERM)
    assert kill.call_args[0][0] == 4242
    run.assert_not_called()
    assert not os.path.exists(native_config.pid_file)


def test_stop_copies_native_log_into_run_dir(native_config, tmp_path):
    """Stop copies the native log into the run directory."""
    os.makedirs(native_config.state_dir)
    with open(native_config.native_log_file, "w") as fh:
        fh.write("starting\nshutting down\n")
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "_native_pid", return_value=None),
        patch("sdcm.utils.minicloud.manager.subprocess.run"),
    ):
        manager.stop()

    assert (tmp_path / "minicloud-native.log").read_text() == "starting\nshutting down\n"


def test_keep_alive_stop_ends_native_log_streamer(native_config):
    """Stop with keep_alive ends the log streamer but leaves the native process running."""
    manager = MinicloudManager(config=native_config)
    manager.keep_alive = True
    streamer = MagicMock()
    manager._container_log_process = streamer
    with patch("sdcm.utils.minicloud.manager.os.kill") as kill:
        manager.stop()

    streamer.terminate.assert_called_once_with()
    assert manager._container_log_process is None
    kill.assert_not_called()
    assert not manager._stopped


def test_is_running_reads_pid(native_config):
    """is_running is True only while a native pid is known."""
    manager = MinicloudManager(config=native_config)
    with patch.object(MinicloudManager, "_native_pid", return_value=4242):
        assert manager.is_running
    with patch.object(MinicloudManager, "_native_pid", return_value=None):
        assert not manager.is_running


def test_start_refuses_healthy_endpoint_without_known_pid(native_config):
    """Start refuses a healthy endpoint that SCT did not start, and launches nothing."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=True),
        patch.object(MinicloudManager, "_native_pid", return_value=None),
        patch("sdcm.utils.minicloud.manager.subprocess.Popen") as popen,
        pytest.raises(MinicloudError, match="SCT did not start"),
    ):
        manager.start()
    popen.assert_not_called()


def test_start_restarts_process_with_different_sizing(native_config):
    """Start stops and relaunches the native process when its sizing differs from the config."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", side_effect=[True, False]),
        patch.object(MinicloudManager, "_native_pid", return_value=4242),
        patch.object(MinicloudManager, "_container_sizing_gaps", return_value=["no --intercept-s3"]),
        patch.object(MinicloudManager, "_stop_native_process") as stop,
        patch.object(MinicloudManager, "_wait_for_health"),
        patch.object(MinicloudManager, "_start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.subprocess.Popen", return_value=MagicMock(pid=5151)) as popen,
    ):
        manager.start()
    stop.assert_called_once_with()
    popen.assert_called_once_with(
        [
            "minicloud",
            "--port",
            "5050",
            "--aws-region",
            native_config.region,
            "--s3-passthrough-buckets",
            ",".join(native_config.s3_passthrough_buckets),
            "--cross-arch=fail",
            "--gcp-project",
            native_config.gcp_project,
            "--intercept-s3",
            "--lightweight",
            "--lightweight-memory",
            "4GiB",
            "--lightweight-vcpus",
            "1",
            "--cache-dir",
            native_config.state_dir,
        ],
        stdin=subprocess.DEVNULL,
        stdout=ANY,
        stderr=subprocess.STDOUT,
        env=ANY,
        start_new_session=True,
    )
    assert popen.call_args.kwargs["stdout"].name == native_config.native_log_file
    assert popen.call_args.kwargs["env"]["AWS_REGION"] == native_config.region


def test_sizing_gaps_read_native_process_args(native_config):
    """_container_sizing_gaps reports gaps from the native process args, without inspecting a container."""
    manager = MinicloudManager(config=native_config)
    args = ["/opt/homebrew/bin/minicloud", "--port", "5050", "--cache-dir", "/x", "PATH=/usr/bin"]
    with (
        patch.object(MinicloudManager, "_native_pid", return_value=4242),
        patch.object(MinicloudManager, "_native_process_args", return_value=args),
        patch.object(MinicloudManager, "_inspect_container") as inspect,
    ):
        gaps = manager._container_sizing_gaps()
    assert "no --intercept-s3" in gaps
    assert any(gap.startswith("lightweight mode") for gap in gaps)
    inspect.assert_not_called()


def test_gce_gaps_read_native_process_env(native_config):
    """_container_gce_gaps reads the GCE credentials from the native process env."""
    manager = MinicloudManager(config=native_config)
    args = ["minicloud", "--gcs-bucket", "bucket", "GOOGLE_APPLICATION_CREDENTIALS=/key.json"]
    with (
        patch.object(MinicloudManager, "_native_pid", return_value=4242),
        patch.object(MinicloudManager, "_native_process_args", return_value=args),
    ):
        assert manager._container_gce_gaps() == []


def test_native_pid_drops_stale_file_of_other_process(native_config):
    """_native_pid drops a pid file whose pid belongs to a process that is not minicloud."""
    write_pid(native_config, os.getpid())
    manager = MinicloudManager(config=native_config)
    with patch.object(MinicloudManager, "_native_process_args", return_value=None):
        assert manager._native_pid() is None
    assert not os.path.exists(native_config.pid_file)


def test_native_pid_reaps_dead_child(native_config):
    """_native_pid reaps a dead child process and drops its pid file."""
    child = subprocess.Popen(["true"])
    os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
    write_pid(native_config, child.pid)
    manager = MinicloudManager(config=native_config)
    with patch.object(MinicloudManager, "_native_process_args", return_value=["minicloud"]):
        assert manager._native_pid() is None
    assert not os.path.exists(native_config.pid_file)


def test_native_process_args_rejects_other_binary():
    """_native_process_args returns None for a pid that is not a minicloud process."""
    assert MinicloudManager._native_process_args(os.getpid()) is None


def test_watcher_not_started_without_known_pid(native_config):
    """The native log watcher does not start without a known pid."""
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "_native_pid", return_value=None),
        patch("sdcm.utils.minicloud.manager.subprocess.Popen"),
        patch("sdcm.utils.minicloud.manager.threading.Thread") as thread,
    ):
        manager._start_log_streaming()
        manager._close_log_file()
    thread.assert_not_called()


def test_tund_socket_honours_env(native_config, monkeypatch):
    """preflight_check reports the tund socket path set by MINICLOUD_TUND_SOCKET."""
    monkeypatch.setenv("MINICLOUD_TUND_SOCKET", "/tmp/other-tund.sock")
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.shutil.which", return_value="/usr/local/bin/minicloud"),
        patch("sdcm.utils.minicloud.manager.Path.exists", return_value=False),
        pytest.raises(MinicloudError, match="/tmp/other-tund.sock"),
    ):
        manager.preflight_check(skip_aws_creds=True)


def test_start_failure_attaches_native_log_tail(native_config):
    """A failed native start attaches the tail of the native log to the error and stops the process."""
    manager = MinicloudManager(config=native_config)
    os.makedirs(native_config.state_dir, exist_ok=True)

    def popen(cmd, stdout, **kwargs):
        stdout.write("".join(f"line {i}\n" for i in range(40)) + "failed to bind port 5050\n")
        return MagicMock(pid=4242)

    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch.object(MinicloudManager, "_wait_for_health", side_effect=MinicloudError("not healthy")),
        patch.object(MinicloudManager, "_stop_native_process") as stop,
        patch("sdcm.utils.minicloud.manager.subprocess.Popen", side_effect=popen),
        pytest.raises(MinicloudError, match="not healthy") as exc_info,
    ):
        manager.start()

    note = "\n".join(exc_info.value.__notes__)
    assert native_config.native_log_file in note
    assert "failed to bind port 5050" in note
    assert "line 10\n" not in note
    assert stop.call_count == 2


def test_start_drops_sct_endpoint_variables_from_the_binary_env(native_config, monkeypatch):
    """Start does not pass the SCT endpoint variables to the native binary env."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5050")
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:5050")
    monkeypatch.setenv("AZURE_ENDPOINT_URL", "https://localhost:5443")
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5050")
    manager = MinicloudManager(config=native_config)
    with (
        patch.object(MinicloudManager, "is_endpoint_healthy", return_value=False),
        patch.object(MinicloudManager, "_wait_for_health"),
        patch.object(MinicloudManager, "_start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.subprocess.Popen", return_value=MagicMock(pid=4242)) as popen,
    ):
        manager.start()

    env = popen.call_args.kwargs["env"]
    for name in ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "AZURE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL"):
        assert name not in env
