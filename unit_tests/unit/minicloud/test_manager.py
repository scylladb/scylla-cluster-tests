"""Tests for MinicloudManager lifecycle: start/stop/reuse, env overrides, region prep,
gce-gap detection and the container death watch."""

import os
from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.minicloud import MinicloudConfig, MinicloudManager


def test_start_runs_docker_container(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    config = MinicloudConfig(
        docker_image="minicloud:test",
        state_dir=str(tmp_path / "state"),
        log_file=str(tmp_path / "minicloud.log"),
    )
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=False):
        with patch("sdcm.utils.minicloud.manager.MinicloudManager._wait_for_health"):
            with patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"):
                with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stdout="cid123\n")
                    with patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"):
                        manager.start()

    run_calls = mock_run.call_args_list
    docker_run_call = run_calls[2]
    cmd = docker_run_call[0][0]
    assert cmd[0] == "docker"
    assert cmd[1] == "run"
    assert "-d" in cmd
    assert "--name" in cmd
    assert "minicloud:test" in cmd
    assert "--port" in cmd
    assert "5000" in cmd


def _started_docker_cmd(config) -> list[str]:
    """Run start() with every side effect stubbed and return the `docker run` argv."""
    manager = MinicloudManager(config=config)
    with (
        patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=False),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._wait_for_health"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="cid123\n")
        manager.start()
    return mock_run.call_args_list[2][0][0]


def test_start_passes_lightweight_vcpus(tmp_path):
    """Guests get one shard per vCPU; the value a run used must come from its config."""
    cmd = _started_docker_cmd(
        MinicloudConfig(
            state_dir=str(tmp_path),
            log_file=str(tmp_path / "minicloud.log"),
            lightweight=True,
            lightweight_memory="6GiB",
            lightweight_vcpus=2,
        )
    )
    assert "--lightweight" in cmd
    assert "--cross-arch=fail" in cmd
    assert cmd[cmd.index("--lightweight-memory") + 1] == "6GiB"
    assert cmd[cmd.index("--lightweight-vcpus") + 1] == "2"


def test_start_locks_guest_memory_by_default(tmp_path):
    """minicloud locks guest RAM, and QEMU can only mlockall() with a memlock rlimit and IPC_LOCK."""
    cmd = _started_docker_cmd(MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log")))
    assert cmd[cmd.index("--ulimit") + 1] == "memlock=-1:-1"
    assert "IPC_LOCK" in cmd
    assert "--lock-guest-memory" in cmd
    assert "--memory-swap" in cmd


@pytest.mark.parametrize("container_memory", ["", "32GiB"])
def test_start_without_guest_locking_runs_container_as_before(tmp_path, container_memory):
    """Off means untouched: no locking flags, no swap limit, and --memory only for a configured cap."""
    cmd = _started_docker_cmd(
        MinicloudConfig(
            state_dir=str(tmp_path),
            log_file=str(tmp_path / "minicloud.log"),
            container_memory=container_memory,
            lock_guest_memory=False,
        )
    )
    for flag in ("--ulimit", "IPC_LOCK", "--memory-swap", "--lock-guest-memory"):
        assert flag not in cmd
    assert ("--memory" in cmd) == bool(container_memory)


def test_start_sizes_container_to_host_memory_by_default(tmp_path):
    """Unset caps keep the container bounded only by the host, but still without swap:
    docker needs --memory alongside --memory-swap, so the host's total RAM stands in."""
    with patch.object(MinicloudManager, "_container_memory_gib", return_value=62.5):
        cmd = _started_docker_cmd(MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log")))
    assert cmd[cmd.index("--memory") + 1] == "62.5g"
    assert cmd[cmd.index("--memory-swap") + 1] == "62.5g"
    assert "--cpus" not in cmd


def test_start_omits_memory_flags_without_meminfo(tmp_path):
    """A host with no /proc/meminfo has nothing to size against: no memory flags at all."""
    with patch.object(MinicloudManager, "_container_memory_gib", return_value=0.0):
        cmd = _started_docker_cmd(MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log")))
    assert "--memory" not in cmd
    assert "--memory-swap" not in cmd


def test_container_memory_gib_falls_back_to_host_total(tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal:       65536000 kB\nMemAvailable:   1024 kB\n")
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    with patch("sdcm.utils.minicloud.manager.Path", return_value=meminfo):
        assert manager._container_memory_gib() == 65536000 / 1024**2


def test_container_memory_gib_unset_without_guest_locking(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lock_guest_memory=False))
    assert manager._container_memory_gib() == 0.0


def test_start_applies_container_limits(tmp_path):
    """docker --memory speaks b/k/m/g, so the GiB config form has to be converted."""
    cmd = _started_docker_cmd(
        MinicloudConfig(
            state_dir=str(tmp_path),
            log_file=str(tmp_path / "minicloud.log"),
            container_memory="32GiB",
            container_cpus="7.5",
        )
    )
    assert cmd[cmd.index("--memory") + 1] == "32g"
    assert cmd[cmd.index("--memory-swap") + 1] == "32g"
    assert cmd[cmd.index("--cpus") + 1] == "7.5"


def test_start_uses_configured_container_name(tmp_path):
    """Two emulators on one host need distinct names, or the second removes the first."""
    cmd = _started_docker_cmd(
        MinicloudConfig(
            state_dir=str(tmp_path),
            log_file=str(tmp_path / "minicloud.log"),
            container_name="minicloud-second",
        )
    )
    assert cmd[cmd.index("--name") + 1] == "minicloud-second"


def test_start_reuses_healthy_endpoint(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=True):
        with patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"):
            with patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"):
                with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
                    manager.start()

    for call in mock_run.call_args_list:
        cmd = call[0][0]
        assert "run" not in cmd or cmd[0] != "docker"


def test_container_gce_gaps_detects_both_missing(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    result = MagicMock(returncode=0, stdout=b'["AWS_REGION=us-east-1"]')
    with patch("sdcm.utils.minicloud.manager.subprocess.run", return_value=result):
        assert manager._container_gce_gaps() == ["no GOOGLE_APPLICATION_CREDENTIALS", "no --gcs-bucket"]


def test_container_gce_gaps_detects_missing_bucket(tmp_path):
    """Credentials present but no --gcs-bucket: image downloads would 500."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    with patch.object(
        MinicloudManager,
        "_inspect_container",
        side_effect=[["GOOGLE_APPLICATION_CREDENTIALS=/etc/minicloud/gcs-key.json"], ["--port", "5000"]],
    ):
        assert manager._container_gce_gaps() == ["no --gcs-bucket"]


def test_container_gce_gaps_none_when_fully_configured(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    with patch.object(
        MinicloudManager,
        "_inspect_container",
        side_effect=[
            ["AWS_REGION=us-east-1", "GOOGLE_APPLICATION_CREDENTIALS=/etc/minicloud/gcs-key.json"],
            ["--port", "5000", "--gcs-bucket", "sct-project-1-minicloud-staging"],
        ],
    ):
        assert manager._container_gce_gaps() == []


def test_start_restarts_gce_container_with_gaps(tmp_path, monkeypatch):
    """A healthy container missing GCP credentials or --gcs-bucket is unusable for gce."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    config = MinicloudConfig(
        backend="gce", docker_image="minicloud:test", state_dir=str(tmp_path), log_file=str(tmp_path / "mc.log")
    )
    manager = MinicloudManager(config=config)

    with (
        patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._get_running_image", return_value="minicloud:test"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._container_gce_gaps", return_value=["no --gcs-bucket"]),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_gcp_credentials"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._wait_for_health"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="cid123\n")
        manager.start()

    assert any(cmd[:2] == ["docker", "run"] for cmd in (c[0][0] for c in mock_run.call_args_list)), (
        "expected the unusable container to be replaced by a fresh 'docker run'"
    )


def test_start_reuses_fully_configured_gce_container(tmp_path, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    config = MinicloudConfig(
        backend="gce", docker_image="minicloud:test", state_dir=str(tmp_path), log_file=str(tmp_path / "mc.log")
    )
    manager = MinicloudManager(config=config)

    with (
        patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._get_running_image", return_value="minicloud:test"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._container_gce_gaps", return_value=[]),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_gcp_credentials"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run,
    ):
        manager.start()

    assert not any(cmd[:2] == ["docker", "run"] for cmd in (c[0][0] for c in mock_run.call_args_list))


RUNNING_DEFAULT_SIZING_CMD = [
    "--port",
    "5000",
    "--lightweight",
    "--lightweight-memory",
    "4GiB",
    "--lightweight-vcpus",
    "1",
    "--lock-guest-memory",
]


def _inspect_stub(cmd=None, memory=0, nano_cpus=0, memory_swap=None):
    """Stand in for `docker inspect` on the fields the sizing comparison reads."""

    def _inspect(go_template):
        if "Config.Cmd" in go_template:
            return cmd
        if "HostConfig.MemorySwap" in go_template:
            return memory if memory_swap is None else memory_swap
        if "HostConfig.Memory" in go_template:
            return memory
        if "HostConfig.NanoCpus" in go_template:
            return nano_cpus
        return None

    return _inspect


HOST_MEMORY_GIB = 62.5


def _host_memory_stub():
    return patch.object(MinicloudManager, "_container_memory_gib", return_value=HOST_MEMORY_GIB)


def test_container_sizing_gaps_none_when_unchanged(tmp_path):
    """An unset cap sizes to host RAM; reading that back must not restart the container every run."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    with (
        _host_memory_stub(),
        patch.object(
            MinicloudManager,
            "_inspect_container",
            side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD, memory=int(HOST_MEMORY_GIB * 1024**3)),
        ),
    ):
        assert manager._container_sizing_gaps() == []


@pytest.mark.parametrize("running_locked", [True, False])
def test_container_sizing_gaps_detects_changed_locking_mode(tmp_path, running_locked):
    """The locking flags are fixed at docker run, so switching modes must restart the container."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lock_guest_memory=not running_locked))
    running = RUNNING_DEFAULT_SIZING_CMD if running_locked else RUNNING_DEFAULT_SIZING_CMD[:-1]
    with (
        _host_memory_stub(),
        patch.object(
            MinicloudManager,
            "_inspect_container",
            side_effect=_inspect_stub(running, memory=int(HOST_MEMORY_GIB * 1024**3)),
        ),
    ):
        assert f"--lock-guest-memory is {running_locked}, this run wants {not running_locked}" in (
            manager._container_sizing_gaps()
        )


def test_container_sizing_gaps_detects_swap_allowed(tmp_path):
    """A container started before --memory-swap was passed can still swap: restart it once."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    with (
        _host_memory_stub(),
        patch.object(
            MinicloudManager,
            "_inspect_container",
            side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD, memory=int(HOST_MEMORY_GIB * 1024**3), memory_swap=0),
        ),
    ):
        assert manager._container_sizing_gaps() == ["--memory-swap is 0, this run wants 62.5"]


def test_container_sizing_gaps_detects_changed_guest_sizing(tmp_path):
    """A rerun with new guest sizing must not silently inherit the old container's guests."""
    manager = MinicloudManager(
        config=MinicloudConfig(state_dir=str(tmp_path), lightweight_memory="8GiB", lightweight_vcpus=2)
    )
    with (
        _host_memory_stub(),
        patch.object(
            MinicloudManager,
            "_inspect_container",
            side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD, memory=int(HOST_MEMORY_GIB * 1024**3)),
        ),
    ):
        assert manager._container_sizing_gaps() == [
            "--lightweight-memory is 4GiB, this run wants 8GiB",
            "--lightweight-vcpus is 1, this run wants 2",
        ]


def test_container_sizing_gaps_detects_changed_docker_caps(tmp_path):
    """The cgroup caps are what check_host_memory sizes against, so a stale one makes it lie."""
    manager = MinicloudManager(
        config=MinicloudConfig(state_dir=str(tmp_path), container_memory="48GiB", container_cpus="4")
    )
    with patch.object(MinicloudManager, "_inspect_container", side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD)):
        assert manager._container_sizing_gaps() == [
            "--memory is 0, this run wants 48",
            "--cpus is 0, this run wants 4",
            "--memory-swap is 0, this run wants 48",
        ]


def test_container_sizing_gaps_ignores_unchanged_docker_caps(tmp_path):
    manager = MinicloudManager(
        config=MinicloudConfig(state_dir=str(tmp_path), container_memory="48GiB", container_cpus="4")
    )
    with patch.object(
        MinicloudManager,
        "_inspect_container",
        side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD, memory=48 * 1024**3, nano_cpus=4 * 10**9),
    ):
        assert manager._container_sizing_gaps() == []


def test_container_sizing_gaps_empty_when_not_inspectable(tmp_path):
    """No evidence of a mismatch is not a reason to kill a working emulator and its VMs."""
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight_memory="8GiB"))
    with patch.object(MinicloudManager, "_inspect_container", side_effect=_inspect_stub(cmd=None)):
        assert manager._container_sizing_gaps() == []


def test_start_restarts_container_with_stale_sizing(tmp_path):
    config = MinicloudConfig(
        docker_image="minicloud:test",
        state_dir=str(tmp_path),
        log_file=str(tmp_path / "mc.log"),
        lightweight_memory="8GiB",
    )
    manager = MinicloudManager(config=config)

    with (
        patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._get_running_image", return_value="minicloud:test"),
        patch.object(MinicloudManager, "_inspect_container", side_effect=_inspect_stub(RUNNING_DEFAULT_SIZING_CMD)),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_gcp_credentials"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._wait_for_health"),
        patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="cid123\n")
        manager.start()

    started = [cmd for cmd in (call[0][0] for call in mock_run.call_args_list) if cmd[:2] == ["docker", "run"]]
    assert started, "expected the differently-sized container to be replaced by a fresh 'docker run'"
    assert started[0][started[0].index("--lightweight-memory") + 1] == "8GiB"


def test_start_sets_aws_endpoint_url(tmp_path):
    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.MinicloudManager.is_endpoint_healthy", return_value=False):
        with patch("sdcm.utils.minicloud.manager.MinicloudManager._wait_for_health"):
            with patch("sdcm.utils.minicloud.manager.MinicloudManager._start_log_streaming"):
                with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stdout="cid123\n")
                    with patch("sdcm.utils.minicloud.manager.MinicloudManager._setup_host_networking"):
                        manager.start()

    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000"


def test_stop_calls_docker_rm_force(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
        manager.stop()

    cmds = [c[0][0] for c in mock_run.call_args_list]
    assert ["docker", "rm", "-f", "minicloud"] in cmds
    assert ["docker", "network", "disconnect", "-f", "host", "minicloud"] in cmds


def test_stop_clears_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:5000")

    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.subprocess.run"):
        manager.stop()

    assert "AWS_ENDPOINT_URL" not in os.environ
    assert "GCE_ENDPOINT_URL" not in os.environ


def test_stop_terminates_log_process(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    mock_log_proc = MagicMock()
    manager._container_log_process = mock_log_proc

    with patch("sdcm.utils.minicloud.manager.subprocess.run"):
        manager.stop()

    mock_log_proc.terminate.assert_called_once()
    assert manager._container_log_process is None


def test_stop_is_idempotent(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
        manager.stop()
        call_count_first = mock_run.call_count
        manager.stop()
        assert mock_run.call_count == call_count_first


def test_stop_skipped_when_keep_alive(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.keep_alive = True

    with patch("sdcm.utils.minicloud.manager.subprocess.run") as mock_run:
        manager.stop()

    mock_run.assert_not_called()


def test_set_env_overrides_sets_endpoint_vars_only(tmp_path, monkeypatch):
    """Endpoint vars only — param delivery belongs to configurations/minicloud.yaml.

    SCT_* param exports here would run after SCTConfiguration is built and never reach
    params, so exporting them is a trap; validate_minicloud_params() enforces the overlay.
    """
    for key in (
        "SCT_IP_SSH_CONNECTIONS",
        "SCT_INSTANCE_PROVISION",
        "SCT_ENTERPRISE_DISABLE_KMS",
        "SCT_FORCE_RUN_IOTUNE",
    ):
        monkeypatch.delenv(key, raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000"
    assert os.environ["SCT_MINICLOUD_ENDPOINT_URL"] == "http://localhost:5000"
    for dead_key in (
        "SCT_IP_SSH_CONNECTIONS",
        "SCT_INSTANCE_PROVISION",
        "SCT_ENTERPRISE_DISABLE_KMS",
        "SCT_FORCE_RUN_IOTUNE",
    ):
        assert dead_key not in os.environ


def test_set_env_overrides_uses_config_port(tmp_path):
    config = MinicloudConfig(port=9876, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:9876"
    assert os.environ["SCT_MINICLOUD_ENDPOINT_URL"] == "http://localhost:9876"


def test_set_env_overrides_sets_gce_endpoint_url_when_backend_is_gce(tmp_path):
    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), backend="gce")
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert os.environ["GCE_ENDPOINT_URL"] == "http://localhost:5000"


def test_set_env_overrides_does_not_set_gce_endpoint_url_when_backend_is_aws(tmp_path):
    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), backend="aws")
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert "GCE_ENDPOINT_URL" not in os.environ


def test_set_env_overrides_does_not_set_gce_endpoint_url_when_no_backend(tmp_path, monkeypatch):
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert "GCE_ENDPOINT_URL" not in os.environ


def test_prepare_regions_configures_every_region(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1", "us-east-1", "eu-north-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.AwsRegion") as mock_region_cls:
        mock_region_cls.return_value = MagicMock()
        manager.prepare_regions()

    assert [call.kwargs["region_name"] for call in mock_region_cls.call_args_list] == [
        "eu-west-1",
        "us-east-1",
        "eu-north-1",
    ]
    assert mock_region_cls.return_value.configure.call_count == 3


def test_prepare_regions_calls_configure(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()

    mock_region_cls.assert_called_once_with(region_name="eu-west-1")
    mock_region.configure.assert_called_once()


def test_prepare_regions_silences_ssm_failures(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("SSM Systems Manager parameter not found")
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()


def test_prepare_regions_silences_ssm_lowercase(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("ssm parameter store unavailable")
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()


def test_prepare_regions_reraises_non_ssm_exceptions(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.manager.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("vpc configuration failed: subnet not found")
        mock_region_cls.return_value = mock_region
        with pytest.raises(Exception, match="vpc"):
            manager.prepare_regions()


def test_is_running_true_when_container_running(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    result = MagicMock()
    result.returncode = 0
    result.stdout = "true\n"
    with patch("sdcm.utils.minicloud.manager.subprocess.run", return_value=result):
        assert manager.is_running is True


def test_is_running_false_when_container_not_running(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    result = MagicMock()
    result.returncode = 1
    result.stdout = ""
    with patch("sdcm.utils.minicloud.manager.subprocess.run", return_value=result):
        assert manager.is_running is False


def test_death_watch_reports_even_with_keep_alive(tmp_path):
    """keep_alive controls teardown, not death reporting — CI (which always sets it) is
    exactly where a mid-test container death must still produce the event."""
    config = MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)
    manager.keep_alive = True
    manager._container_id = "cid123"

    log_process = MagicMock()
    log_process.wait.return_value = 0
    with (
        patch.object(MinicloudManager, "_snapshot_container_state", return_value={"ExitCode": 137}) as mock_snapshot,
        patch("sdcm.utils.minicloud.manager.TestFrameworkEvent") as mock_event,
    ):
        manager._watch_container_death(log_process)

    mock_snapshot.assert_called_once()
    mock_event.assert_called_once()


def test_death_watch_silent_when_we_stopped_it(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)
    manager._stopping = True

    log_process = MagicMock()
    with patch("sdcm.utils.minicloud.manager.TestFrameworkEvent") as mock_event:
        manager._watch_container_death(log_process)
    mock_event.assert_not_called()
