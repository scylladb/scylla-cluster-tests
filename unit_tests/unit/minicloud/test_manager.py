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
