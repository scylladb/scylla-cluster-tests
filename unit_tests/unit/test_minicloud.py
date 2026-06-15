import os
from unittest.mock import patch, MagicMock

import pytest
import requests

from sdcm.utils.gce_region import GceRegion
from sdcm.utils.gce_utils import _gce_client_options
from sdcm.utils.minicloud import (
    MinicloudConfig,
    MinicloudError,
    MinicloudManager,
    check_minicloud_reachability,
    ensure_minicloud_ready,
    get_minicloud_endpoint,
    is_minicloud_active,
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("SCT_MINICLOUD_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("MINICLOUD_DOCKER", raising=False)


def test_is_minicloud_active_from_aws_endpoint_url(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_sct_env(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_docker_env(monkeypatch):
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:latest")
    assert is_minicloud_active() is True


def test_is_minicloud_inactive_by_default():
    assert is_minicloud_active() is False


def test_is_minicloud_inactive_for_real_aws_endpoint(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://ec2.us-east-1.amazonaws.com")
    assert is_minicloud_active() is False


def test_get_minicloud_endpoint_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9999")
    assert get_minicloud_endpoint() == "http://localhost:9999"


def test_get_minicloud_endpoint_default():
    assert get_minicloud_endpoint() == "http://localhost:5000"


def test_check_minicloud_reachability_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    with patch("sdcm.utils.minicloud.requests.post", return_value=mock_response):
        assert check_minicloud_reachability("http://localhost:5000") is True


def test_check_minicloud_reachability_400_is_ok():
    mock_response = MagicMock()
    mock_response.status_code = 400
    with patch("sdcm.utils.minicloud.requests.post", return_value=mock_response):
        assert check_minicloud_reachability("http://localhost:5000") is True


def test_check_minicloud_reachability_connection_error():
    with patch("sdcm.utils.minicloud.requests.post", side_effect=requests.ConnectionError("refused")):
        with pytest.raises(RuntimeError, match="minicloud is not reachable"):
            check_minicloud_reachability("http://localhost:5000")


def test_check_minicloud_reachability_timeout():
    with patch("sdcm.utils.minicloud.requests.post", side_effect=requests.Timeout("timed out")):
        with pytest.raises(RuntimeError, match="timed out"):
            check_minicloud_reachability("http://localhost:5000")


def test_minicloud_config_defaults():
    config = MinicloudConfig()
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == "2.5GiB"
    assert config.s3_passthrough_buckets == ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]
    assert config.region == "us-east-1"
    assert config.docker_image == "scylladb/minicloud:dev"


def test_minicloud_config_from_env_defaults(monkeypatch):
    for key in (
        "MINICLOUD_DOCKER",
        "MINICLOUD_PORT",
        "MINICLOUD_LIGHTWEIGHT_MEMORY",
        "S3_PASSTHROUGH_BUCKETS",
        "MINICLOUD_AWS_REGION",
    ):
        monkeypatch.delenv(key, raising=False)

    config = MinicloudConfig.from_env()
    assert config.docker_image == "scylladb/minicloud:dev"
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == "2.5GiB"
    assert config.s3_passthrough_buckets == ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]
    assert config.region == "us-east-1"


def test_minicloud_config_from_env_custom_docker_image(monkeypatch):
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:v2.0")
    config = MinicloudConfig.from_env()
    assert config.docker_image == "minicloud:v2.0"


def test_minicloud_config_from_env_custom_port(monkeypatch):
    monkeypatch.setenv("MINICLOUD_PORT", "9000")
    config = MinicloudConfig.from_env()
    assert config.port == 9000


def test_minicloud_config_from_env_custom_region(monkeypatch):
    monkeypatch.setenv("MINICLOUD_AWS_REGION", "eu-west-1")
    config = MinicloudConfig.from_env()
    assert config.region == "eu-west-1"


def test_minicloud_config_from_env_custom_buckets(monkeypatch):
    monkeypatch.setenv("S3_PASSTHROUGH_BUCKETS", "bucket-a,bucket-b,bucket-c")
    config = MinicloudConfig.from_env()
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b", "bucket-c"]


def test_minicloud_config_from_env_custom_memory(monkeypatch):
    monkeypatch.setenv("MINICLOUD_LIGHTWEIGHT_MEMORY", "8GiB")
    config = MinicloudConfig.from_env()
    assert config.lightweight_memory == "8GiB"


def test_minicloud_error_is_exception():
    err = MinicloudError("something went wrong")
    assert isinstance(err, Exception)
    assert "something went wrong" in str(err)


def test_minicloud_error_preserves_message():
    msg = "KVM not available on this host"
    err = MinicloudError(msg)
    assert str(err) == msg


def test_preflight_check_fails_no_kvm(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        mock_kvm = MagicMock()
        mock_kvm.exists.return_value = False

        def path_side_effect(arg):
            if str(arg) == "/dev/kvm":
                return mock_kvm
            from pathlib import Path as RealPath  # noqa: PLC0415

            return RealPath(arg)

        mock_path_cls.side_effect = path_side_effect

        with pytest.raises(MinicloudError, match="/dev/kvm"):
            manager.preflight_check()


def test_preflight_check_fails_docker_not_found(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        mock_kvm = MagicMock()
        mock_kvm.exists.return_value = True

        def path_side_effect(arg):
            if str(arg) == "/dev/kvm":
                return mock_kvm
            from pathlib import Path as RealPath  # noqa: PLC0415

            return RealPath(arg)

        mock_path_cls.side_effect = path_side_effect

        with patch("sdcm.utils.minicloud.shutil.which", return_value=None):
            with pytest.raises(MinicloudError, match="docker is not available"):
                manager.preflight_check()


def test_preflight_check_fails_bad_aws_credentials(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    bad_result = MagicMock()
    bad_result.returncode = 1

    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        mock_kvm = MagicMock()
        mock_kvm.exists.return_value = True

        def path_side_effect(arg):
            if str(arg) == "/dev/kvm":
                return mock_kvm
            from pathlib import Path as RealPath  # noqa: PLC0415

            return RealPath(arg)

        mock_path_cls.side_effect = path_side_effect

        with patch("sdcm.utils.minicloud.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.subprocess.run", return_value=bad_result):
                with pytest.raises(MinicloudError, match="AWS credentials"):
                    manager.preflight_check()


def test_preflight_check_fails_aws_cli_not_found(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        mock_kvm = MagicMock()
        mock_kvm.exists.return_value = True

        def path_side_effect(arg):
            if str(arg) == "/dev/kvm":
                return mock_kvm
            from pathlib import Path as RealPath  # noqa: PLC0415

            return RealPath(arg)

        mock_path_cls.side_effect = path_side_effect

        with patch("sdcm.utils.minicloud.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.subprocess.run", side_effect=FileNotFoundError("aws not found")):
                with pytest.raises(MinicloudError, match="AWS CLI not found"):
                    manager.preflight_check()


def test_preflight_check_skip_aws_creds(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))

    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        mock_kvm = MagicMock()
        mock_kvm.exists.return_value = True

        def path_side_effect(arg):
            if str(arg) == "/dev/kvm":
                return mock_kvm
            from pathlib import Path as RealPath  # noqa: PLC0415

            return RealPath(arg)

        mock_path_cls.side_effect = path_side_effect

        with patch("sdcm.utils.minicloud.shutil.which", return_value="/usr/bin/docker"):
            with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
                manager.preflight_check(skip_aws_creds=True)
                mock_run.assert_not_called()


def test_start_runs_docker_container(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    config = MinicloudConfig(
        docker_image="minicloud:test",
        state_dir=str(tmp_path / "state"),
        log_file=str(tmp_path / "minicloud.log"),
    )
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.MinicloudManager._is_endpoint_healthy", return_value=False):
        with patch("sdcm.utils.minicloud.MinicloudManager._wait_for_health"):
            with patch("sdcm.utils.minicloud.MinicloudManager._start_log_streaming"):
                with patch("sdcm.utils.minicloud.MinicloudManager._setup_host_networking"):
                    with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
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


def test_start_reuses_healthy_endpoint(tmp_path, monkeypatch):
    config = MinicloudConfig(state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.MinicloudManager._is_endpoint_healthy", return_value=True):
        with patch("sdcm.utils.minicloud.MinicloudManager._start_log_streaming"):
            with patch("sdcm.utils.minicloud.MinicloudManager._setup_host_networking"):
                with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
                    manager.start()

    for call in mock_run.call_args_list:
        cmd = call[0][0]
        assert "run" not in cmd or cmd[0] != "docker"


def test_start_sets_aws_endpoint_url(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), log_file=str(tmp_path / "minicloud.log"))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.MinicloudManager._is_endpoint_healthy", return_value=False):
        with patch("sdcm.utils.minicloud.MinicloudManager._wait_for_health"):
            with patch("sdcm.utils.minicloud.MinicloudManager._start_log_streaming"):
                with patch("sdcm.utils.minicloud.MinicloudManager._setup_host_networking"):
                    with patch("sdcm.utils.minicloud.subprocess.run"):
                        manager.start()

    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000"


def test_stop_calls_docker_rm_force(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
        manager.stop()

    cmds = [c[0][0] for c in mock_run.call_args_list]
    assert ["docker", "rm", "-f", "minicloud"] in cmds
    assert ["docker", "network", "disconnect", "-f", "host", "minicloud"] in cmds


def test_stop_clears_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:5000")

    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.subprocess.run"):
        manager.stop()

    assert "AWS_ENDPOINT_URL" not in os.environ
    assert "GCE_ENDPOINT_URL" not in os.environ


def test_stop_terminates_log_process(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    mock_log_proc = MagicMock()
    manager._container_log_process = mock_log_proc

    with patch("sdcm.utils.minicloud.subprocess.run"):
        manager.stop()

    mock_log_proc.terminate.assert_called_once()
    assert manager._container_log_process is None


def test_stop_is_idempotent(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
        manager.stop()
        call_count_first = mock_run.call_count
        manager.stop()
        assert mock_run.call_count == call_count_first


def test_stop_skipped_when_keep_alive(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.keep_alive = True

    with patch("sdcm.utils.minicloud.subprocess.run") as mock_run:
        manager.stop()

    mock_run.assert_not_called()


def test_set_env_overrides_sets_all_six_vars(tmp_path, monkeypatch):
    for key in (
        "AWS_ENDPOINT_URL",
        "SCT_MINICLOUD_ENDPOINT_URL",
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
    assert os.environ["SCT_IP_SSH_CONNECTIONS"] == "private"
    assert os.environ["SCT_INSTANCE_PROVISION"] == "on_demand"
    assert os.environ["SCT_ENTERPRISE_DISABLE_KMS"] == "true"
    assert os.environ["SCT_FORCE_RUN_IOTUNE"] == "false"


def test_set_env_overrides_uses_config_port(tmp_path, monkeypatch):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    config = MinicloudConfig(port=9876, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:9876"
    assert os.environ["SCT_MINICLOUD_ENDPOINT_URL"] == "http://localhost:9876"


def test_prepare_region_calls_configure(tmp_path):
    config = MinicloudConfig(region="eu-west-1", state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region_cls.return_value = mock_region
        manager.prepare_region()

    mock_region_cls.assert_called_once_with(region_name="eu-west-1")
    mock_region.configure.assert_called_once()


def test_prepare_region_silences_ssm_failures(tmp_path):
    config = MinicloudConfig(region="eu-west-1", state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("SSM Systems Manager parameter not found")
        mock_region_cls.return_value = mock_region
        manager.prepare_region()


def test_prepare_region_silences_ssm_lowercase(tmp_path):
    config = MinicloudConfig(region="eu-west-1", state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("ssm parameter store unavailable")
        mock_region_cls.return_value = mock_region
        manager.prepare_region()


def test_prepare_region_reraises_non_ssm_exceptions(tmp_path):
    config = MinicloudConfig(region="eu-west-1", state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("vpc configuration failed: subnet not found")
        mock_region_cls.return_value = mock_region
        with pytest.raises(Exception, match="vpc"):
            manager.prepare_region()


def test_is_running_true_when_container_running(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    result = MagicMock()
    result.returncode = 0
    result.stdout = "true\n"
    with patch("sdcm.utils.minicloud.subprocess.run", return_value=result):
        assert manager.is_running is True


def test_is_running_false_when_container_not_running(tmp_path):
    config = MinicloudConfig(state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    result = MagicMock()
    result.returncode = 1
    result.stdout = ""
    with patch("sdcm.utils.minicloud.subprocess.run", return_value=result):
        assert manager.is_running is False


def test_set_env_overrides_sets_gce_endpoint_url_when_backend_is_gce(tmp_path, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert os.environ["GCE_ENDPOINT_URL"] == "http://localhost:5000"


def test_set_env_overrides_does_not_set_gce_endpoint_url_when_backend_is_aws(tmp_path, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert "GCE_ENDPOINT_URL" not in os.environ


def test_set_env_overrides_does_not_set_gce_endpoint_url_when_no_backend(tmp_path, monkeypatch):
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert "GCE_ENDPOINT_URL" not in os.environ


def test_gce_client_options_returns_client_options_when_endpoint_set(monkeypatch):
    from google.api_core.client_options import ClientOptions  # noqa: PLC0415

    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:9099")
    result = _gce_client_options()

    assert "client_options" in result
    assert isinstance(result["client_options"], ClientOptions)
    assert result["client_options"].api_endpoint == "http://localhost:9099"


def test_gce_client_options_returns_empty_dict_when_no_endpoint(monkeypatch):
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)
    result = _gce_client_options()

    assert result == {}


def test_gce_region_is_minicloud_true_when_endpoint_set(monkeypatch):
    monkeypatch.setenv("GCE_ENDPOINT_URL", "http://localhost:9099")

    fake_info = {
        "project_id": "test-project",
        "type": "service_account",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "private_key_id": "key-id",
        "private_key": "",
        "client_id": "123",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    with patch("sdcm.utils.gce_region.KeyStore") as mock_keystore_cls:
        mock_keystore_cls.return_value.get_gcp_credentials.return_value = fake_info
        with patch("sdcm.utils.gce_region.service_account.Credentials.from_service_account_info"):
            with patch("sdcm.utils.gce_region.build"):
                with patch("sdcm.utils.gce_region.compute_v1.NetworksClient"):
                    with patch("sdcm.utils.gce_region.compute_v1.FirewallsClient"):
                        with patch("sdcm.utils.gce_region.compute_v1.SubnetworksClient"):
                            with patch("sdcm.utils.gce_region.compute_v1.RoutesClient"):
                                with patch("sdcm.utils.gce_region.storage.Client"):
                                    region = GceRegion("us-central1")
                                    assert region._is_minicloud is True


def test_gce_region_is_minicloud_false_when_no_endpoint(monkeypatch):
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    fake_info = {
        "project_id": "test-project",
        "type": "service_account",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "private_key_id": "key-id",
        "private_key": "",
        "client_id": "123",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    with patch("sdcm.utils.gce_region.KeyStore") as mock_keystore_cls:
        mock_keystore_cls.return_value.get_gcp_credentials.return_value = fake_info
        with patch("sdcm.utils.gce_region.service_account.Credentials.from_service_account_info"):
            with patch("sdcm.utils.gce_region.build"):
                with patch("sdcm.utils.gce_region.compute_v1.NetworksClient"):
                    with patch("sdcm.utils.gce_region.compute_v1.FirewallsClient"):
                        with patch("sdcm.utils.gce_region.compute_v1.SubnetworksClient"):
                            with patch("sdcm.utils.gce_region.compute_v1.RoutesClient"):
                                with patch("sdcm.utils.gce_region.storage.Client"):
                                    region = GceRegion("us-central1")
                                    assert region._is_minicloud is False


def test_ensure_minicloud_ready_retries_then_succeeds(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")

    call_count = {"n": 0}

    def mock_check(endpoint=None, timeout=5):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise RuntimeError("not reachable")
        return True

    with patch("sdcm.utils.minicloud.check_minicloud_reachability", side_effect=mock_check):
        with patch("sdcm.utils.minicloud.time.sleep"):
            ensure_minicloud_ready()

    assert call_count["n"] == 3
    assert os.environ.get("AWS_ENDPOINT_URL") == "http://localhost:5000"


def test_ensure_minicloud_ready_falls_through_to_auto_start(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")

    def mock_check_always_fails(endpoint=None, timeout=5):
        raise RuntimeError("not reachable")

    mock_manager = MagicMock()

    with patch("sdcm.utils.minicloud.check_minicloud_reachability", side_effect=mock_check_always_fails):
        with patch("sdcm.utils.minicloud.time.sleep"):
            with patch("sdcm.utils.minicloud.MinicloudConfig.from_env"):
                with patch("sdcm.utils.minicloud.MinicloudManager", return_value=mock_manager):
                    ensure_minicloud_ready(backend="aws")

    mock_manager.preflight_check.assert_called_once_with(skip_aws_creds=True)
    mock_manager.start.assert_called_once()
    mock_manager.prepare_region.assert_called_once()
