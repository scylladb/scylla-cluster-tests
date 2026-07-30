import os
from unittest.mock import patch, MagicMock

import pytest
import requests

from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.utils.gce_region import GceRegion
from sdcm.utils.gce_utils import _gce_client_options
from sdcm.utils.minicloud import (
    MINICLOUD_DEFAULT_REGION,
    MINICLOUD_DOCKER_IMAGE_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
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
    # MinicloudManager.set_env_overrides() writes straight into os.environ (that is its
    # job - it configures the SCT process). monkeypatch cannot undo what it never saw, so
    # snapshot and restore the whole environment: a leaked AWS_ENDPOINT_URL makes
    # is_minicloud_active() true for every test that runs afterwards in this process,
    # which silently disables AZ/region fallback and reds the provisioner suite.
    original_env = os.environ.copy()
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("SCT_MINICLOUD_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("MINICLOUD_DOCKER", raising=False)
    # region resolution reads both of these; a developer's exported value must not
    # decide what these tests assert
    monkeypatch.delenv("MINICLOUD_AWS_REGION", raising=False)
    monkeypatch.delenv("SCT_REGION_NAME", raising=False)
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_is_minicloud_active_from_aws_endpoint_url(monkeypatch):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_sct_env(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_docker_env(monkeypatch):
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:latest")
    assert is_minicloud_active() is True


def test_is_minicloud_active_from_params():
    """A test-case yaml can only switch minicloud on through the param, not the environment."""
    assert is_minicloud_active(params={"minicloud_endpoint_url": "http://localhost:5000"}) is True
    assert get_minicloud_endpoint(params={"minicloud_endpoint_url": "http://localhost:6000"}) == "http://localhost:6000"


def test_is_minicloud_inactive_for_empty_params():
    assert is_minicloud_active(params={}) is False
    assert is_minicloud_active(params={"minicloud_endpoint_url": ""}) is False


def test_minicloud_endpoint_env_beats_params(monkeypatch):
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:7000")
    assert get_minicloud_endpoint(params={"minicloud_endpoint_url": "http://localhost:6000"}) == "http://localhost:7000"


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
    with patch("sdcm.utils.minicloud.requests.post", return_value=mock_response) as mock_post:
        assert check_minicloud_reachability("http://localhost:5000") is True
    assert mock_post.call_args.kwargs["data"]["Action"] == "DescribeVpcs"


def test_check_minicloud_reachability_400_is_unhealthy():
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "<Error><Code>UnsupportedOperation</Code></Error>"
    with patch("sdcm.utils.minicloud.requests.post", return_value=mock_response):
        with pytest.raises(RuntimeError, match="HTTP 400"):
            check_minicloud_reachability("http://localhost:5000")


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
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]
    assert config.region == MINICLOUD_DEFAULT_REGION
    assert config.docker_image == "ghcr.io/scylladb/minicloud:master-4bd3fb6"


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
    assert config.docker_image == "ghcr.io/scylladb/minicloud:master-4bd3fb6"
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]
    assert config.region == MINICLOUD_DEFAULT_REGION


def test_minicloud_lightweight_memory_default_can_start_scylla():
    """Scylla terminates below 1 GiB/shard, and the guest OS eats ~1.7 GiB of the VM.

    A default that cannot boot scylla-server is a silent trap: the image starts scylla on
    first boot, long before SCT could apply append_scylla_args or developer_mode.
    """
    gib = float(MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT.removesuffix("GiB"))
    assert gib >= 3.0, f"{MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT} leaves under 1 GiB/shard for scylla"


def test_minicloud_config_from_env_custom_docker_image(monkeypatch):
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:v2.0")
    config = MinicloudConfig.from_env()
    assert config.docker_image == "minicloud:v2.0"


def test_minicloud_config_from_env_empty_docker_image_env_does_not_win(monkeypatch):
    """An exported-but-empty MINICLOUD_DOCKER must not blank out the param/default image.

    A Jenkins job or scripts/ wrapper that passes an unset image param through as an empty
    env var would otherwise resolve to `docker run ''`.
    """
    monkeypatch.setenv("MINICLOUD_DOCKER", "")
    assert MinicloudConfig.from_env().docker_image == MINICLOUD_DOCKER_IMAGE_DEFAULT
    config = MinicloudConfig.from_env(params={"minicloud_docker_image": "minicloud:from-params"})
    assert config.docker_image == "minicloud:from-params"


def test_minicloud_config_from_env_sct_docker_image_env(monkeypatch):
    """The sct.py container commands build no SCTConfiguration, so the env form must be read.

    Without this, `sct.py start-minicloud` ignored the job's image selection and started the
    built-in default while the run's config dump reported the selected one.
    """
    monkeypatch.setenv("SCT_MINICLOUD_DOCKER_IMAGE", "minicloud:from-sct-env")
    assert MinicloudConfig.from_env().docker_image == "minicloud:from-sct-env"

    # the bare MINICLOUD_DOCKER stays the more specific override
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:from-bare-env")
    assert MinicloudConfig.from_env().docker_image == "minicloud:from-bare-env"


def test_minicloud_config_from_env_custom_port(monkeypatch):
    monkeypatch.setenv("MINICLOUD_PORT", "9000")
    config = MinicloudConfig.from_env()
    assert config.port == 9000


def test_minicloud_config_from_env_custom_region(monkeypatch):
    monkeypatch.setenv("MINICLOUD_AWS_REGION", "eu-west-1")
    config = MinicloudConfig.from_env()
    assert config.region == "eu-west-1"


def test_minicloud_config_prepares_all_supported_regions_by_default():
    """minicloud scopes resources per region, so every region a test might use must exist."""
    config = MinicloudConfig.from_env()
    assert config.regions == AWS_SUPPORTED_REGIONS
    assert "eu-west-1" in config.regions
    assert "us-east-1" in config.regions


def test_minicloud_config_env_narrows_regions(monkeypatch):
    monkeypatch.setenv("MINICLOUD_AWS_REGION", "eu-west-1,us-east-2")
    config = MinicloudConfig.from_env()
    assert config.regions == ["eu-west-1", "us-east-2"]


def test_minicloud_config_default_region_follows_the_test(monkeypatch):
    """The container's --aws-region should point at the region the test provisions in."""
    config = MinicloudConfig.from_env(params={"region_name": ["eu-west-1"]})
    assert config.region == "eu-west-1"
    assert config.regions == AWS_SUPPORTED_REGIONS

    monkeypatch.setenv("SCT_REGION_NAME", "eu-north-1")
    assert MinicloudConfig.from_env().region == "eu-north-1"


def test_minicloud_config_default_region_outside_prepared_set(monkeypatch):
    """A narrowed region set wins: never hand the container a region we did not prepare."""
    monkeypatch.setenv("MINICLOUD_AWS_REGION", "us-east-2")
    config = MinicloudConfig.from_env(params={"region_name": ["eu-west-1"]})
    assert config.regions == ["us-east-2"]
    assert config.region == "us-east-2"


def test_prepare_regions_configures_every_region(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1", "us-east-1", "eu-north-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region_cls.return_value = MagicMock()
        manager.prepare_regions()

    assert [call.kwargs["region_name"] for call in mock_region_cls.call_args_list] == [
        "eu-west-1",
        "us-east-1",
        "eu-north-1",
    ]
    assert mock_region_cls.return_value.configure.call_count == 3


def test_minicloud_config_from_env_custom_buckets(monkeypatch):
    monkeypatch.setenv("S3_PASSTHROUGH_BUCKETS", "bucket-a,bucket-b,bucket-c")
    config = MinicloudConfig.from_env()
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b", "bucket-c"]


def test_minicloud_config_from_env_custom_memory(monkeypatch):
    monkeypatch.setenv("MINICLOUD_LIGHTWEIGHT_MEMORY", "8GiB")
    config = MinicloudConfig.from_env()
    assert config.lightweight_memory == "8GiB"


def test_minicloud_config_lightweight_params_are_honoured(monkeypatch):
    """A test-case yaml (or Jenkins job) must be able to size the lightweight VMs.

    These params were declared in sct_config but never read, so every run silently got the
    hardcoded default no matter what the yaml said.
    """
    monkeypatch.delenv("MINICLOUD_LIGHTWEIGHT_MEMORY", raising=False)
    monkeypatch.delenv("S3_PASSTHROUGH_BUCKETS", raising=False)

    config = MinicloudConfig.from_env(
        params={
            "cluster_backend": "gce",
            "minicloud_lightweight": True,
            "minicloud_lightweight_memory": "6GiB",
            "minicloud_s3_passthrough_buckets": ["bucket-a", "bucket-b"],
        }
    )
    assert config.lightweight is True
    assert config.lightweight_memory == "6GiB"
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b"]


def test_minicloud_config_env_overrides_lightweight_params(monkeypatch):
    """The bare env var is the per-invocation override, so it wins over the param."""
    monkeypatch.setenv("MINICLOUD_LIGHTWEIGHT_MEMORY", "12GiB")
    config = MinicloudConfig.from_env(params={"minicloud_lightweight_memory": "6GiB"})
    assert config.lightweight_memory == "12GiB"


def test_minicloud_config_splits_comma_joined_bucket_param(monkeypatch):
    """StringOrList does not split on commas, so the yaml default arrives as one string.

    Passing it through unsplit hands minicloud a single bogus bucket name and every S3
    passthrough (keystore creds included) starts failing.
    """
    monkeypatch.delenv("S3_PASSTHROUGH_BUCKETS", raising=False)
    config = MinicloudConfig.from_env(params={"minicloud_s3_passthrough_buckets": "bucket-a,bucket-b"})
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b"]

    # and the same value wrapped in a list, which is what an explicit yaml list yields
    config = MinicloudConfig.from_env(params={"minicloud_s3_passthrough_buckets": ["bucket-a,bucket-b"]})
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b"]


def test_minicloud_config_lightweight_can_be_disabled_by_param():
    config = MinicloudConfig.from_env(params={"minicloud_lightweight": False})
    assert config.lightweight is False


def test_minicloud_config_lightweight_stays_on_when_param_absent():
    """A params mapping without the key must not read as 'lightweight off'."""
    config = MinicloudConfig.from_env(params={"cluster_backend": "aws"})
    assert config.lightweight is True


def test_minicloud_config_falls_back_to_defaults_on_empty_params():
    """An unset/blank param must not blank out the default."""
    config = MinicloudConfig.from_env(
        params={"minicloud_lightweight_memory": "", "minicloud_s3_passthrough_buckets": []}
    )
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == [
        "scylla-qa-keystore",
        "cloudius-jenkins-test",
        "downloads.scylladb.com",
    ]


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


def test_container_gce_gaps_detects_both_missing(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path)))
    result = MagicMock(returncode=0, stdout=b'["AWS_REGION=us-east-1"]')
    with patch("sdcm.utils.minicloud.subprocess.run", return_value=result):
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
        patch("sdcm.utils.minicloud.MinicloudManager._is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.MinicloudManager._get_running_image", return_value="minicloud:test"),
        patch("sdcm.utils.minicloud.MinicloudManager._container_gce_gaps", return_value=["no --gcs-bucket"]),
        patch("sdcm.utils.minicloud.MinicloudManager._setup_gcp_credentials"),
        patch("sdcm.utils.minicloud.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.MinicloudManager._wait_for_health"),
        patch("sdcm.utils.minicloud.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.subprocess.run") as mock_run,
    ):
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
        patch("sdcm.utils.minicloud.MinicloudManager._is_endpoint_healthy", return_value=True),
        patch("sdcm.utils.minicloud.MinicloudManager._get_running_image", return_value="minicloud:test"),
        patch("sdcm.utils.minicloud.MinicloudManager._container_gce_gaps", return_value=[]),
        patch("sdcm.utils.minicloud.MinicloudManager._setup_gcp_credentials"),
        patch("sdcm.utils.minicloud.MinicloudManager._setup_host_networking"),
        patch("sdcm.utils.minicloud.MinicloudManager._start_log_streaming"),
        patch("sdcm.utils.minicloud.subprocess.run") as mock_run,
    ):
        manager.start()

    assert not any(cmd[:2] == ["docker", "run"] for cmd in (c[0][0] for c in mock_run.call_args_list))


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


def test_prepare_regions_calls_configure(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()

    mock_region_cls.assert_called_once_with(region_name="eu-west-1")
    mock_region.configure.assert_called_once()


def test_prepare_regions_silences_ssm_failures(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("SSM Systems Manager parameter not found")
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()


def test_prepare_regions_silences_ssm_lowercase(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
        mock_region = MagicMock()
        mock_region.configure.side_effect = Exception("ssm parameter store unavailable")
        mock_region_cls.return_value = mock_region
        manager.prepare_regions()


def test_prepare_regions_reraises_non_ssm_exceptions(tmp_path):
    config = MinicloudConfig(regions=["eu-west-1"], state_dir=str(tmp_path))
    manager = MinicloudManager(config=config)

    with patch("sdcm.utils.minicloud.AwsRegion") as mock_region_cls:
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
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), backend="gce")
    manager = MinicloudManager(config=config)
    manager.set_env_overrides()

    assert os.environ["GCE_ENDPOINT_URL"] == "http://localhost:5000"


def test_set_env_overrides_does_not_set_gce_endpoint_url_when_backend_is_aws(tmp_path, monkeypatch):
    monkeypatch.delenv("GCE_ENDPOINT_URL", raising=False)

    config = MinicloudConfig(port=5000, state_dir=str(tmp_path), backend="aws")
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
    mock_manager.prepare_regions.assert_called_once()


# --- host memory preflight -----------------------------------------------------------------


def _meminfo_path_patch(available_kib):
    """Patch sdcm.utils.minicloud.Path so /proc/meminfo reports the given MemAvailable."""
    mock_meminfo = MagicMock()
    mock_meminfo.exists.return_value = True
    mock_meminfo.read_text.return_value = f"MemTotal: 999999999 kB\nMemAvailable: {available_kib} kB\n"

    def path_side_effect(arg):
        if str(arg) == "/proc/meminfo":
            return mock_meminfo
        from pathlib import Path as RealPath  # noqa: PLC0415

        return RealPath(arg)

    return patch("sdcm.utils.minicloud.Path", side_effect=path_side_effect)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("4GiB", 4.0), ("2.5GiB", 2.5), ("4096MiB", 4.0), ("4G", 4.0), ("1TiB", 1024.0)],
)
def test_parse_memory_gib(value, expected):
    assert MinicloudManager._parse_memory_gib(value) == pytest.approx(expected)


def test_parse_memory_gib_rejects_garbage():
    with pytest.raises(MinicloudError, match="cannot parse"):
        MinicloudManager._parse_memory_gib("lots")


@pytest.mark.parametrize(
    ("value", "expected"),
    [(3, 3), ("3 3", 6), ([3, 3], 6), (None, 0), ("", 0), ([], 0)],
)
def test_sum_node_counts(value, expected):
    assert MinicloudManager._sum_node_counts(value) == expected


def test_check_host_memory_fails_when_guests_exceed_available(tmp_path):
    # 6 db + 1 loader + 1 monitor = 8 guests x 4GiB + 2GiB headroom = 34GiB needed, 16GiB available
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": "3 3", "n_loaders": 1, "n_monitor_nodes": 1}
    with _meminfo_path_patch(16 * 1024 * 1024):
        with pytest.raises(MinicloudError, match="8 guest.*34.0GiB needed.*16.0GiB"):
            manager._check_host_memory(params)


def test_check_host_memory_passes_when_it_fits(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": 1, "n_loaders": 0, "n_monitor_nodes": 0}
    with _meminfo_path_patch(16 * 1024 * 1024):
        manager._check_host_memory(params)


def test_check_host_memory_skipped_outside_lightweight_mode(tmp_path):
    # non-lightweight sizing follows the requested instance types - no fixed per-guest figure
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=False))
    with patch("sdcm.utils.minicloud.Path") as mock_path_cls:
        manager._check_host_memory({"n_db_nodes": 100})
        mock_path_cls.assert_not_called()


def test_preflight_check_runs_memory_check_when_params_given(tmp_path):
    manager = MinicloudManager(config=MinicloudConfig(state_dir=str(tmp_path), lightweight=True))
    params = {"n_db_nodes": "3 3", "n_loaders": 1, "n_monitor_nodes": 1}

    mock_kvm = MagicMock()
    mock_kvm.exists.return_value = True
    mock_meminfo = MagicMock()
    mock_meminfo.exists.return_value = True
    mock_meminfo.read_text.return_value = "MemAvailable: 8388608 kB\n"  # 8GiB

    def path_side_effect(arg):
        if str(arg) == "/dev/kvm":
            return mock_kvm
        if str(arg) == "/proc/meminfo":
            return mock_meminfo
        from pathlib import Path as RealPath  # noqa: PLC0415

        return RealPath(arg)

    with patch("sdcm.utils.minicloud.Path", side_effect=path_side_effect):
        with patch("sdcm.utils.minicloud.shutil.which", return_value="/usr/bin/docker"):
            with pytest.raises(MinicloudError, match="not enough memory"):
                manager.preflight_check(skip_aws_creds=True, params=params)
