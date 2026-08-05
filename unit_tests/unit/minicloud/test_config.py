"""Tests for MinicloudConfig: defaults, params-only from_env resolution and MinicloudError."""

import os

import pytest

from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.utils.minicloud import (
    MINICLOUD_CONTAINER_NAME,
    MINICLOUD_DEFAULT_REGION,
    MINICLOUD_GCP_PROJECT_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT,
    MINICLOUD_STATE_DIR_DEFAULT,
    MinicloudConfig,
    MinicloudError,
    default_minicloud_image,
)

DEFAULT_BUCKETS = ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]


def test_minicloud_config_defaults():
    config = MinicloudConfig()
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == DEFAULT_BUCKETS
    assert config.region == MINICLOUD_DEFAULT_REGION
    assert config.docker_image == default_minicloud_image()
    assert config.skip_memory_check is False


def test_default_minicloud_image_comes_from_values_file():
    """The tag has one source of truth: the renovate-managed values_minicloud.yaml."""
    image = default_minicloud_image()
    assert image.startswith("ghcr.io/scylladb/minicloud:")


def test_minicloud_config_from_env_defaults():
    config = MinicloudConfig.from_env()
    assert config.docker_image == default_minicloud_image()
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == DEFAULT_BUCKETS
    assert config.region == MINICLOUD_DEFAULT_REGION
    assert config.gcs_bucket == ""
    assert config.skip_memory_check is False


def test_minicloud_config_ignores_bare_minicloud_env_vars(monkeypatch):
    """from_env() is params-only: no bare MINICLOUD_*/S3_PASSTHROUGH_BUCKETS env is read.

    Every knob is a documented ``minicloud_*`` SCT param (each with its automatic SCT_*
    env form), so a stray shell export must not silently reconfigure a run.
    """
    monkeypatch.setenv("MINICLOUD_DOCKER", "minicloud:from-env")
    monkeypatch.setenv("MINICLOUD_PORT", "9000")
    monkeypatch.setenv("MINICLOUD_LIGHTWEIGHT", "false")
    monkeypatch.setenv("MINICLOUD_LIGHTWEIGHT_MEMORY", "12GiB")
    monkeypatch.setenv("MINICLOUD_AWS_REGION", "us-east-2")
    monkeypatch.setenv("S3_PASSTHROUGH_BUCKETS", "bucket-x")

    config = MinicloudConfig.from_env()
    assert config.docker_image == default_minicloud_image()
    assert config.port == 5000
    assert config.lightweight is True
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.regions == AWS_SUPPORTED_REGIONS
    assert config.s3_passthrough_buckets == DEFAULT_BUCKETS


def test_minicloud_lightweight_memory_default_can_start_scylla():
    """Scylla terminates below 1 GiB/shard, and the guest OS eats ~1.7 GiB of the VM.

    A default that cannot boot scylla-server is a silent trap: the image starts scylla on
    first boot, long before SCT could apply append_scylla_args or developer_mode.
    """
    gib = float(MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT.removesuffix("GiB"))
    assert gib >= 3.0, f"{MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT} leaves under 1 GiB/shard for scylla"


def test_minicloud_config_stress_image_param_is_the_renovate_default():
    """The image resolves through stress_image.minicloud — the renovate-managed knob."""
    config = MinicloudConfig.from_env(params={"stress_image.minicloud": "minicloud:from-renovate"})
    assert config.docker_image == "minicloud:from-renovate"


def test_minicloud_config_docker_image_param_overrides_renovate_default():
    """minicloud_docker_image stays the more specific, per-run override."""
    config = MinicloudConfig.from_env(
        params={"minicloud_docker_image": "minicloud:v2.0", "stress_image.minicloud": "minicloud:from-renovate"}
    )
    assert config.docker_image == "minicloud:v2.0"


def test_minicloud_config_empty_image_params_do_not_blank_default():
    """An unset image param passed through as an empty string must not resolve to `docker run ''`."""
    config = MinicloudConfig.from_env(params={"minicloud_docker_image": "", "stress_image.minicloud": ""})
    assert config.docker_image == default_minicloud_image()
    config = MinicloudConfig.from_env(
        params={"minicloud_docker_image": "", "stress_image.minicloud": "minicloud:from-renovate"}
    )
    assert config.docker_image == "minicloud:from-renovate"


def test_minicloud_config_port_follows_endpoint_url(monkeypatch):
    """The port derives only from the resolved endpoint — a custom-port endpoint must
    drive the manager's port instead of being overwritten by a rebuilt localhost:5000."""
    config = MinicloudConfig.from_env(params={"minicloud_endpoint_url": "http://localhost:6000"})
    assert config.port == 6000

    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:7000")
    assert MinicloudConfig.from_env().port == 7000


def test_minicloud_config_prepares_all_supported_regions_by_default():
    """minicloud scopes resources per region, so every region a test might use must exist."""
    config = MinicloudConfig.from_env()
    assert config.regions == AWS_SUPPORTED_REGIONS
    assert "eu-west-1" in config.regions
    assert "us-east-1" in config.regions


@pytest.mark.parametrize(
    "value",
    ["eu-west-1,us-east-2", ["eu-west-1", "us-east-2"], ["eu-west-1,us-east-2"]],
    ids=["plain-string", "list", "list-of-joined-strings"],
)
def test_minicloud_config_regions_param_narrows_regions(value):
    """minicloud_regions is StringOrList — every shape it can arrive in flattens the same."""
    config = MinicloudConfig.from_env(params={"minicloud_regions": value})
    assert config.regions == ["eu-west-1", "us-east-2"]


def test_minicloud_config_default_region_follows_the_test(monkeypatch):
    """The container's --aws-region should point at the region the test provisions in."""
    config = MinicloudConfig.from_env(params={"region_name": ["eu-west-1"]})
    assert config.region == "eu-west-1"
    assert config.regions == AWS_SUPPORTED_REGIONS

    monkeypatch.setenv("SCT_REGION_NAME", "eu-north-1")
    assert MinicloudConfig.from_env().region == "eu-north-1"


def test_minicloud_config_default_region_outside_prepared_set():
    """A narrowed region set wins: never hand the container a region we did not prepare."""
    config = MinicloudConfig.from_env(params={"minicloud_regions": "us-east-2", "region_name": ["eu-west-1"]})
    assert config.regions == ["us-east-2"]
    assert config.region == "us-east-2"


@pytest.mark.parametrize(
    "value",
    ["bucket-a,bucket-b,bucket-c", ["bucket-a", "bucket-b", "bucket-c"], ["bucket-a,bucket-b", "bucket-c"]],
    ids=["plain-string", "list", "list-of-joined-strings"],
)
def test_minicloud_config_buckets_param_shapes(value):
    """minicloud_s3_passthrough_buckets is StringOrList — every shape flattens the same."""
    config = MinicloudConfig.from_env(params={"minicloud_s3_passthrough_buckets": value})
    assert config.s3_passthrough_buckets == ["bucket-a", "bucket-b", "bucket-c"]


def test_minicloud_config_lightweight_params_are_honoured():
    """A test-case yaml (or Jenkins job) must be able to size the lightweight VMs.

    These params were declared in sct_config but never read, so every run silently got the
    hardcoded default no matter what the yaml said.
    """
    config = MinicloudConfig.from_env(
        params={
            "cluster_backend": "gce",
            "minicloud_lightweight": True,
            "minicloud_lightweight_memory": "6GiB",
        }
    )
    assert config.lightweight is True
    assert config.lightweight_memory == "6GiB"


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
        params={
            "minicloud_lightweight_memory": "",
            "minicloud_s3_passthrough_buckets": "",
            "minicloud_regions": "",
        }
    )
    assert config.lightweight_memory == MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    assert config.s3_passthrough_buckets == DEFAULT_BUCKETS
    assert config.regions == AWS_SUPPORTED_REGIONS


def test_minicloud_config_gcs_bucket_param():
    config = MinicloudConfig.from_env(params={"minicloud_gcs_bucket": "sct-project-1-minicloud-staging"})
    assert config.gcs_bucket == "sct-project-1-minicloud-staging"


def test_minicloud_config_sizing_params_are_honoured():
    """The whole point of these params: local and CI pick different values from config."""
    config = MinicloudConfig.from_env(
        params={
            "minicloud_lightweight_vcpus": 4,
            "minicloud_container_memory": "48GiB",
            "minicloud_container_cpus": "12",
            "minicloud_container_name": "minicloud-ci",
            "minicloud_state_dir": "/mnt/nvme/minicloud",
        }
    )
    assert config.lightweight_vcpus == 4
    assert config.container_memory == "48GiB"
    assert config.container_cpus == "12"
    assert config.container_name == "minicloud-ci"
    assert config.state_dir == "/mnt/nvme/minicloud"


def test_minicloud_config_sizing_defaults():
    config = MinicloudConfig.from_env()
    assert config.lightweight_vcpus == MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT
    assert config.container_memory == ""  # empty means "no docker limit"
    assert config.container_cpus == ""
    assert config.container_name == MINICLOUD_CONTAINER_NAME
    assert config.state_dir == os.path.expanduser(MINICLOUD_STATE_DIR_DEFAULT)


def test_minicloud_config_empty_sizing_params_do_not_blank_defaults():
    """Pipelines pass '' for an unset job parameter — that must read as "not set"."""
    config = MinicloudConfig.from_env(
        params={
            "minicloud_lightweight_vcpus": "",
            "minicloud_container_memory": "",
            "minicloud_container_cpus": "",
            "minicloud_container_name": "",
            "minicloud_state_dir": "",
        }
    )
    assert config.lightweight_vcpus == MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT
    assert config.container_name == MINICLOUD_CONTAINER_NAME
    assert config.state_dir == os.path.expanduser(MINICLOUD_STATE_DIR_DEFAULT)


def test_minicloud_config_state_dir_drives_log_file():
    """log_file is derived from state_dir, so it has to follow the configured value."""
    config = MinicloudConfig.from_env(params={"minicloud_state_dir": "/mnt/nvme/minicloud"})
    assert config.log_file == "/mnt/nvme/minicloud/minicloud.log"


def test_minicloud_config_state_dir_expands_tilde():
    config = MinicloudConfig.from_env(params={"minicloud_state_dir": "~/scratch/minicloud"})
    assert config.state_dir == os.path.expanduser("~/scratch/minicloud")
    assert "~" not in config.log_file


def test_minicloud_config_gce_project_param_wins_over_env(monkeypatch):
    """gce_project can arrive from yaml/defaults without ever reaching os.environ.

    Ignoring the param created the real GCS staging bucket in the default project instead of
    the one the GCE clients talk to.
    """
    monkeypatch.setenv("SCT_GCE_PROJECT", "gcp-from-env")
    config = MinicloudConfig.from_env(params={"gce_project": "gcp-from-yaml"})
    assert config.gcp_project == "gcp-from-yaml"


def test_minicloud_config_gce_project_falls_back_to_env_then_default(monkeypatch):
    monkeypatch.delenv("SCT_GCE_PROJECT", raising=False)
    assert MinicloudConfig.from_env().gcp_project == MINICLOUD_GCP_PROJECT_DEFAULT
    # a param mapping that carries a blank value must not blank out the fallbacks
    assert MinicloudConfig.from_env(params={"gce_project": ""}).gcp_project == MINICLOUD_GCP_PROJECT_DEFAULT

    monkeypatch.setenv("SCT_GCE_PROJECT", "gcp-from-env")
    assert MinicloudConfig.from_env().gcp_project == "gcp-from-env"
    assert MinicloudConfig.from_env(params={"gce_project": ""}).gcp_project == "gcp-from-env"


def test_minicloud_config_skip_memory_check_param():
    assert MinicloudConfig.from_env().skip_memory_check is False
    assert MinicloudConfig.from_env(params={"minicloud_skip_memory_check": True}).skip_memory_check is True


def test_minicloud_error_is_exception():
    err = MinicloudError("something went wrong")
    assert isinstance(err, Exception)
    assert "something went wrong" in str(err)


def test_minicloud_error_preserves_message():
    msg = "KVM not available on this host"
    err = MinicloudError(msg)
    assert str(err) == msg
