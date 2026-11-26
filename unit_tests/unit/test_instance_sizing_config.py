# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import dataclasses
import logging
import pathlib
from pathlib import Path
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from sdcm import sct_abs_path
from sdcm.sct_config import config as sct_config
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo


@pytest.fixture(autouse=True)
def suppress_sizing_logs(caplog):
    """Raise the sdcm logger to ERROR so sizing resolution logs stay quiet."""
    """Suppress noisy sizing resolution logs during tests."""
    with caplog.at_level(logging.ERROR, logger="sdcm"):
        yield


@pytest.fixture(autouse=True)
def clear_sizing_resolution_cache():
    """Empty the sizing resolution cache before and after each test."""
    sct_config._SIZING_RESOLUTION_CACHE.clear()
    yield
    sct_config._SIZING_RESOLUTION_CACHE.clear()


MINIMAL_CONFIG = "unit_tests/test_configs/minimal_test_case.yaml"

AWS_INSTANCE = InstanceTypeInfo(
    instance_type="i8g.2xlarge",
    cloud="aws",
    family="i8g",
    vcpus=8,
    memory_gb=64.0,
    local_disk_gb=1875.0,
    local_disk_count=1,
    arch="arm64",
    price_per_hour=None,
)

GCE_INSTANCE = InstanceTypeInfo(
    instance_type="n2-standard-8",
    cloud="gce",
    family="n2",
    vcpus=8,
    memory_gb=32.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="x86_64",
    price_per_hour=None,
)

GCE_LOADER_ARM = InstanceTypeInfo(
    instance_type="n4a-standard-4",
    cloud="gce",
    family="n4a-standard",
    vcpus=4,
    memory_gb=16.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="arm64",
    price_per_hour=None,
)

AWS_LOADER_X86 = InstanceTypeInfo(
    instance_type="c6i.xlarge",
    cloud="aws",
    family="c6i",
    vcpus=4,
    memory_gb=8.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="x86_64",
    price_per_hour=None,
)

AWS_LOADER_ARM = InstanceTypeInfo(
    instance_type="c7g.xlarge",
    cloud="aws",
    family="c7g",
    vcpus=4,
    memory_gb=8.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="arm64",
    price_per_hour=None,
)

AZURE_LOADER_X86 = InstanceTypeInfo(
    instance_type="Standard_F4s_v2",
    cloud="azure",
    family="Standard_F",
    vcpus=4,
    memory_gb=8.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="x86_64",
    price_per_hour=None,
)

AZURE_LOADER_ARM = InstanceTypeInfo(
    instance_type="Standard_D4ps_v6",
    cloud="azure",
    family="Standard_D",
    vcpus=4,
    memory_gb=16.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="arm64",
    price_per_hour=None,
)


def make_catalog(*instances: InstanceTypeInfo) -> InstanceCatalog:
    """Build an InstanceCatalog holding the given instances and default family preferences."""
    cat = InstanceCatalog()
    cat.instances = list(instances)
    cat.cloud_defaults = {"aws": {"arch": "arm64"}, "gce": {"arch": "x86_64"}}
    cat.preferred_families = {
        "db": {"aws": ["i8g"], "gce": ["n2"]},
        "loader": {"aws": ["c6i"], "gce": ["n2"]},
        "monitor": {"aws": ["t3"], "gce": ["n2"]},
    }
    return cat


@pytest.fixture(autouse=True)
def silence_loggers():
    """Lower the anyconfig, botocore and boto3 logger levels."""
    logging.getLogger("anyconfig").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)


def test_dict_constraint_resolved_to_instance_type(monkeypatch):
    """A dict vcpu constraint from env resolves to a concrete instance type."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_literal_instance_type_passes_through_unchanged(monkeypatch):
    """A literal instance type from env is kept as written."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i4i.large"


def test_docker_backend_skips_resolution(monkeypatch):
    """The docker backend never loads the instance catalog."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)

    catalog_loaded = []
    real_from_dir = InstanceCatalog.from_directory

    def tracking_from_dir(path: Path) -> InstanceCatalog:
        """Record the catalog path, then load it for real."""
        catalog_loaded.append(path)
        return real_from_dir(path)

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", side_effect=tracking_from_dir):
        sct_config.SCTConfiguration()

    assert not catalog_loaded, "catalog should not be loaded for docker backend"


def test_missing_vcpu_in_constraint_raises_value_error(monkeypatch):
    """A constraint without vcpu raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)),
        pytest.raises(ValueError, match="Invalid constraint for instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_no_matching_instance_raises_value_error(monkeypatch):
    """A constraint no instance satisfies raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "9999")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)),
        pytest.raises(ValueError, match="Cannot resolve instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_env_var_dot_notation_vcpu_and_memory_resolved(monkeypatch):
    """Dot-notation vcpu and memory env vars resolve to a matching instance type."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_env_var_double_underscore_notation_vcpu_and_memory_resolved(monkeypatch):
    """SCT_INSTANCE_TYPE_DB__VCPU / __MEMORY (bash-exportable, uppercase sub-keys) resolve like the dot form."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB__VCPU", "8")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB__MEMORY", "64")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_missing_catalog_directory_logs_warning_and_skips(monkeypatch, caplog):
    """A missing catalog directory logs a warning and leaves the size unresolved."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    sct_config._SIZING_RESOLUTION_CACHE.clear()

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", side_effect=FileNotFoundError("not found")),
        caplog.at_level(logging.WARNING, logger="sdcm.sct_config"),
    ):
        conf = sct_config.SCTConfiguration()

    assert "catalog" in caplog.text.lower() or "not found" in caplog.text.lower()
    assert conf.get("instance_type_db") != "i8g.2xlarge"


def test_resolve_instance_sizes_method_direct(monkeypatch):
    """The resolve_instance_sizes method resolves a dict constraint in the given env."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)

    conf = sct_config.SCTConfiguration()

    fake_env = {
        "cluster_backend": "aws",
        "instance_type_db": {"vcpu": "8"},
    }

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)):
        conf._resolve_instance_sizes(fake_env)

    assert fake_env["instance_type_db"] == "i8g.2xlarge"


def test_aws_loader_ami_resolves_amd64_for_x86_instance_type(monkeypatch):
    """An x86 AWS loader type gets the amd64 Ubuntu SSM AMI."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c6i.xlarge")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_X86)),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == (
        "resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    )


def test_aws_loader_ami_resolves_arm64_for_arm_instance_type(monkeypatch):
    """An arm AWS loader type gets the arm64 Ubuntu SSM AMI."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c7g.xlarge")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_ARM)),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == (
        "resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
    )


def test_aws_loader_ami_not_overridden_when_explicitly_set(monkeypatch):
    """An explicit SCT_AMI_ID_LOADER wins over the resolved AMI."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c6i.xlarge")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-custom-loader")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_X86)),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == "ami-custom-loader"


def test_azure_loader_image_resolves_x86_sku_for_x86_instance_type(monkeypatch):
    """An x86 Azure loader type gets the x86 Ubuntu image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_LOADER", "Standard_F4s_v2")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AZURE_LOADER_X86)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("azure_image_loader") == "Canonical:ubuntu-26_04-lts:server:latest"


def test_azure_loader_image_resolves_arm_sku_for_arm_instance_type(monkeypatch):
    """An arm Azure loader type gets the arm64 Ubuntu image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_LOADER", "Standard_D4ps_v6")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AZURE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("azure_image_loader") == "Canonical:ubuntu-26_04-lts:server-arm64:latest"


def test_gce_loader_image_resolves_amd64_for_x86_instance_type(monkeypatch):
    """An x86 GCE loader type gets the amd64 Ubuntu image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n2-standard-8")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_gce_loader_image_resolves_arm64_for_arm_instance_type(monkeypatch):
    """An arm GCE loader type gets the arm64 Ubuntu image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-arm64")


def test_gce_loader_image_not_overridden_when_explicitly_set(monkeypatch):
    """An explicit SCT_GCE_IMAGE_LOADER wins over the resolved image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")
    monkeypatch.setenv("SCT_GCE_IMAGE_LOADER", "https://example.com/custom-loader-image")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader") == "https://example.com/custom-loader-image"


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("aws", id="aws"),
        pytest.param("aws-siren", id="aws-siren"),
        pytest.param("k8s-eks", id="k8s-eks"),
        pytest.param("k8s-local-kind-aws", id="k8s-local-kind-aws"),
    ],
)
def test_aws_family_backends_resolve_the_loader_ami_arch_marker(monkeypatch, backend):
    """AWS-family backends substitute the arch marker in the loader AMI."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c7g.xlarge")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_ARM)),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get("ami_id_loader")
    assert conf.get("ami_id_loader").endswith("/current/arm64/hvm/ebs-gp3/ami-id")


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("gce", id="gce"),
        pytest.param("gce-siren", id="gce-siren"),
        pytest.param("k8s-gke", id="k8s-gke"),
    ],
)
def test_gce_family_backends_resolve_the_loader_image_arch_marker(monkeypatch, backend):
    """GCE-family backends substitute the arch marker in the loader image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get("gce_image_loader")
    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-arm64")


def test_aws_arch_lookup_failure_falls_back_instead_of_raising(monkeypatch):
    """A failed AWS arch lookup falls back to amd64 instead of raising."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c9z.unknown")

    boto_error = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeInstanceTypes")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_ARM)),
        patch("sdcm.sct_config.config.get_arch_from_instance_type", side_effect=boto_error),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader").endswith("/current/amd64/hvm/ebs-gp3/ami-id")


@pytest.mark.parametrize(
    "provider, instance_env, instance_type, image_param, expected_suffix",
    [
        pytest.param(
            "aws",
            "SCT_INSTANCE_TYPE_LOADER",
            "c7g.xlarge",
            "ami_id_loader",
            "/current/arm64/hvm/ebs-gp3/ami-id",
            id="aws-arm64",
        ),
        pytest.param(
            "gce",
            "SCT_GCE_INSTANCE_TYPE_LOADER",
            "n4a-standard-4",
            "gce_image_loader",
            "/ubuntu-2604-lts-arm64",
            id="gce-arm64",
        ),
    ],
)
def test_xcloud_resolves_the_loader_image_arch_marker(
    monkeypatch, provider, instance_env, instance_type, image_param, expected_suffix
):
    """The xcloud backend substitutes the arch marker in the loader image of the chosen provider."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "xcloud")
    monkeypatch.setenv("SCT_XCLOUD_PROVIDER", provider)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv(instance_env, instance_type)

    catalog = make_catalog(AWS_LOADER_ARM if provider == "aws" else GCE_LOADER_ARM)
    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=catalog),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get(image_param)
    assert conf.get(image_param).endswith(expected_suffix)


def test_arm_loader_instance_with_undetectable_arch_raises(monkeypatch):
    """An arm loader type whose arch cannot be looked up raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c9g.xlarge")

    boto_error = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeInstanceTypes")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_LOADER_ARM)),
        patch("sdcm.sct_config.config.get_arch_from_instance_type", side_effect=boto_error),
        patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
        pytest.raises(ValueError, match="c9g.xlarge"),
    ):
        sct_config.SCTConfiguration()


def test_arm_loader_instance_resolved_as_x86_raises(monkeypatch):
    """An arm loader type the catalog labels x86_64 raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    mislabelled = dataclasses.replace(GCE_LOADER_ARM, arch="x86_64")
    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(mislabelled)),
        pytest.raises(ValueError, match="n4a-standard-4"),
    ):
        sct_config.SCTConfiguration()


def test_literal_loader_instance_type_wins_over_sizing_arch_hint(monkeypatch):
    """A literal loader instance type wins over a sizing arch hint."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n2-standard-8")
    monkeypatch.setenv("SCT_SIZING_LOADER__arch", "arm64")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_unknown_arch_marker_value_raises():
    """substitute_arch_markers rejects an unknown arch value."""
    with pytest.raises(ValueError, match="riscv64"):
        sct_config.substitute_arch_markers("ubuntu-2604-lts-{arch}", "riscv64")


@pytest.mark.parametrize(
    "cloud, instance_type, expected",
    [
        pytest.param("aws", "c7g.xlarge", True, id="aws-c7g-arm"),
        pytest.param("aws", "im4gn.large", True, id="aws-im4gn-arm"),
        pytest.param("aws", "a1.medium", True, id="aws-a1-arm"),
        pytest.param("aws", "g4dn.xlarge", False, id="aws-g4dn-x86"),
        pytest.param("aws", "c5.2xlarge", False, id="aws-c5-x86"),
        pytest.param("gce", "n4a-standard-4", True, id="gce-n4a-arm"),
        pytest.param("gce", "t2a-standard-4", True, id="gce-t2a-arm"),
        pytest.param("gce", "n2d-highcpu-4", False, id="gce-n2d-x86"),
        pytest.param("azure", "Standard_D4ps_v6", True, id="azure-d4ps-arm"),
        pytest.param("azure", "Standard_F4s_v2", False, id="azure-f4s-x86"),
        pytest.param("oci", "VM.Standard.A1.Flex:2", True, id="oci-a1-arm"),
        pytest.param("oci", "VM.Standard3.Flex:8:16", False, id="oci-standard3-x86"),
    ],
)
def test_arm_instance_type_detection(cloud, instance_type, expected):
    """is_arm_instance_type classifies each instance type as ARM or not."""
    assert sct_config.is_arm_instance_type(cloud, instance_type) is expected


def test_every_catalog_entry_matches_its_arm_pattern():
    """Every catalog entry arch agrees with is_arm_instance_type."""
    catalog = InstanceCatalog.from_directory(Path("data/instance_catalog"))
    for instance in catalog.instances:
        is_arm = instance.arch in ("arm64", "aarch64")
        assert sct_config.is_arm_instance_type(instance.cloud, instance.instance_type) is is_arm, (
            f"{instance.cloud} {instance.instance_type} arch={instance.arch}"
        )


@pytest.mark.parametrize(
    "stress_cmd, expected_tools",
    [
        pytest.param("cassandra-harry -mode cql3", ["harry"], id="harry"),
        pytest.param("hydra-kcl -t 10", ["kcl"], id="kcl"),
        pytest.param("ndbench -p numKeys=100", ["ndbench"], id="ndbench"),
        pytest.param("nosqlbench run driver=cql", ["nosqlbench"], id="nosqlbench"),
        pytest.param("cassandra-stress write duration=1m", [], id="cassandra-stress"),
        pytest.param("scylla-bench -workload=sequential", [], id="scylla-bench"),
        pytest.param("latte run workload.rn", [], id="latte"),
    ],
)
def test_amd64_only_stress_tools_detected_from_stress_commands(monkeypatch, stress_cmd, expected_tools):
    """Stress commands map to the AMD64-only tools they need."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", stress_cmd)
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "false")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf._amd64_only_stress_tools() == expected_tools


def test_ycsb_with_dns_routing_requires_the_amd64_only_dns_image(monkeypatch):
    """YCSB with DNS routing needs the AMD64-only alternator-dns tool."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "bin/ycsb load scylla -P workloads/workloada")
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "true")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf._amd64_only_stress_tools() == ["alternator-dns"]


def test_ycsb_upgrade_test_stress_params_with_dns_routing_requires_the_amd64_only_dns_image(monkeypatch):
    """YCSB upgrade stress params with DNS routing need the alternator-dns tool and an x86 loader."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_BEFORE_UPGRADE", "bin/ycsb load dynamodb -P workloads/workloada")
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "true")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf._amd64_only_stress_tools() == ["alternator-dns"]
    assert conf.get("sizing_loader").get("arch") == "x86_64"


def test_ycsb_without_dns_routing_keeps_arm_loaders(monkeypatch):
    """YCSB without DNS routing leaves the loader arch unconstrained."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "bin/ycsb load scylla -P workloads/workloada")
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "false")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("sizing_loader").get("arch") is None


def test_amd64_only_stress_tool_constrains_sizing_loader_to_x86(monkeypatch):
    """An AMD64-only stress tool pins the sizing loader to x86_64."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-harry -mode cql3")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("sizing_loader")["arch"] == "x86_64"
    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_explicit_arm_sizing_loader_with_an_amd64_only_stress_tool_raises(monkeypatch):
    """An explicit arm64 sizing loader with an AMD64-only stress tool raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-harry -mode cql3")
    monkeypatch.setenv("SCT_SIZING_LOADER__arch", "arm64")
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_LOADER_ARM)),
        pytest.raises(ValueError, match="linux/amd64"),
    ):
        sct_config.SCTConfiguration()


def test_literal_arm_loader_with_an_amd64_only_stress_tool_raises(monkeypatch):
    """A literal arm loader type with an AMD64-only stress tool raises ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "ndbench -p numKeys=100")
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(GCE_LOADER_ARM)),
        pytest.raises(ValueError, match="ndbench"),
    ):
        sct_config.SCTConfiguration()


def test_env_sizing_loader_constraints_survive_the_x86_constraint(monkeypatch):
    """Sizing loader vcpu and memory constraints survive the x86 pin."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "nosqlbench run driver=cql")
    monkeypatch.setenv("SCT_SIZING_LOADER__vcpu", "8")
    monkeypatch.setenv("SCT_SIZING_LOADER__memory", ">=32")

    with patch(
        "sdcm.sct_config.config.InstanceCatalog.from_directory",
        return_value=make_catalog(GCE_INSTANCE, GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    sizing_loader = conf.get("sizing_loader")
    assert sizing_loader["arch"] == "x86_64"
    assert sizing_loader["vcpu"] == "8"
    assert sizing_loader["memory"] == ">=32"
    assert conf.get("gce_instance_type_loader") == "n2-standard-8"


OCI_LOADER_ARM = InstanceTypeInfo(
    instance_type="VM.Standard.A1.Flex:4:16",
    cloud="oci",
    family="VM.Standard",
    vcpus=4,
    memory_gb=16.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="arm64",
    price_per_hour=None,
)


def test_oci_loader_image_resolves_the_arch_marker(monkeypatch):
    """OCI substitutes the arch marker in the loader image."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "oci")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_OCI_INSTANCE_TYPE_LOADER", "VM.Standard.A1.Flex:4:16")
    monkeypatch.setenv("SCT_OCI_IMAGE_LOADER", "ubuntu-24.04-{arch}")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(OCI_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("oci_image_loader") == "ubuntu-24.04-arm64"


def test_oci_loader_image_without_a_marker_is_left_alone(monkeypatch):
    """An OCI loader image without an arch marker is kept unchanged."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "oci")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_OCI_INSTANCE_TYPE_LOADER", "VM.Standard.A1.Flex:4:16")
    monkeypatch.setenv("SCT_OCI_IMAGE_LOADER", "ocid1.image.oc1.phx.aaaaaaaa")

    with patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(OCI_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("oci_image_loader") == "ocid1.image.oc1.phx.aaaaaaaa"


def test_no_file_derived_paths_in_the_sct_config_package():
    """`Path(__file__).parent...` silently resolves one level wrong inside the package.

    `sct_config.py` became `sct_config/config.py`, so any path built by walking up from `__file__`
    now lands in `sdcm/` instead of the repo root. `_resolve_instance_sizes` and `_get_loader_arch`
    both did this, and both swallow the resulting FileNotFoundError with a warning -- so the only
    symptom is sizing and loader-arch resolution quietly not working. Use `sct_abs_path()`.
    """
    package = pathlib.Path(sct_abs_path("sdcm/sct_config"))
    offenders = [
        f"{path.relative_to(package.parent.parent)}:{num}"
        for path in sorted(package.rglob("*.py"))
        for num, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if "__file__" in line and "sct_abs_path" not in line
    ]
    assert not offenders, (
        f"paths derived from __file__ inside sdcm/sct_config/: {offenders}. "
        f"Use sct_abs_path() -- it is anchored at the repo root, not at the module's location."
    )


@pytest.mark.parametrize(
    "provision, expected_spot_region",
    [
        pytest.param("spot", "eu-west-2", id="spot"),
        pytest.param("on_demand", None, id="on-demand"),
    ],
)
def test_spot_runs_rank_by_interruption_in_the_first_region(monkeypatch, provision, expected_spot_region):
    """SCT-707: only a spot run asks the matcher to rank by interruption, and it uses the first region."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_INSTANCE_PROVISION", provision)
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-2 eu-west-1")

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)),
        patch("sdcm.sct_config.config.select_instance", wraps=sct_config.select_instance) as select,
    ):
        sct_config.SCTConfiguration()

    assert select.call_args.kwargs["spot_region"] == expected_spot_region


@pytest.mark.parametrize(
    "provision, test_duration, expected_spot_region",
    [
        # short: the policy picks spot, so rank by interruption
        pytest.param("auto", "60", "eu-west-2", id="auto-short"),
        # long and unset: the policy picks on_demand, so the spot ranking is dropped
        pytest.param(None, "1440", None, id="unset-long"),
    ],
)
def test_spot_ranking_follows_the_duration_policy(monkeypatch, provision, test_duration, expected_spot_region):
    """Sizing runs before the duration policy settles `instance_provision`, so the ranking must follow it."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-2 eu-west-1")
    monkeypatch.setenv("SCT_TEST_DURATION", test_duration)
    if provision:
        monkeypatch.setenv("SCT_INSTANCE_PROVISION", provision)
    else:
        monkeypatch.delenv("SCT_INSTANCE_PROVISION", raising=False)

    with (
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)),
        patch("sdcm.sct_config.config.select_instance", wraps=sct_config.select_instance) as select,
    ):
        sct_config.SCTConfiguration()

    assert select.call_args.kwargs["spot_region"] == expected_spot_region


@pytest.mark.parametrize(
    "provision, test_duration",
    [
        # the spot decision never changes: no re-rank at all
        pytest.param("spot", None, id="spot-no-rerank"),
        # the duration policy turns it spot: re-rank, but for the region of the first sizing
        pytest.param("auto", "60", id="auto-rerank"),
    ],
)
def test_spot_ranking_keeps_the_first_region_after_a_resolved_placement(monkeypatch, provision, test_duration):
    """run-test reads the region that region fallback moved provisioning to, and must still pick the type that
    provision-resources launched, which was ranked for the configured first region.
    """
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_INSTANCE_PROVISION", provision)
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-2")  # also restores what the placement writes to os.environ
    monkeypatch.setenv("SCT_TEST_ID", "11111111-2222-3333-4444-555555555555")
    if test_duration:
        monkeypatch.setenv("SCT_TEST_DURATION", test_duration)
    placement = {"region_name": "eu-west-1", "amis": {"ami_id_db_scylla": "ami-dummy"}}

    with (
        patch("sdcm.sct_config.config.TestConfig.read_resolved_placement", return_value=placement),
        patch("sdcm.sct_config.config.InstanceCatalog.from_directory", return_value=make_catalog(AWS_INSTANCE)),
        patch("sdcm.sct_config.config.select_instance", wraps=sct_config.select_instance) as select,
    ):
        config = sct_config.SCTConfiguration()

    assert config.region_names == ["eu-west-1"]
    assert [call.kwargs["spot_region"] for call in select.call_args_list][-1] == "eu-west-2"
    assert "eu-west-1" not in [call.kwargs["spot_region"] for call in select.call_args_list]
