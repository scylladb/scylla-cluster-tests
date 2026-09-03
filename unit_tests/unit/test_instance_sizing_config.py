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
from pathlib import Path
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from sdcm import sct_config
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo


@pytest.fixture(autouse=True)
def _suppress_sizing_logs(caplog):
    """Suppress noisy sizing resolution logs during tests."""
    with caplog.at_level(logging.ERROR, logger="sdcm"):
        yield


@pytest.fixture(autouse=True)
def _clear_sizing_resolution_cache():
    sct_config._SIZING_RESOLUTION_CACHE.clear()
    yield
    sct_config._SIZING_RESOLUTION_CACHE.clear()


_MINIMAL_CONFIG = "unit_tests/test_configs/minimal_test_case.yaml"

_AWS_INSTANCE = InstanceTypeInfo(
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

_GCE_INSTANCE = InstanceTypeInfo(
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

_GCE_LOADER_ARM = InstanceTypeInfo(
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

_AWS_LOADER_X86 = InstanceTypeInfo(
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

_AWS_LOADER_ARM = InstanceTypeInfo(
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

_AZURE_LOADER_X86 = InstanceTypeInfo(
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

_AZURE_LOADER_ARM = InstanceTypeInfo(
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


def _make_catalog(*instances: InstanceTypeInfo) -> InstanceCatalog:
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
    logging.getLogger("anyconfig").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)


def test_dict_constraint_resolved_to_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_literal_instance_type_passes_through_unchanged(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i4i.large"


def test_docker_backend_skips_resolution(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)

    catalog_loaded = []
    real_from_dir = InstanceCatalog.from_directory

    def tracking_from_dir(path: Path) -> InstanceCatalog:
        catalog_loaded.append(path)
        return real_from_dir(path)

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", side_effect=tracking_from_dir):
        sct_config.SCTConfiguration()

    assert not catalog_loaded, "catalog should not be loaded for docker backend"


def test_missing_vcpu_in_constraint_raises_value_error(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)),
        pytest.raises(ValueError, match="Invalid constraint for instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_no_matching_instance_raises_value_error(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "9999")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)),
        pytest.raises(ValueError, match="Cannot resolve instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_env_var_dot_notation_vcpu_and_memory_resolved(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_env_var_double_underscore_notation_vcpu_and_memory_resolved(monkeypatch):
    """SCT_INSTANCE_TYPE_DB__VCPU / __MEMORY (bash-exportable, uppercase sub-keys) resolve like the dot form."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB__VCPU", "8")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB__MEMORY", "64")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_missing_catalog_directory_logs_warning_and_skips(monkeypatch, caplog):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    sct_config._SIZING_RESOLUTION_CACHE.clear()

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", side_effect=FileNotFoundError("not found")),
        caplog.at_level(logging.WARNING, logger="sdcm.sct_config"),
    ):
        conf = sct_config.SCTConfiguration()

    assert "catalog" in caplog.text.lower() or "not found" in caplog.text.lower()
    assert conf.get("instance_type_db") != "i8g.2xlarge"


def test_resolve_instance_sizes_method_direct(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)

    conf = sct_config.SCTConfiguration()

    fake_env = {
        "cluster_backend": "aws",
        "instance_type_db": {"vcpu": "8"},
    }

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf._resolve_instance_sizes(fake_env)

    assert fake_env["instance_type_db"] == "i8g.2xlarge"


def test_aws_loader_ami_resolves_amd64_for_x86_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c6i.xlarge")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_X86)),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == (
        "resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    )


def test_aws_loader_ami_resolves_arm64_for_arm_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c7g.xlarge")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_ARM)),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == (
        "resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
    )


def test_aws_loader_ami_not_overridden_when_explicitly_set(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c6i.xlarge")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-custom-loader")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_X86)),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader") == "ami-custom-loader"


def test_azure_loader_image_resolves_x86_sku_for_x86_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_LOADER", "Standard_F4s_v2")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AZURE_LOADER_X86)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("azure_image_loader") == "Canonical:ubuntu-26_04-lts:server:latest"


def test_azure_loader_image_resolves_arm_sku_for_arm_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_LOADER", "Standard_D4ps_v6")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AZURE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("azure_image_loader") == "Canonical:ubuntu-26_04-lts:server-arm64:latest"


def test_gce_loader_image_resolves_amd64_for_x86_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n2-standard-8")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_gce_loader_image_resolves_arm64_for_arm_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-arm64")


def test_gce_loader_image_not_overridden_when_explicitly_set(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")
    monkeypatch.setenv("SCT_GCE_IMAGE_LOADER", "https://example.com/custom-loader-image")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader") == "https://example.com/custom-loader-image"


@pytest.mark.parametrize("backend", ["aws", "aws-siren", "k8s-eks", "k8s-local-kind-aws"])
def test_aws_family_backends_resolve_the_loader_ami_arch_marker(monkeypatch, backend):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c7g.xlarge")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_ARM)),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get("ami_id_loader")
    assert conf.get("ami_id_loader").endswith("/current/arm64/hvm/ebs-gp3/ami-id")


@pytest.mark.parametrize("backend", ["gce", "gce-siren", "k8s-gke"])
def test_gce_family_backends_resolve_the_loader_image_arch_marker(monkeypatch, backend):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get("gce_image_loader")
    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-arm64")


def test_aws_arch_lookup_failure_falls_back_instead_of_raising(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c9z.unknown")

    boto_error = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeInstanceTypes")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_ARM)),
        patch("sdcm.sct_config.get_arch_from_instance_type", side_effect=boto_error),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("ami_id_loader").endswith("/current/amd64/hvm/ebs-gp3/ami-id")


@pytest.mark.parametrize(
    "provider, instance_env, instance_type, image_param, expected_suffix",
    [
        ("aws", "SCT_INSTANCE_TYPE_LOADER", "c7g.xlarge", "ami_id_loader", "/current/arm64/hvm/ebs-gp3/ami-id"),
        ("gce", "SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4", "gce_image_loader", "/ubuntu-2604-lts-arm64"),
    ],
)
def test_xcloud_resolves_the_loader_image_arch_marker(
    monkeypatch, provider, instance_env, instance_type, image_param, expected_suffix
):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "xcloud")
    monkeypatch.setenv("SCT_XCLOUD_PROVIDER", provider)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv(instance_env, instance_type)

    catalog = _make_catalog(_AWS_LOADER_ARM if provider == "aws" else _GCE_LOADER_ARM)
    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=catalog),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
    ):
        conf = sct_config.SCTConfiguration()

    assert "{arch}" not in conf.get(image_param)
    assert conf.get(image_param).endswith(expected_suffix)


def test_arm_loader_instance_with_undetectable_arch_raises(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_LOADER", "c9g.xlarge")

    boto_error = ClientError({"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeInstanceTypes")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_LOADER_ARM)),
        patch("sdcm.sct_config.get_arch_from_instance_type", side_effect=boto_error),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda value, regions: value),
        pytest.raises(ValueError, match="c9g.xlarge"),
    ):
        sct_config.SCTConfiguration()


def test_arm_loader_instance_resolved_as_x86_raises(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    mislabelled = dataclasses.replace(_GCE_LOADER_ARM, arch="x86_64")
    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(mislabelled)),
        pytest.raises(ValueError, match="n4a-standard-4"),
    ):
        sct_config.SCTConfiguration()


def test_literal_loader_instance_type_wins_over_sizing_arch_hint(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n2-standard-8")
    monkeypatch.setenv("SCT_SIZING_LOADER__arch", "arm64")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_unknown_arch_marker_value_raises():
    with pytest.raises(ValueError, match="riscv64"):
        sct_config.substitute_arch_markers("ubuntu-2604-lts-{arch}", "riscv64")


@pytest.mark.parametrize(
    "cloud, instance_type, expected",
    [
        ("aws", "c7g.xlarge", True),
        ("aws", "im4gn.large", True),
        ("aws", "a1.medium", True),
        ("aws", "g4dn.xlarge", False),
        ("aws", "c5.2xlarge", False),
        ("gce", "n4a-standard-4", True),
        ("gce", "t2a-standard-4", True),
        ("gce", "n2d-highcpu-4", False),
        ("azure", "Standard_D4ps_v6", True),
        ("azure", "Standard_F4s_v2", False),
        ("oci", "VM.Standard.A1.Flex:2", True),
        ("oci", "VM.Standard3.Flex:8:16", False),
    ],
)
def test_arm_instance_type_detection(cloud, instance_type, expected):
    assert sct_config.is_arm_instance_type(cloud, instance_type) is expected


def test_every_catalog_entry_matches_its_arm_pattern():
    catalog = InstanceCatalog.from_directory(Path("data/instance_catalog"))
    for instance in catalog.instances:
        is_arm = instance.arch in ("arm64", "aarch64")
        assert sct_config.is_arm_instance_type(instance.cloud, instance.instance_type) is is_arm, (
            f"{instance.cloud} {instance.instance_type} arch={instance.arch}"
        )


@pytest.mark.parametrize(
    "stress_cmd, expected_tools",
    [
        ("cassandra-harry -mode cql3", ["harry"]),
        ("hydra-kcl -t 10", ["kcl"]),
        ("ndbench -p numKeys=100", ["ndbench"]),
        ("nosqlbench run driver=cql", ["nosqlbench"]),
        ("cassandra-stress write duration=1m", []),
        ("scylla-bench -workload=sequential", []),
        ("latte run workload.rn", []),
    ],
)
def test_amd64_only_stress_tools_detected_from_stress_commands(monkeypatch, stress_cmd, expected_tools):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", stress_cmd)
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "false")

    with patch(
        "sdcm.sct_config.InstanceCatalog.from_directory",
        return_value=_make_catalog(_GCE_INSTANCE, _GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf._amd64_only_stress_tools() == expected_tools


def test_ycsb_with_dns_routing_requires_the_amd64_only_dns_image(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "bin/ycsb load scylla -P workloads/workloada")
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "true")

    with patch(
        "sdcm.sct_config.InstanceCatalog.from_directory",
        return_value=_make_catalog(_GCE_INSTANCE, _GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf._amd64_only_stress_tools() == ["alternator-dns"]


def test_ycsb_without_dns_routing_keeps_arm_loaders(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "bin/ycsb load scylla -P workloads/workloada")
    monkeypatch.setenv("SCT_ALTERNATOR_USE_DNS_ROUTING", "false")

    with patch(
        "sdcm.sct_config.InstanceCatalog.from_directory",
        return_value=_make_catalog(_GCE_INSTANCE, _GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("sizing_loader").get("arch") is None


def test_amd64_only_stress_tool_constrains_sizing_loader_to_x86(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-harry -mode cql3")

    with patch(
        "sdcm.sct_config.InstanceCatalog.from_directory",
        return_value=_make_catalog(_GCE_INSTANCE, _GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    assert conf.get("sizing_loader")["arch"] == "x86_64"
    assert conf.get("gce_image_loader").endswith("/ubuntu-2604-lts-amd64")


def test_explicit_arm_sizing_loader_with_an_amd64_only_stress_tool_raises(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-harry -mode cql3")
    monkeypatch.setenv("SCT_SIZING_LOADER__arch", "arm64")
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_LOADER_ARM)),
        pytest.raises(ValueError, match="linux/amd64"),
    ):
        sct_config.SCTConfiguration()


def test_literal_arm_loader_with_an_amd64_only_stress_tool_raises(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "ndbench -p numKeys=100")
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_LOADER", "n4a-standard-4")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_GCE_LOADER_ARM)),
        pytest.raises(ValueError, match="ndbench"),
    ):
        sct_config.SCTConfiguration()


def test_env_sizing_loader_constraints_survive_the_x86_constraint(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_STRESS_CMD", "nosqlbench run driver=cql")
    monkeypatch.setenv("SCT_SIZING_LOADER__vcpu", "8")
    monkeypatch.setenv("SCT_SIZING_LOADER__memory", ">=32")

    with patch(
        "sdcm.sct_config.InstanceCatalog.from_directory",
        return_value=_make_catalog(_GCE_INSTANCE, _GCE_LOADER_ARM),
    ):
        conf = sct_config.SCTConfiguration()

    sizing_loader = conf.get("sizing_loader")
    assert sizing_loader["arch"] == "x86_64"
    assert sizing_loader["vcpu"] == "8"
    assert sizing_loader["memory"] == ">=32"
    assert conf.get("gce_instance_type_loader") == "n2-standard-8"


_OCI_LOADER_ARM = InstanceTypeInfo(
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
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "oci")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_OCI_INSTANCE_TYPE_LOADER", "VM.Standard.A1.Flex:4:16")
    monkeypatch.setenv("SCT_OCI_IMAGE_LOADER", "ubuntu-24.04-{arch}")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_OCI_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("oci_image_loader") == "ubuntu-24.04-arm64"


def test_oci_loader_image_without_a_marker_is_left_alone(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "oci")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_OCI_INSTANCE_TYPE_LOADER", "VM.Standard.A1.Flex:4:16")
    monkeypatch.setenv("SCT_OCI_IMAGE_LOADER", "ocid1.image.oc1.phx.aaaaaaaa")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_OCI_LOADER_ARM)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("oci_image_loader") == "ocid1.image.oc1.phx.aaaaaaaa"
