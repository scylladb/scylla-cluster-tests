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

import pytest

from sdcm.utils.cloud_sizes import (
    SIZING,
    InstanceSpec,
    SizeMapping,
    get_cloud_params,
    identify_size,
    resolve_size,
)


# ---------------------------------------------------------------------------
# resolve_size — forward mappings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("large", "i8g.large", id="large"),
        pytest.param("xlarge", "i8g.xlarge", id="xlarge"),
        pytest.param("2xlarge", "i8g.2xlarge", id="2xlarge"),
        pytest.param("4xlarge", "i8g.4xlarge", id="4xlarge"),
        pytest.param("8xlarge", "i8g.8xlarge", id="8xlarge"),
        pytest.param("16xlarge", "i8g.16xlarge", id="16xlarge"),
    ],
)
def test_resolve_size_db_aws_arm(size, expected_type):
    spec = resolve_size("db", size, "aws", "arm")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("large", "i4i.large", id="large"),
        pytest.param("xlarge", "i4i.xlarge", id="xlarge"),
        pytest.param("2xlarge", "i4i.2xlarge", id="2xlarge"),
        pytest.param("4xlarge", "i4i.4xlarge", id="4xlarge"),
        pytest.param("8xlarge", "i4i.8xlarge", id="8xlarge"),
        pytest.param("16xlarge", "i4i.16xlarge", id="16xlarge"),
    ],
)
def test_resolve_size_db_aws_x86(size, expected_type):
    spec = resolve_size("db", size, "aws", "x86")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type,ssd_count,disk_type",
    [
        pytest.param("large", "z3-highmem-4", 4, "pd-ssd", id="large"),
        pytest.param("xlarge", "z3-highmem-8", 4, "pd-ssd", id="xlarge"),
        pytest.param("2xlarge", "z3-highmem-16", 4, "pd-ssd", id="2xlarge"),
        pytest.param("4xlarge", "z3-highmem-32", 4, "pd-ssd", id="4xlarge"),
        pytest.param("8xlarge", "z3-highmem-48", 4, "pd-ssd", id="8xlarge"),
        pytest.param("16xlarge", "z3-highmem-88", 4, "pd-ssd", id="16xlarge"),
    ],
)
def test_resolve_size_db_gce(size, expected_type, ssd_count, disk_type):
    spec = resolve_size("db", size, "gce")
    assert spec.instance_type == expected_type
    assert spec.local_ssd_count == ssd_count
    assert spec.root_disk_type == disk_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("large", "Standard_L4s_v4", id="large"),
        pytest.param("xlarge", "Standard_L8s_v4", id="xlarge"),
        pytest.param("2xlarge", "Standard_L16s_v4", id="2xlarge"),
        pytest.param("4xlarge", "Standard_L32s_v4", id="4xlarge"),
        pytest.param("8xlarge", "Standard_L64s_v4", id="8xlarge"),
        pytest.param("16xlarge", "Standard_L80s_v4", id="16xlarge"),
    ],
)
def test_resolve_size_db_azure(size, expected_type):
    spec = resolve_size("db", size, "azure")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("large", "DenseIO.E5.Flex:2:32", id="large"),
        pytest.param("xlarge", "DenseIO.E5.Flex:4:64", id="xlarge"),
        pytest.param("2xlarge", "DenseIO.E5.Flex:8:128", id="2xlarge"),
        pytest.param("4xlarge", "DenseIO.E5.Flex:16:256", id="4xlarge"),
        pytest.param("8xlarge", "DenseIO.E5.Flex:32:512", id="8xlarge"),
        pytest.param("16xlarge", "DenseIO.E5.Flex:64:1024", id="16xlarge"),
    ],
)
def test_resolve_size_db_oci(size, expected_type):
    spec = resolve_size("db", size, "oci")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "c6i.xlarge", id="small"),
        pytest.param("medium", "c6i.2xlarge", id="medium"),
        pytest.param("large", "c6i.4xlarge", id="large"),
        pytest.param("2xlarge", "c6i.8xlarge", id="2xlarge"),
        pytest.param("xlarge", "c6i.16xlarge", id="xlarge"),
    ],
)
def test_resolve_size_loader_aws_arm(size, expected_type):
    spec = resolve_size("loader", size, "aws", "arm")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "c6i.xlarge", id="small"),
        pytest.param("medium", "c6i.2xlarge", id="medium"),
        pytest.param("large", "c6i.4xlarge", id="large"),
        pytest.param("2xlarge", "c6i.8xlarge", id="2xlarge"),
        pytest.param("xlarge", "c6i.16xlarge", id="xlarge"),
    ],
)
def test_resolve_size_loader_aws_x86(size, expected_type):
    spec = resolve_size("loader", size, "aws", "x86")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "e2-standard-4", id="small"),
        pytest.param("medium", "e2-standard-8", id="medium"),
        pytest.param("large", "e2-standard-16", id="large"),
        pytest.param("2xlarge", "e2-standard-32", id="2xlarge"),
        pytest.param("xlarge", "e2-highcpu-32", id="xlarge"),
    ],
)
def test_resolve_size_loader_gce(size, expected_type):
    spec = resolve_size("loader", size, "gce")
    assert spec.instance_type == expected_type
    assert spec.local_ssd_count == 0
    assert spec.root_disk_type == ""


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "Standard_F4s_v2", id="small"),
        pytest.param("medium", "Standard_F8s_v2", id="medium"),
        pytest.param("large", "Standard_F16s_v2", id="large"),
        pytest.param("2xlarge", "Standard_F32s_v2", id="2xlarge"),
        pytest.param("xlarge", "Standard_F48s_v2", id="xlarge"),
    ],
)
def test_resolve_size_loader_azure(size, expected_type):
    spec = resolve_size("loader", size, "azure")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "VM.Standard3.Flex:4:32", id="small"),
        pytest.param("medium", "VM.Standard3.Flex:8:64", id="medium"),
        pytest.param("large", "VM.Standard3.Flex:16:128", id="large"),
        pytest.param("2xlarge", "VM.Standard3.Flex:32:256", id="2xlarge"),
        pytest.param("xlarge", "VM.Standard3.Flex:64:512", id="xlarge"),
    ],
)
def test_resolve_size_loader_oci(size, expected_type):
    spec = resolve_size("loader", size, "oci")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "t3.large", id="small"),
        pytest.param("medium", "m6i.xlarge", id="medium"),
        pytest.param("large", "m6i.2xlarge", id="large"),
    ],
)
def test_resolve_size_monitor_aws(size, expected_type):
    spec_arm = resolve_size("monitor", size, "aws", "arm")
    spec_x86 = resolve_size("monitor", size, "aws", "x86")
    assert spec_arm.instance_type == expected_type
    assert spec_x86.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "n2-highmem-4", id="small"),
        pytest.param("medium", "n2-highmem-8", id="medium"),
        pytest.param("large", "n2-highmem-16", id="large"),
    ],
)
def test_resolve_size_monitor_gce(size, expected_type):
    spec = resolve_size("monitor", size, "gce")
    assert spec.instance_type == expected_type
    assert spec.local_ssd_count == 0


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "Standard_D2_v4", id="small"),
        pytest.param("medium", "Standard_D4_v4", id="medium"),
        pytest.param("large", "Standard_D8_v4", id="large"),
    ],
)
def test_resolve_size_monitor_azure(size, expected_type):
    spec = resolve_size("monitor", size, "azure")
    assert spec.instance_type == expected_type


@pytest.mark.parametrize(
    "size,expected_type",
    [
        pytest.param("small", "VM.Standard.E4.Flex:2:16", id="small"),
        pytest.param("medium", "VM.Standard.E4.Flex:4:32", id="medium"),
        pytest.param("large", "VM.Standard.E4.Flex:8:64", id="large"),
    ],
)
def test_resolve_size_monitor_oci(size, expected_type):
    spec = resolve_size("monitor", size, "oci")
    assert spec.instance_type == expected_type


# ---------------------------------------------------------------------------
# resolve_size — error cases
# ---------------------------------------------------------------------------


def test_resolve_size_unknown_role_raises():
    with pytest.raises(ValueError, match="Unknown role"):
        resolve_size("unknown_role", "large", "aws")


def test_resolve_size_unknown_size_raises():
    with pytest.raises(ValueError, match="Unknown size"):
        resolve_size("db", "nonexistent", "aws")


def test_resolve_size_unknown_cloud_raises():
    with pytest.raises(ValueError, match="Unknown cloud"):
        resolve_size("db", "large", "openstack")


def test_resolve_size_unknown_arch_raises():
    with pytest.raises(ValueError, match="Unknown arch"):
        resolve_size("db", "large", "aws", "arm64")


# ---------------------------------------------------------------------------
# identify_size — reverse lookups
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cloud,instance_type,expected",
    [
        pytest.param("aws", "i8g.large", ("db", "large"), id="aws-arm-large"),
        pytest.param("aws", "i8g.2xlarge", ("db", "2xlarge"), id="aws-arm-2xlarge"),
        pytest.param("aws", "i4i.2xlarge", ("db", "2xlarge"), id="aws-x86-2xlarge"),
        pytest.param("aws", "i4i.16xlarge", ("db", "16xlarge"), id="aws-x86-16xlarge"),
        pytest.param("gce", "z3-highmem-16", ("db", "2xlarge"), id="gce-db-2xlarge"),
        pytest.param("gce", "z3-highmem-4", ("db", "large"), id="gce-db-large"),
        pytest.param("azure", "Standard_L16s_v4", ("db", "2xlarge"), id="azure-db-2xlarge"),
        pytest.param("oci", "DenseIO.E5.Flex:8:128", ("db", "2xlarge"), id="oci-db-2xlarge"),
        pytest.param("aws", "c6i.xlarge", ("loader", "small"), id="aws-loader-small"),
        pytest.param("aws", "c6i.8xlarge", ("loader", "2xlarge"), id="aws-loader-2xlarge"),
        pytest.param("aws", "c6i.16xlarge", ("loader", "xlarge"), id="aws-loader-xlarge"),
        pytest.param("gce", "e2-standard-8", ("loader", "medium"), id="gce-loader-medium"),
        pytest.param("gce", "e2-highcpu-32", ("loader", "xlarge"), id="gce-loader-xlarge"),
        pytest.param("azure", "Standard_F4s_v2", ("loader", "small"), id="azure-loader-small"),
        pytest.param("oci", "VM.Standard3.Flex:16:128", ("loader", "large"), id="oci-loader-large"),
        pytest.param("aws", "t3.large", ("monitor", "small"), id="aws-monitor-small"),
        pytest.param("aws", "m6i.2xlarge", ("monitor", "large"), id="aws-monitor-large"),
        pytest.param("gce", "n2-highmem-8", ("monitor", "medium"), id="gce-monitor-medium"),
        pytest.param("azure", "Standard_D8_v4", ("monitor", "large"), id="azure-monitor-large"),
        pytest.param("oci", "VM.Standard.E4.Flex:4:32", ("monitor", "medium"), id="oci-monitor-medium"),
    ],
)
def test_identify_size_known_types(cloud, instance_type, expected):
    assert identify_size(cloud, instance_type) == expected


@pytest.mark.parametrize(
    "cloud,instance_type",
    [
        pytest.param("aws", "x99.nonexistent", id="aws-unknown"),
        pytest.param("gce", "n1-standard-4", id="gce-unknown"),
        pytest.param("azure", "Standard_B2ms", id="azure-unknown"),
        pytest.param("oci", "VM.Standard2.1", id="oci-unknown"),
    ],
)
def test_identify_size_unknown_returns_none(cloud, instance_type):
    assert identify_size(cloud, instance_type) is None


def test_identify_size_unknown_cloud_raises():
    with pytest.raises(ValueError, match="Unknown cloud"):
        identify_size("openstack", "m1.xlarge")


# ---------------------------------------------------------------------------
# get_cloud_params — param name generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "role,size,cloud,arch,expected",
    [
        pytest.param(
            "db",
            "2xlarge",
            "aws",
            "arm",
            {"instance_type_db": "i8g.2xlarge"},
            id="aws-arm-db",
        ),
        pytest.param(
            "db",
            "2xlarge",
            "aws",
            "x86",
            {"instance_type_db": "i4i.2xlarge"},
            id="aws-x86-db",
        ),
        pytest.param(
            "db",
            "2xlarge",
            "gce",
            "arm",
            {"gce_instance_type_db": "z3-highmem-16", "gce_n_local_ssd_disk_db": 4, "gce_root_disk_type_db": "pd-ssd"},
            id="gce-db",
        ),
        pytest.param(
            "db",
            "2xlarge",
            "azure",
            "arm",
            {"azure_instance_type_db": "Standard_L16s_v4"},
            id="azure-db",
        ),
        pytest.param(
            "db",
            "2xlarge",
            "oci",
            "arm",
            {"oci_instance_type_db": "DenseIO.E5.Flex:8:128"},
            id="oci-db",
        ),
        pytest.param(
            "loader",
            "medium",
            "aws",
            "x86",
            {"instance_type_loader": "c6i.2xlarge"},
            id="aws-loader",
        ),
        pytest.param(
            "loader",
            "medium",
            "gce",
            "arm",
            {"gce_instance_type_loader": "e2-standard-8"},
            id="gce-loader-no-ssd",
        ),
        pytest.param(
            "loader",
            "medium",
            "azure",
            "arm",
            {"azure_instance_type_loader": "Standard_F8s_v2"},
            id="azure-loader",
        ),
        pytest.param(
            "loader",
            "medium",
            "oci",
            "arm",
            {"oci_instance_type_loader": "VM.Standard3.Flex:8:64"},
            id="oci-loader",
        ),
        pytest.param(
            "monitor",
            "medium",
            "aws",
            "arm",
            {"instance_type_monitor": "m6i.xlarge"},
            id="aws-monitor",
        ),
        pytest.param(
            "monitor",
            "medium",
            "gce",
            "arm",
            {"gce_instance_type_monitor": "n2-highmem-8"},
            id="gce-monitor",
        ),
        pytest.param(
            "monitor",
            "medium",
            "azure",
            "arm",
            {"azure_instance_type_monitor": "Standard_D4_v4"},
            id="azure-monitor",
        ),
        pytest.param(
            "monitor",
            "medium",
            "oci",
            "arm",
            {"oci_instance_type_monitor": "VM.Standard.E4.Flex:4:32"},
            id="oci-monitor",
        ),
    ],
)
def test_get_cloud_params(role, size, cloud, arch, expected):
    spec = resolve_size(role, size, cloud, arch)
    params = get_cloud_params(role, spec, cloud)
    assert params == expected


def test_get_cloud_params_gce_no_ssd_omits_disk_params():
    spec = resolve_size("monitor", "medium", "gce")
    params = get_cloud_params("monitor", spec, "gce")
    assert "gce_n_local_ssd_disk_monitor" not in params
    assert "gce_root_disk_type_monitor" not in params


def test_get_cloud_params_unknown_cloud_raises():
    spec = InstanceSpec("some.type", 4, 16.0)
    with pytest.raises(ValueError, match="Unknown cloud"):
        get_cloud_params("db", spec, "openstack")


# ---------------------------------------------------------------------------
# SIZING registry completeness
# ---------------------------------------------------------------------------


def test_sizing_has_all_db_sizes():
    db_sizes = set(SIZING["db"])
    assert db_sizes == {"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "16xlarge"}


def test_sizing_has_all_loader_sizes():
    loader_sizes = set(SIZING["loader"])
    assert loader_sizes == {"small", "medium", "large", "2xlarge", "xlarge"}


def test_sizing_has_all_monitor_sizes():
    monitor_sizes = set(SIZING["monitor"])
    assert monitor_sizes == {"small", "medium", "large"}


def test_sizing_all_mappings_are_size_mapping_instances():
    for role, sizes in SIZING.items():
        for size, mapping in sizes.items():
            assert isinstance(mapping, SizeMapping), f"SIZING[{role!r}][{size!r}] is not a SizeMapping"


def test_sizing_all_specs_have_non_empty_instance_type():
    for role, sizes in SIZING.items():
        for size, mapping in sizes.items():
            for attr in ("aws_arm", "aws_x86", "gce", "azure", "oci"):
                spec = getattr(mapping, attr)
                assert spec.instance_type, f"SIZING[{role!r}][{size!r}].{attr}.instance_type is empty"
                assert isinstance(spec, InstanceSpec), f"SIZING[{role!r}][{size!r}].{attr} is not an InstanceSpec"


# ---------------------------------------------------------------------------
# db_oracle role — resolves through the "db" sizing table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "size,cloud,arch,expected_type",
    [
        pytest.param("2xlarge", "aws", "arm", "i8g.2xlarge", id="aws-arm-2xlarge"),
        pytest.param("2xlarge", "aws", "x86", "i4i.2xlarge", id="aws-x86-2xlarge"),
        pytest.param("2xlarge", "gce", "arm", "z3-highmem-16", id="gce-2xlarge"),
        pytest.param("2xlarge", "azure", "arm", "Standard_L16s_v4", id="azure-2xlarge"),
        pytest.param("2xlarge", "oci", "arm", "DenseIO.E5.Flex:8:128", id="oci-2xlarge"),
        pytest.param("large", "aws", "arm", "i8g.large", id="aws-arm-large"),
    ],
)
def test_resolve_size_db_oracle(size, cloud, arch, expected_type):
    spec = resolve_size("db_oracle", size, cloud, arch)
    assert spec.instance_type == expected_type


# ---------------------------------------------------------------------------
# zero_token role — resolves through the "db" sizing table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "size,cloud,arch,expected_type",
    [
        pytest.param("large", "aws", "arm", "i8g.large", id="aws-arm-large"),
        pytest.param("large", "aws", "x86", "i4i.large", id="aws-x86-large"),
        pytest.param("large", "gce", "arm", "z3-highmem-4", id="gce-large"),
        pytest.param("large", "azure", "arm", "Standard_L4s_v4", id="azure-large"),
        pytest.param("large", "oci", "arm", "DenseIO.E5.Flex:2:32", id="oci-large"),
        pytest.param("2xlarge", "aws", "arm", "i8g.2xlarge", id="aws-arm-2xlarge"),
    ],
)
def test_resolve_size_zero_token(size, cloud, arch, expected_type):
    spec = resolve_size("zero_token", size, cloud, arch)
    assert spec.instance_type == expected_type


def test_resolve_size_alias_unknown_role_raises():
    with pytest.raises(ValueError, match="Unknown role"):
        resolve_size("not_a_role", "large", "aws")


# ---------------------------------------------------------------------------
# get_cloud_params — db_oracle and zero_token param name generation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "role,size,cloud,arch,expected",
    [
        pytest.param(
            "db_oracle",
            "2xlarge",
            "aws",
            "arm",
            {"instance_type_db_oracle": "i8g.2xlarge"},
            id="aws-arm-db_oracle",
        ),
        pytest.param(
            "db_oracle",
            "2xlarge",
            "aws",
            "x86",
            {"instance_type_db_oracle": "i4i.2xlarge"},
            id="aws-x86-db_oracle",
        ),
        pytest.param(
            "db_oracle",
            "2xlarge",
            "gce",
            "arm",
            {"gce_instance_type_db_oracle": "z3-highmem-16"},
            id="gce-db_oracle-no-disk-params",
        ),
        pytest.param(
            "db_oracle",
            "2xlarge",
            "azure",
            "arm",
            {"azure_instance_type_db_oracle": "Standard_L16s_v4"},
            id="azure-db_oracle",
        ),
        pytest.param(
            "db_oracle",
            "2xlarge",
            "oci",
            "arm",
            {"oci_instance_type_db_oracle": "DenseIO.E5.Flex:8:128"},
            id="oci-db_oracle",
        ),
        pytest.param(
            "zero_token",
            "large",
            "aws",
            "arm",
            {"zero_token_instance_type_db": "i8g.large"},
            id="aws-arm-zero_token",
        ),
        pytest.param(
            "zero_token",
            "large",
            "aws",
            "x86",
            {"zero_token_instance_type_db": "i4i.large"},
            id="aws-x86-zero_token",
        ),
        pytest.param(
            "zero_token",
            "large",
            "gce",
            "arm",
            {"gce_zero_token_instance_type_db": "z3-highmem-4"},
            id="gce-zero_token-no-disk-params",
        ),
        pytest.param(
            "zero_token",
            "large",
            "azure",
            "arm",
            {"azure_zero_token_instance_type_db": "Standard_L4s_v4"},
            id="azure-zero_token",
        ),
        pytest.param(
            "zero_token",
            "large",
            "oci",
            "arm",
            {"oci_zero_token_instance_type_db": "DenseIO.E5.Flex:2:32"},
            id="oci-zero_token",
        ),
    ],
)
def test_get_cloud_params_oracle_and_zero_token(role, size, cloud, arch, expected):
    spec = resolve_size(role, size, cloud, arch)
    params = get_cloud_params(role, spec, cloud)
    assert params == expected


def test_get_cloud_params_gce_oracle_no_disk_params():
    spec = resolve_size("db_oracle", "2xlarge", "gce")
    params = get_cloud_params("db_oracle", spec, "gce")
    assert "gce_n_local_ssd_disk_db_oracle" not in params
    assert "gce_root_disk_type_db_oracle" not in params


def test_get_cloud_params_gce_zero_token_no_disk_params():
    spec = resolve_size("zero_token", "large", "gce")
    params = get_cloud_params("zero_token", spec, "gce")
    assert "gce_n_local_ssd_disk_zero_token" not in params
    assert "gce_root_disk_type_zero_token" not in params
