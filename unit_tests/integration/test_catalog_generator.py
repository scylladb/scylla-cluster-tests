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

import os

import pytest

from sdcm.utils.cloud_catalog.catalog_generator import (
    generate_aws_catalog,
    generate_azure_catalog,
    generate_gce_catalog,
    generate_oci_catalog,
)
from sdcm.utils.cloud_catalog.instance_catalog import InstanceTypeInfo


@pytest.fixture()
def _check_aws_credentials():
    try:
        import boto3  # noqa: PLC0415

        boto3.client("sts", region_name="us-east-1").get_caller_identity()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"AWS credentials not available: {exc}")


@pytest.fixture()
def _check_gce_credentials():
    try:
        from google.cloud import compute_v1  # noqa: PLC0415, F401
        import google.auth  # noqa: PLC0415

        _, project = google.auth.default()
        if not project:
            pytest.skip("GCE default project not set")
    except ImportError:
        pytest.skip("google-cloud-compute not installed")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"GCE credentials not available: {exc}")


@pytest.fixture()
def _check_azure_credentials():
    if not os.environ.get("AZURE_SUBSCRIPTION_ID"):
        pytest.skip("AZURE_SUBSCRIPTION_ID not set")
    try:
        from azure.identity import DefaultAzureCredential  # noqa: PLC0415

        DefaultAzureCredential().get_token("https://management.azure.com/.default")
    except ImportError:
        pytest.skip("azure-identity not installed")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Azure credentials not available: {exc}")


@pytest.fixture()
def _check_oci_credentials():
    oci_config = os.path.expanduser("~/.oci/config")
    if not os.path.exists(oci_config) and not os.environ.get("OCI_CONFIG_FILE"):
        pytest.skip("OCI credentials not configured (~/.oci/config or OCI_CONFIG_FILE)")
    try:
        import oci  # noqa: PLC0415

        oci.config.from_file()
    except ImportError:
        pytest.skip("oci SDK not installed")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"OCI credentials invalid: {exc}")


# --- AWS tests ---


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_returns_instances():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert len(result) > 0, "Expected at least one i8g instance"


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_cloud_field():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.cloud == "aws" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_family_field():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.family == "i8g" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_vcpus_positive():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.vcpus > 0 for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_arch_arm64():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.arch == "arm64" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_memory_positive():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.memory_gb > 0 for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_i8g_local_disk_present():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(r.local_disk_gb > 0 for r in result), "i8g instances should have local NVMe"


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_multiple_families():
    result = generate_aws_catalog(["i8g", "i7i"], region="us-east-1")
    families = {r.family for r in result}
    assert "i8g" in families
    assert "i7i" in families


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_returns_instance_type_info_objects():
    result = generate_aws_catalog(["i8g"], region="us-east-1")
    assert all(isinstance(r, InstanceTypeInfo) for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_aws_credentials")
def test_generate_aws_empty_family_returns_empty():
    result = generate_aws_catalog(["nonexistent99z"], region="us-east-1")
    assert result == []


# --- GCE tests ---


@pytest.mark.integration
@pytest.mark.usefixtures("_check_gce_credentials")
def test_generate_gce_n2_returns_instances():
    result = generate_gce_catalog(["n2"], zone="us-east1-b")
    assert len(result) > 0, "Expected at least one n2 machine type"


@pytest.mark.integration
@pytest.mark.usefixtures("_check_gce_credentials")
def test_generate_gce_cloud_field():
    result = generate_gce_catalog(["n2"], zone="us-east1-b")
    assert all(r.cloud == "gce" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_gce_credentials")
def test_generate_gce_family_field():
    result = generate_gce_catalog(["n2"], zone="us-east1-b")
    assert all(r.family == "n2" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_gce_credentials")
def test_generate_gce_vcpus_positive():
    result = generate_gce_catalog(["n2"], zone="us-east1-b")
    assert all(r.vcpus > 0 for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_gce_credentials")
def test_generate_gce_returns_instance_type_info_objects():
    result = generate_gce_catalog(["n2"], zone="us-east1-b")
    assert all(isinstance(r, InstanceTypeInfo) for r in result)


# --- Azure tests ---


@pytest.mark.integration
@pytest.mark.usefixtures("_check_azure_credentials")
def test_generate_azure_standard_l_returns_instances():
    result = generate_azure_catalog(["Standard_L"], region="eastus")
    assert len(result) > 0, "Expected at least one Standard_L VM size"


@pytest.mark.integration
@pytest.mark.usefixtures("_check_azure_credentials")
def test_generate_azure_cloud_field():
    result = generate_azure_catalog(["Standard_L"], region="eastus")
    assert all(r.cloud == "azure" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_azure_credentials")
def test_generate_azure_family_field():
    result = generate_azure_catalog(["Standard_L"], region="eastus")
    assert all(r.family == "Standard_L" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_azure_credentials")
def test_generate_azure_price_present():
    result = generate_azure_catalog(["Standard_L"], region="eastus")
    assert any(r.price_per_hour is not None for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_azure_credentials")
def test_generate_azure_returns_instance_type_info_objects():
    result = generate_azure_catalog(["Standard_L"], region="eastus")
    assert all(isinstance(r, InstanceTypeInfo) for r in result)


# --- OCI tests ---


@pytest.mark.integration
@pytest.mark.usefixtures("_check_oci_credentials")
def test_generate_oci_dense_io_returns_instances():
    result = generate_oci_catalog(["BM.DenseIO", "VM.DenseIO"])
    assert len(result) > 0


@pytest.mark.integration
@pytest.mark.usefixtures("_check_oci_credentials")
def test_generate_oci_cloud_field():
    result = generate_oci_catalog(["BM.DenseIO"])
    assert all(r.cloud == "oci" for r in result)


@pytest.mark.integration
@pytest.mark.usefixtures("_check_oci_credentials")
def test_generate_oci_returns_instance_type_info_objects():
    result = generate_oci_catalog(["VM.DenseIO"])
    assert all(isinstance(r, InstanceTypeInfo) for r in result)
