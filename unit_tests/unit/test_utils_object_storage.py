"""Unit tests for sdcm.utils.object_storage."""

import pytest

from sdcm.utils.object_storage import backup_bucket_region, object_storage_endpoints

OCI_ENDPOINT_TEMPLATE = {
    "name": "https://ns.compat.objectstorage.{region}.oci.customer-oci.com",
    "aws_region": "{region}",
}
S3_ENDPOINT_TEMPLATE = {"name": "s3.{region}.amazonaws.com", "port": 443, "https": True, "aws_region": "{region}"}


class Params(dict):
    """dict params with the AWS `region_names` property of SCTConfiguration."""

    region_names = ["eu-west-1", "us-east-1"]


@pytest.mark.parametrize(
    "oci_region_name",
    [
        pytest.param(["us-ashburn-1", "us-phoenix-1"], id="list"),
        pytest.param("us-ashburn-1 us-phoenix-1", id="space-separated"),
    ],
)
def test_backup_bucket_region_on_oci_uses_oci_region_name(oci_region_name):
    """On OCI the backup bucket region is the first configured OCI region, whatever the config format."""
    params = Params(cluster_backend="oci", oci_region_name=oci_region_name)

    assert backup_bucket_region(params) == "us-ashburn-1"


@pytest.mark.parametrize(
    "cluster_backend",
    [
        pytest.param("aws", id="aws"),
        pytest.param("gce", id="gce"),
        pytest.param("azure", id="azure"),
    ],
)
def test_backup_bucket_region_outside_oci_uses_region_names(cluster_backend):
    """Outside OCI the backup bucket region is the first of the region_names."""
    assert backup_bucket_region(Params(cluster_backend=cluster_backend)) == "eu-west-1"


@pytest.mark.parametrize(
    "cluster_backend,endpoint,expected",
    [
        pytest.param(
            "oci",
            OCI_ENDPOINT_TEMPLATE,
            {"name": "https://ns.compat.objectstorage.us-ashburn-1.oci.customer-oci.com", "aws_region": "us-ashburn-1"},
            id="oci",
        ),
        pytest.param(
            "aws",
            S3_ENDPOINT_TEMPLATE,
            {"name": "s3.eu-west-1.amazonaws.com", "port": 443, "https": True, "aws_region": "eu-west-1"},
            id="aws",
        ),
    ],
)
def test_object_storage_endpoints_fill_region(cluster_backend, endpoint, expected):
    """object_storage_endpoints fills the {region} placeholder per backend and leaves the params unchanged."""
    params = Params(
        cluster_backend=cluster_backend,
        oci_region_name=["us-ashburn-1"],
        append_scylla_yaml={"object_storage_endpoints": [endpoint]},
    )

    assert object_storage_endpoints(params) == [expected]
    assert params["append_scylla_yaml"]["object_storage_endpoints"] == [endpoint], "params must not be modified"


@pytest.mark.parametrize(
    "append_scylla_yaml",
    [
        pytest.param(None, id="none"),
        pytest.param({}, id="empty"),
        pytest.param({"auto_snapshot": False}, id="no-endpoints-key"),
    ],
)
def test_missing_object_storage_endpoints(append_scylla_yaml):
    """object_storage_endpoints returns an empty list when scylla.yaml has no object_storage_endpoints."""
    params = Params(cluster_backend="oci", append_scylla_yaml=append_scylla_yaml)

    assert object_storage_endpoints(params) == []
