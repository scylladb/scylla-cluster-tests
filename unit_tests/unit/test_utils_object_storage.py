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


@pytest.mark.parametrize("oci_region_name", [["us-ashburn-1", "us-phoenix-1"], "us-ashburn-1 us-phoenix-1"])
def test_backup_bucket_region_on_oci_uses_oci_region_name(oci_region_name):
    params = Params(cluster_backend="oci", oci_region_name=oci_region_name)

    assert backup_bucket_region(params) == "us-ashburn-1"


@pytest.mark.parametrize("cluster_backend", ["aws", "gce", "azure"])
def test_backup_bucket_region_outside_oci_uses_region_names(cluster_backend):
    assert backup_bucket_region(Params(cluster_backend=cluster_backend)) == "eu-west-1"


@pytest.mark.parametrize(
    "cluster_backend,endpoint,expected",
    [
        (
            "oci",
            OCI_ENDPOINT_TEMPLATE,
            {"name": "https://ns.compat.objectstorage.us-ashburn-1.oci.customer-oci.com", "aws_region": "us-ashburn-1"},
        ),
        (
            "aws",
            S3_ENDPOINT_TEMPLATE,
            {"name": "s3.eu-west-1.amazonaws.com", "port": 443, "https": True, "aws_region": "eu-west-1"},
        ),
    ],
)
def test_object_storage_endpoints_fill_region(cluster_backend, endpoint, expected):
    params = Params(
        cluster_backend=cluster_backend,
        oci_region_name=["us-ashburn-1"],
        append_scylla_yaml={"object_storage_endpoints": [endpoint]},
    )

    assert object_storage_endpoints(params) == [expected]
    assert params["append_scylla_yaml"]["object_storage_endpoints"] == [endpoint], "params must not be modified"


@pytest.mark.parametrize("append_scylla_yaml", [None, {}, {"auto_snapshot": False}])
def test_missing_object_storage_endpoints(append_scylla_yaml):
    params = Params(cluster_backend="oci", append_scylla_yaml=append_scylla_yaml)

    assert object_storage_endpoints(params) == []
