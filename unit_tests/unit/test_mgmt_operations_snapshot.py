"""Unit tests for SnapshotOperations backup bucket access."""

from unittest.mock import patch

import pytest

from sdcm.mgmt.operations import SnapshotOperations

OCI_ENDPOINT = "https://ns.compat.objectstorage.us-phoenix-1.oci.customer-oci.com"
OCI_CREDS = {"access_key_id": "SENTINEL_OCI_ACCESS_KEY_ID", "secret_access_key": "SENTINEL_OCI_SECRET_ACCESS_KEY"}


def snapshot_ops(params: dict) -> SnapshotOperations:
    """SnapshotOperations with only ``params`` set, bypassing the constructor."""
    ops = SnapshotOperations.__new__(SnapshotOperations)
    ops.params = params
    return ops


def test_backup_s3_client_uses_plain_boto3_outside_oci():
    """_backup_s3_client calls plain boto3 with the given region when the backend is not OCI."""
    ops = snapshot_ops({"cluster_backend": "aws"})
    with patch("sdcm.mgmt.operations.boto3.client") as mock_client:
        ops._backup_s3_client("us-east-1")
    mock_client.assert_called_once_with("s3", region_name="us-east-1")


def test_backup_s3_client_uses_oci_endpoint_and_credentials():
    """_backup_s3_client targets the OCI endpoint with OCI credentials and path-style addressing."""
    ops = snapshot_ops(
        {
            "cluster_backend": "oci",
            "append_scylla_yaml": {"object_storage_endpoints": [{"name": OCI_ENDPOINT, "aws_region": "us-phoenix-1"}]},
        }
    )
    with (
        patch("sdcm.mgmt.operations.boto3.client") as mock_client,
        patch("sdcm.mgmt.operations.TestConfig") as mock_test_config,
    ):
        mock_test_config.return_value.backup_oci_credentials = OCI_CREDS
        ops._backup_s3_client("us-phoenix-1")

    kwargs = mock_client.call_args.kwargs
    assert mock_client.call_args.args == ("s3",)
    assert kwargs["region_name"] == "us-phoenix-1"
    assert kwargs["endpoint_url"] == OCI_ENDPOINT
    assert kwargs["aws_access_key_id"] == OCI_CREDS["access_key_id"]
    assert kwargs["aws_secret_access_key"] == OCI_CREDS["secret_access_key"]
    assert kwargs["config"].s3 == {"addressing_style": "path"}


@pytest.mark.parametrize(
    "region_name",
    [pytest.param(None, id="no-region"), pytest.param("us-east-1", id="aws-region")],
)
def test_backup_s3_client_uses_endpoint_region_on_oci(region_name):
    """OCI rejects requests signed for another region, so the AWS `region_name` param must not leak in."""
    ops = snapshot_ops(
        {
            "cluster_backend": "oci",
            "append_scylla_yaml": {"object_storage_endpoints": [{"name": OCI_ENDPOINT, "aws_region": "us-phoenix-1"}]},
        }
    )
    with (
        patch("sdcm.mgmt.operations.boto3.client") as mock_client,
        patch("sdcm.mgmt.operations.TestConfig") as mock_test_config,
    ):
        mock_test_config.return_value.backup_oci_credentials = OCI_CREDS
        ops._backup_s3_client(region_name)

    assert mock_client.call_args.kwargs["region_name"] == "us-phoenix-1"


def test_backup_s3_client_fills_region_placeholder_from_oci_region_name():
    """The {region} placeholder in the OCI endpoint is filled from oci_region_name."""
    ops = snapshot_ops(
        {
            "cluster_backend": "oci",
            "oci_region_name": ["us-ashburn-1"],
            "append_scylla_yaml": {
                "object_storage_endpoints": [
                    {"name": "https://ns.compat.objectstorage.{region}.oci.customer-oci.com", "aws_region": "{region}"}
                ]
            },
        }
    )
    with (
        patch("sdcm.mgmt.operations.boto3.client") as mock_client,
        patch("sdcm.mgmt.operations.TestConfig") as mock_test_config,
    ):
        mock_test_config.return_value.backup_oci_credentials = OCI_CREDS
        ops._backup_s3_client("us-east-1")

    kwargs = mock_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://ns.compat.objectstorage.us-ashburn-1.oci.customer-oci.com"
    assert kwargs["region_name"] == "us-ashburn-1"
