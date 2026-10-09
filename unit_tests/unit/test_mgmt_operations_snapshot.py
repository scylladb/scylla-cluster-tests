"""Unit tests for SnapshotOperations backup bucket access."""

from unittest.mock import patch

import pytest

from sdcm.mgmt.operations import SnapshotOperations

OCI_ENDPOINT = "https://ns.compat.objectstorage.us-phoenix-1.oci.customer-oci.com"
OCI_CREDS = {"access_key_id": "SENTINEL_OCI_ACCESS_KEY_ID", "secret_access_key": "SENTINEL_OCI_SECRET_ACCESS_KEY"}


def _snapshot_ops(params: dict) -> SnapshotOperations:
    ops = SnapshotOperations.__new__(SnapshotOperations)
    ops.params = params
    return ops


def test_backup_s3_client_uses_plain_boto3_outside_oci():
    ops = _snapshot_ops({"cluster_backend": "aws"})
    with patch("sdcm.mgmt.operations.boto3.client") as mock_client:
        ops._backup_s3_client("us-east-1")
    mock_client.assert_called_once_with("s3", region_name="us-east-1")


def test_backup_s3_client_uses_oci_endpoint_and_credentials():
    ops = _snapshot_ops(
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


def test_backup_s3_client_falls_back_to_endpoint_region_on_oci():
    """Bucket-region discovery via get_bucket_location is an AWS thing; OCI takes it from the endpoint."""
    ops = _snapshot_ops(
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
        ops._backup_s3_client(None)

    assert mock_client.call_args.kwargs["region_name"] == "us-phoenix-1"


@pytest.mark.parametrize("append_scylla_yaml", [None, {}, {"auto_snapshot": False}])
def test_backup_s3_client_requires_oci_endpoints(append_scylla_yaml):
    ops = _snapshot_ops({"cluster_backend": "oci", "append_scylla_yaml": append_scylla_yaml})
    with pytest.raises(ValueError, match="object_storage_endpoints"):
        ops._backup_s3_client("us-phoenix-1")
