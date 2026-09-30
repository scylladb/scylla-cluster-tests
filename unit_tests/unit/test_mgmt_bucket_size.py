import logging
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from sdcm.mgmt.operations import SnapshotOperations

CLUSTER_ID = "8c20f334-cf37-4528-9219-862d75b84c99"


@pytest.fixture
def bucket_ops():
    ops = SnapshotOperations.__new__(SnapshotOperations)
    ops.log = logging.getLogger(__name__)
    ops.params = MagicMock()
    return ops


@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="manager-backup-tests")
        objects = {
            f"backup/sst/cluster/{CLUSTER_ID}/dc/dc1/node/n1/keyspace/ks/table/t/v/a.db": 1000,
            f"backup/sst/cluster/{CLUSTER_ID}/dc/dc1/node/n2/keyspace/ks/table/t/v/b.db": 2000,
            f"backup/meta/cluster/{CLUSTER_ID}/dc/dc1/node/n1/task/t/tag/sm_1/manifest.json.gz": 30,
            f"backup/schema/cluster/{CLUSTER_ID}/task_t_tag_sm_1_schema.json.gz": 5,
            "backup/sst/cluster/other-cluster/dc/dc1/node/n1/keyspace/ks/table/t/v/c.db": 999999,
            f"path/backup/sst/cluster/{CLUSTER_ID}/dc/dc1/node/n1/keyspace/ks/table/t/v/d.db": 7,
        }
        for key, size in objects.items():
            s3.put_object(Bucket="manager-backup-tests", Key=key, Body=b"x" * size)
        yield


def test_get_cluster_size_on_bucket_s3(bucket_ops, s3_bucket):
    assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "s3:manager-backup-tests") == 3035
    assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "AWS_US_EAST_1:s3:manager-backup-tests") == 3035
    assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "s3:manager-backup-tests/path") == 7
    assert bucket_ops.get_cluster_size_on_bucket("missing-cluster", "s3:manager-backup-tests") == 0


def test_get_cluster_size_on_bucket_gcs(bucket_ops):
    blobs_by_prefix = {
        f"backup/sst/cluster/{CLUSTER_ID}/": [MagicMock(size=1000), MagicMock(size=2000)],
        f"backup/meta/cluster/{CLUSTER_ID}/": [MagicMock(size=30)],
        f"backup/schema/cluster/{CLUSTER_ID}/": [MagicMock(size=5)],
    }
    fake_client = MagicMock()
    fake_client.list_blobs.side_effect = lambda bucket_or_name, prefix: blobs_by_prefix.get(prefix, [])
    with patch("sdcm.mgmt.operations.get_gce_storage_client", return_value=(fake_client, {})):
        assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "gcs:manager-backup-tests") == 3035
        assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "GCE_US_EAST_1:gcs:manager-backup-tests") == 3035
        assert bucket_ops.get_cluster_size_on_bucket("missing-cluster", "gcs:manager-backup-tests") == 0
    assert {call.kwargs["bucket_or_name"] for call in fake_client.list_blobs.call_args_list} == {"manager-backup-tests"}


def test_get_cluster_size_on_bucket_azure(bucket_ops):
    blobs_by_prefix = {
        f"backup/sst/cluster/{CLUSTER_ID}/": [MagicMock(size=1000), MagicMock(size=2000)],
        f"backup/meta/cluster/{CLUSTER_ID}/": [MagicMock(size=30)],
        f"backup/schema/cluster/{CLUSTER_ID}/": [MagicMock(size=5)],
    }
    container_client = MagicMock()
    container_client.list_blobs.side_effect = lambda name_starts_with: blobs_by_prefix.get(name_starts_with, [])
    fake_service = MagicMock()
    fake_service.blob.get_container_client.return_value = container_client
    with patch("sdcm.mgmt.operations.AzureService", return_value=fake_service):
        assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "azure:manager-backup-tests") == 3035
        assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "AZURE_EASTUS:azure:manager-backup-tests") == 3035
        assert bucket_ops.get_cluster_size_on_bucket("missing-cluster", "azure:manager-backup-tests") == 0
    fake_service.blob.get_container_client.assert_called_with(container="manager-backup-tests")


def test_get_cluster_size_on_bucket_unsupported_backend(bucket_ops):
    assert bucket_ops.get_cluster_size_on_bucket(CLUSTER_ID, "unknown:bucket") is None
