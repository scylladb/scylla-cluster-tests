"""Unit tests for cluster helpers of sdcm.xcloud.api_client.ScyllaCloudAPIClient."""

from unittest.mock import patch

import pytest

from sdcm.xcloud.api_client import ScyllaCloudAPIClient

ACCOUNT_ID = 100
CLUSTER_ID = 200


@pytest.fixture
def api_client():
    """Real API client; the tests patch its raw API calls, so no request reaches Scylla Cloud."""
    return ScyllaCloudAPIClient(api_url="https://cloud.example.com/api", auth_token="token")


def test_get_active_cluster_node_ids_skips_deleted_nodes(api_client):
    """Nodes that are deleted or being deleted must not be reported as active, whatever the status case."""
    nodes = [
        {"id": 1, "status": "ACTIVE"},
        {"id": 2, "status": "deleted"},
        {"id": 3, "status": "PENDING_DELETE"},
        {"id": 4, "status": "CREATING"},
    ]
    with patch.object(api_client, "get_cluster_nodes", return_value=nodes) as get_cluster_nodes:
        assert api_client.get_active_cluster_node_ids(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID) == {1, 4}

    get_cluster_nodes.assert_called_once_with(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID)


def test_get_cluster_total_storage_counts_only_db_nodes(api_client):
    """Total storage must count only DB nodes; non-DB nodes (e.g. VS, Manager) carry a nodeType field."""
    cluster_details = {
        "instance": {"totalStorage": 500},
        "nodes": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4, "nodeType": "VECTOR_SEARCH"}],
    }
    with patch.object(api_client, "get_cluster_details", return_value=cluster_details) as get_cluster_details:
        assert api_client.get_cluster_total_storage(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID) == 1500

    get_cluster_details.assert_called_once_with(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID, enriched=True)


def test_get_cluster_used_storage_returns_requested_cluster_in_gb(api_client):
    """Used storage must be taken from the requested cluster only and converted from bytes to GB."""
    clusters = [
        {"id": 999, "metrics": {"STORAGE_USED": 7 * 1024**3}},
        {"id": CLUSTER_ID, "metrics": {"STORAGE_USED": 3 * 1024**3}},
    ]
    with patch.object(api_client, "get_clusters", return_value=clusters) as get_clusters:
        assert api_client.get_cluster_used_storage(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID) == 3

    get_clusters.assert_called_once_with(account_id=ACCOUNT_ID, metrics="STORAGE_USED")
