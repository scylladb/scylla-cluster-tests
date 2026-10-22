"""Unit tests for sdcm.xcloud.cluster_runtime_utils module."""

import time
from unittest.mock import MagicMock

import pytest

from sdcm.cluster_cloud import ScyllaCloudCluster
from sdcm.xcloud.api_client import ScyllaCloudAPIClient
from sdcm.xcloud.cluster_runtime_utils import (
    ScaleOutFailedException,
    wait_for_cluster_disk_utilization,
    wait_for_cluster_scale_out,
    wait_for_resize_request,
)

ACCOUNT_ID = 100
CLUSTER_ID = 200


@pytest.fixture
def api_client():
    """Cloud API client mock, so no request reaches Scylla Cloud."""
    return MagicMock(spec=ScyllaCloudAPIClient)


@pytest.fixture
def cluster(api_client):
    """Scylla Cloud cluster mock exposing the mocked API client and the test cluster IDs."""
    return MagicMock(spec=ScyllaCloudCluster, api_client=api_client, account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID)


@pytest.fixture
def no_sleep(monkeypatch):
    """Record sleep calls instead of sleeping, so polling and retries run instantly."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    return sleeps


# --- disk utilization ---


def test_wait_for_cluster_disk_utilization_polls_until_target_reached(cluster, api_client, no_sleep):
    """The wait must keep polling used storage until utilization reaches the target percentage."""
    api_client.get_cluster_total_storage.return_value = 100
    api_client.get_cluster_used_storage.side_effect = [10, 69, 71]

    check_interval = 5
    wait_for_cluster_disk_utilization(cluster, target_utilization=70, check_interval=check_interval)

    assert api_client.get_cluster_used_storage.call_count == 3
    api_client.get_cluster_used_storage.assert_called_with(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID)
    assert no_sleep == [check_interval] * 2


# --- cluster resize ---


@pytest.mark.parametrize(
    "requests_sequence",
    [
        pytest.param([[{"id": 7, "requestType": "RESIZE_CLUSTER_V3", "status": "IN_PROGRESS"}]], id="found-at-once"),
        pytest.param(
            [
                [],
                [{"id": 6, "requestType": "CREATE_CLUSTER", "status": "IN_PROGRESS"}],
                [{"id": 7, "requestType": "RESIZE_CLUSTER_V3", "status": "IN_PROGRESS"}],
            ],
            id="found-after-retries",
        ),
        pytest.param(
            [
                [
                    {"id": 5, "requestType": "RESIZE_CLUSTER_V3", "status": "COMPLETED"},
                    {"id": 7, "requestType": "RESIZE_CLUSTER_V3", "status": "IN_PROGRESS"},
                ]
            ],
            id="older-completed-resize-listed-first",
        ),
    ],
)
def test_wait_for_resize_request_returns_in_progress_resize_id(cluster, api_client, no_sleep, requests_sequence):
    """The wait must return the ID of the in-progress resize request, retrying until it appears."""
    api_client.get_cluster_requests.side_effect = requests_sequence

    assert wait_for_resize_request(cluster) == 7
    assert api_client.get_cluster_requests.call_count == len(requests_sequence)
    api_client.get_cluster_requests.assert_called_with(account_id=ACCOUNT_ID, cluster_id=CLUSTER_ID)


def test_wait_for_cluster_scale_out_retries_until_completed(cluster, api_client, no_sleep):
    """The wait must keep polling the resize request until it is COMPLETED."""
    api_client.get_cluster_request_details.side_effect = [
        {"status": "IN_PROGRESS"},
        {"status": "IN_PROGRESS"},
        {"status": "COMPLETED"},
    ]

    wait_for_cluster_scale_out(cluster, request_id=7)

    assert api_client.get_cluster_request_details.call_count == 3
    api_client.get_cluster_request_details.assert_called_with(account_id=ACCOUNT_ID, request_id=7)


@pytest.mark.parametrize("status", [pytest.param("FAILED", id="failed"), pytest.param("CANCELLED", id="cancelled")])
def test_wait_for_cluster_scale_out_failed_request_raises_immediately(cluster, api_client, no_sleep, status):
    """A failed or cancelled resize must fail the wait at once instead of polling until the retry limit."""
    api_client.get_cluster_request_details.return_value = {"status": status}

    with pytest.raises(ScaleOutFailedException):
        wait_for_cluster_scale_out(cluster, request_id=7)

    assert api_client.get_cluster_request_details.call_count == 1
