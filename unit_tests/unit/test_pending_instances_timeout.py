"""How long SCT waits for pending instances, on a real cloud and on minicloud (SCT-1145)."""

import math
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_aws import AWSCluster
from sdcm.cluster_gce import GCE_RUNNING_POLL_INTERVAL, CreateGCENodeError, GCECluster
from sdcm.utils.common import (
    MINICLOUD_PENDING_INSTANCES_TIMEOUT,
    PENDING_INSTANCES_POLL_INTERVAL,
    PENDING_INSTANCES_TIMEOUT,
    list_instances_aws,
)

MINICLOUD_ENV_VARS = ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL")


@pytest.fixture
def no_minicloud_env(monkeypatch):
    """Unset the minicloud endpoint variables so the tests run as on a real cloud."""
    for var in MINICLOUD_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def pending_ec2_client():
    """Mocked boto3 EC2 client with one pending instance and no random jitter."""
    client = MagicMock()
    client.describe_instances.return_value = {
        "Reservations": [{"Instances": [{"InstanceId": "i-0001", "State": {"Name": "pending"}}]}]
    }
    with (
        patch("sdcm.utils.common.boto3.client", return_value=client),
        patch("sdcm.utils.common.random.random", return_value=0),
    ):
        yield client


@pytest.fixture
def patched_list_instances():
    """Patch the AWS instance listing and the EC2 wrapper, yielding the list_instances_aws mock."""
    with (
        patch("sdcm.cluster_aws.list_instances_aws", return_value={"eu-west-1": []}) as mock_list,
        patch("sdcm.cluster_aws.ec2_client.EC2ClientWrapper"),
    ):
        yield mock_list


@pytest.fixture
def never_running_gce_cluster():
    """Factory for a fake GCE cluster whose instance never reaches RUNNING, with sleep patched out."""
    with patch("sdcm.utils.decorators.time.sleep"):
        yield lambda params: NeverRunningGCECluster(params=params)


def waiter_config(client: MagicMock) -> dict:
    """Return the WaiterConfig passed to the last EC2 waiter call."""
    return client.get_waiter.return_value.wait.call_args.kwargs["WaiterConfig"]


def fake_aws_cluster(params: dict) -> SimpleNamespace:
    """Minimal stand-in for AWSCluster carrying only the attributes _get_instances reads."""
    return SimpleNamespace(
        region_names=["eu-west-1"],
        node_type="scylla-db",
        params=params,
        test_config=SimpleNamespace(test_id=lambda: "test-id-123"),
    )


class NeverRunningGCECluster(SimpleNamespace):
    """Fake GCE cluster that counts attempts and always reports the instance as STAGING."""

    attempts = 0

    def _get_running_instance(self, name, dc_idx):
        self.attempts += 1
        raise CreateGCENodeError(f"Instance {name} is not in RUNNING state: STAGING")


def test_list_instances_aws_waits_ten_minutes_by_default(pending_ec2_client):
    """Default pending wait is 600 s, polled every PENDING_INSTANCES_POLL_INTERVAL."""
    list_instances_aws(region_name="eu-west-1", running=True)

    assert waiter_config(pending_ec2_client) == {
        "Delay": PENDING_INSTANCES_POLL_INTERVAL,
        "MaxAttempts": math.ceil(PENDING_INSTANCES_TIMEOUT / PENDING_INSTANCES_POLL_INTERVAL),
    }
    assert PENDING_INSTANCES_TIMEOUT == 600


def test_list_instances_aws_rounds_pending_timeout_up_to_a_whole_poll(pending_ec2_client):
    """A pending_timeout that is not a multiple of the poll interval rounds up to whole polls."""
    list_instances_aws(region_name="eu-west-1", running=True, pending_timeout=20)

    assert waiter_config(pending_ec2_client)["MaxAttempts"] == 2


def test_get_instances_keeps_default_wait_on_real_aws(no_minicloud_env, patched_list_instances):
    """On real AWS, _get_instances waits the default PENDING_INSTANCES_TIMEOUT."""
    AWSCluster._get_instances(fake_aws_cluster({}), dc_idx=0)

    assert patched_list_instances.call_args.kwargs["pending_timeout"] == PENDING_INSTANCES_TIMEOUT


def test_get_instances_waits_for_cold_image_cache_on_minicloud(no_minicloud_env, patched_list_instances):
    """On minicloud, _get_instances waits the longer MINICLOUD_PENDING_INSTANCES_TIMEOUT for a cold image cache."""
    AWSCluster._get_instances(fake_aws_cluster({"minicloud_endpoint_url": "http://localhost:5000"}), dc_idx=0)

    assert patched_list_instances.call_args.kwargs["pending_timeout"] == MINICLOUD_PENDING_INSTANCES_TIMEOUT
    assert MINICLOUD_PENDING_INSTANCES_TIMEOUT > PENDING_INSTANCES_TIMEOUT


def test_gce_running_wait_keeps_default_on_real_gce(no_minicloud_env, never_running_gce_cluster):
    """On real GCE, a never-running instance is retried PENDING_INSTANCES_TIMEOUT / poll-interval times."""
    cluster = never_running_gce_cluster({})

    with pytest.raises(CreateGCENodeError):
        GCECluster._get_instance_with_retry(cluster, name="db-1", dc_idx=0)

    assert cluster.attempts == PENDING_INSTANCES_TIMEOUT // GCE_RUNNING_POLL_INTERVAL


def test_gce_running_wait_covers_cold_image_cache_on_minicloud(no_minicloud_env, never_running_gce_cluster):
    """On minicloud, a never-running instance is retried MINICLOUD_PENDING_INSTANCES_TIMEOUT / poll-interval times."""
    cluster = never_running_gce_cluster({"minicloud_endpoint_url": "http://localhost:5000"})

    with pytest.raises(CreateGCENodeError):
        GCECluster._get_instance_with_retry(cluster, name="db-1", dc_idx=0)

    assert cluster.attempts == MINICLOUD_PENDING_INSTANCES_TIMEOUT // GCE_RUNNING_POLL_INTERVAL
