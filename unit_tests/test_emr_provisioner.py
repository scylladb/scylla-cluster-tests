"""Unit tests for EMR cluster provisioner using moto for AWS API mocking."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import pytest

os.environ["MOTO_AMIS_PATH"] = str(Path(__file__).parent / "test_data" / "mocked_ami_data.json")
from moto.server import ThreadedMotoServer

from sdcm.provision.aws.emr_provisioner import EmrClusterProvisioner, list_emr_clusters

AWS_REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_server():
    """Run a mocked AWS server for testing."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    aws_endpoint_url = f"http://{host}:{port}"
    os.environ["AWS_ENDPOINT_URL"] = aws_endpoint_url
    yield aws_endpoint_url
    del os.environ["AWS_ENDPOINT_URL"]
    server.stop()


@pytest.fixture(scope="module")
def iam_roles(moto_server):
    """Create required IAM roles for EMR."""
    iam = boto3.client("iam", region_name=AWS_REGION)
    trust_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"elasticmapreduce.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    ec2_trust_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

    iam.create_role(RoleName="EMR_DefaultRole", AssumeRolePolicyDocument=trust_policy)
    iam.create_role(RoleName="EMR_EC2_DefaultRole", AssumeRolePolicyDocument=ec2_trust_policy)
    iam.create_instance_profile(InstanceProfileName="EMR_EC2_DefaultRole")
    iam.add_role_to_instance_profile(InstanceProfileName="EMR_EC2_DefaultRole", RoleName="EMR_EC2_DefaultRole")


class MockParams:
    """Mock params object that behaves like SCTConfiguration for testing."""

    def __init__(self, **kwargs):
        self._data = {
            "emr_release_label": "emr-7.8.0",
            "emr_instance_type_master": "m5.xlarge",
            "emr_instance_type_core": "m5.xlarge",
            "emr_instance_count_core": 2,
            "emr_instance_type_task": "",
            "emr_instance_count_task": 0,
            "emr_spot_bid_percentage": 100,
            "emr_applications": ["Spark"],
            "emr_keep_alive": True,
            "emr_log_uri": "",
        }
        self._data.update(kwargs)

    def get(self, key, default=None):
        return self._data.get(key, default)


@pytest.fixture()
def emr_params():
    return MockParams()


@pytest.fixture()
def provisioner(moto_server, iam_roles, emr_params):
    return EmrClusterProvisioner(region_name=AWS_REGION, params=emr_params)


def test_create_emr_cluster(provisioner):
    """Test creating an EMR cluster."""
    cluster_id = provisioner.create_emr_cluster(
        test_id="test-1234-5678",
        user="test_user",
    )
    assert cluster_id is not None
    assert cluster_id.startswith("j-")
    assert provisioner.cluster_id == cluster_id


def test_get_emr_cluster_status(provisioner):
    """Test getting EMR cluster status after creation."""
    provisioner.create_emr_cluster(
        test_id="test-status-check",
        user="test_user",
    )
    status = provisioner.get_emr_cluster_status()
    assert "State" in status
    assert status["State"] in ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING")


def test_get_emr_cluster_description(provisioner):
    """Test getting full EMR cluster description."""
    provisioner.create_emr_cluster(
        test_id="test-describe",
        user="test_user",
    )
    desc = provisioner.get_emr_cluster_description()
    assert desc["Id"] == provisioner.cluster_id
    assert "Status" in desc


def test_terminate_emr_cluster(provisioner):
    """Test terminating an EMR cluster."""
    provisioner.create_emr_cluster(
        test_id="test-terminate",
        user="test_user",
    )
    cluster_id = provisioner.cluster_id
    provisioner.terminate_emr_cluster()

    status = provisioner.get_emr_cluster_status(cluster_id)
    assert status["State"] in ("TERMINATING", "TERMINATED")


def test_list_emr_clusters_by_tags(moto_server, iam_roles):
    """Test listing EMR clusters filtered by tags."""
    params = MockParams()
    prov = EmrClusterProvisioner(region_name=AWS_REGION, params=params)

    prov.create_emr_cluster(test_id="list-test-001", user="user1")
    prov.create_emr_cluster(test_id="list-test-002", user="user1")

    clusters = list_emr_clusters({"RunByUser": "user1"}, region_name=AWS_REGION)
    assert len(clusters) >= 2

    clusters_by_test = list_emr_clusters({"TestId": "list-test-001"}, region_name=AWS_REGION)
    assert len(clusters_by_test) >= 1
    assert all(c["Tags"].get("TestId") == "list-test-001" for c in clusters_by_test)


def test_build_instance_groups_default(provisioner):
    """Test default instance groups configuration."""
    groups = provisioner._build_instance_groups()
    assert len(groups) == 2  # Master + Core

    master = groups[0]
    assert master["Name"] == "Master"
    assert master["InstanceRole"] == "MASTER"
    assert master["Market"] == "ON_DEMAND"
    assert master["InstanceCount"] == 1

    core = groups[1]
    assert core["Name"] == "Core"
    assert core["InstanceRole"] == "CORE"
    assert core["InstanceCount"] == 2


def test_build_instance_groups_with_task():
    """Test instance groups with task nodes (Spot)."""
    params = MockParams(
        emr_instance_type_task="m5.xlarge",
        emr_instance_count_task=3,
        emr_spot_bid_percentage=80,
    )
    prov = EmrClusterProvisioner(region_name=AWS_REGION, params=params)
    groups = prov._build_instance_groups()
    assert len(groups) == 3

    task = groups[2]
    assert task["Name"] == "Task"
    assert task["InstanceRole"] == "TASK"
    assert task["Market"] == "SPOT"
    assert task["InstanceCount"] == 3
    assert task["BidPriceAsPercentageOfOnDemandPrice"] == 80


def test_add_step(provisioner):
    """Test adding a step to an EMR cluster."""
    provisioner.create_emr_cluster(
        test_id="test-step",
        user="test_user",
    )
    step_id = provisioner.add_step(
        cluster_id=provisioner.cluster_id,
        step_name="test-spark-submit",
        jar_path="s3://bucket/test.jar",
        args=["--conf", "spark.executor.memory=4g"],
    )
    assert step_id is not None


def test_get_step_status(provisioner):
    """Test getting step status."""
    provisioner.create_emr_cluster(
        test_id="test-step-status",
        user="test_user",
    )
    step_id = provisioner.add_step(
        cluster_id=provisioner.cluster_id,
        step_name="test-step-status",
        jar_path="s3://bucket/test.jar",
    )
    status = provisioner.get_step_status(provisioner.cluster_id, step_id)
    assert "State" in status
