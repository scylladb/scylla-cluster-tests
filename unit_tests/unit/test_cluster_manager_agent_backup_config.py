"""Unit tests for BaseNode.update_manager_agent_backup_config."""

from contextlib import nullcontext
from unittest.mock import Mock

import pytest

from sdcm.cluster import BaseNode

OCI_ENDPOINT = "https://ns.compat.objectstorage.us-phoenix-1.oci.customer-oci.com"
OCI_CREDS = {"access_key_id": "SENTINEL_OCI_ACCESS_KEY_ID", "secret_access_key": "SENTINEL_OCI_SECRET_ACCESS_KEY"}
OCI_ENDPOINTS_YAML = {"object_storage_endpoints": [{"name": OCI_ENDPOINT, "aws_region": "us-phoenix-1"}]}


@pytest.fixture(autouse=True)
def _node_region(monkeypatch):
    """BaseNode.region goes through vm_region, which is abstract on BaseNode."""
    monkeypatch.setattr(BaseNode, "region", property(lambda self: "us-east-1"))


def _node(params: dict) -> tuple[BaseNode, dict]:
    """BaseNode with the manager agent yaml captured in a plain dict."""
    node = BaseNode.__new__(BaseNode)
    node.log = Mock()
    node.parent_cluster = Mock(params=params)
    node.test_config = Mock(backup_oci_credentials=OCI_CREDS)
    agent_yaml = {}
    node.remote_manager_agent_yaml = lambda: nullcontext(agent_yaml)
    return node, agent_yaml


def test_oci_configures_s3_section_from_object_storage_endpoints():
    node, agent_yaml = _node(
        {"backup_bucket_backend": "s3", "cluster_backend": "oci", "append_scylla_yaml": OCI_ENDPOINTS_YAML}
    )

    node.update_manager_agent_backup_config(region="us-phoenix-1")

    assert agent_yaml["s3"] == {
        "endpoint": OCI_ENDPOINT,
        "region": "us-phoenix-1",
        "access_key_id": OCI_CREDS["access_key_id"],
        "secret_access_key": OCI_CREDS["secret_access_key"],
        "provider": "Other",
    }
    node.log.warning.assert_not_called()


@pytest.mark.parametrize("append_scylla_yaml", [None, {}, {"auto_snapshot": False}])
def test_oci_without_object_storage_endpoints_warns_instead_of_failing(append_scylla_yaml):
    """OCI tests that install the manager agent but never back up must not crash on setup."""
    node, agent_yaml = _node(
        {"backup_bucket_backend": "s3", "cluster_backend": "oci", "append_scylla_yaml": append_scylla_yaml}
    )

    node.update_manager_agent_backup_config(region="us-phoenix-1")

    assert agent_yaml["s3"] == {}
    node.log.warning.assert_called_once()
    assert "object_storage_endpoints" in node.log.warning.call_args.args[0]


def test_non_oci_s3_keeps_region_override():
    """The OCI branch must not swallow the generic s3 path used by aws / aws-siren."""
    node, agent_yaml = _node({"backup_bucket_backend": "s3", "cluster_backend": "aws", "append_scylla_yaml": None})

    node.update_manager_agent_backup_config(region="eu-west-1")

    assert agent_yaml["s3"] == {"region": "eu-west-1"}


def test_non_oci_s3_same_region_leaves_section_empty():
    node, agent_yaml = _node({"backup_bucket_backend": "s3", "cluster_backend": "aws", "append_scylla_yaml": None})

    node.update_manager_agent_backup_config(region="us-east-1")

    assert agent_yaml["s3"] == {}


def test_general_config_is_written_to_rclone_section():
    node, agent_yaml = _node(
        {"backup_bucket_backend": "s3", "cluster_backend": "oci", "append_scylla_yaml": OCI_ENDPOINTS_YAML}
    )
    rclone = {"transfers": 4, "checkers": 8}

    node.update_manager_agent_backup_config(region="us-phoenix-1", general_config=rclone)

    assert agent_yaml["rclone"] == rclone
