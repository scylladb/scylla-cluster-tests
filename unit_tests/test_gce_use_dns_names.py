import pytest
from unittest.mock import MagicMock

from sdcm.cluster_gce import GCENode
from sdcm.provision.network_configuration import NetworkInterface
from sdcm.sct_config import SCTConfiguration


def test_gce_node_network_interfaces_with_public_ip():
    """network_interfaces returns correct NetworkInterface with public IP."""
    mock_iface = MagicMock()
    mock_iface.network_i_p = "10.0.0.1"
    mock_iface.access_configs = [MagicMock(nat_i_p="34.1.2.3")]

    node = MagicMock(spec=GCENode)
    node._instance = MagicMock(network_interfaces=[mock_iface])
    node.private_dns_name = "node-1.us-east1-b.c.my-project.internal"
    node.public_dns_name = "34.1.2.3.bc.googleusercontent.com"
    node.use_dns_names = True

    result = GCENode.network_interfaces.fget(node)

    assert len(result) == 1
    assert isinstance(result[0], NetworkInterface)
    assert result[0].ipv4_private_addresses == ["10.0.0.1"]
    assert result[0].ipv4_public_address == "34.1.2.3"
    assert result[0].dns_private_name == "node-1.us-east1-b.c.my-project.internal"
    assert result[0].dns_public_name == "34.1.2.3.bc.googleusercontent.com"
    assert result[0].use_dns_names is True
    assert result[0].device_index == 0


def test_gce_node_network_interfaces_no_public_ip():
    """Empty nat_i_p normalizes to None (not empty string)."""
    mock_iface = MagicMock()
    mock_iface.network_i_p = "10.0.0.2"
    mock_iface.access_configs = [MagicMock(nat_i_p="")]

    node = MagicMock(spec=GCENode)
    node._instance = MagicMock(network_interfaces=[mock_iface])
    node.private_dns_name = "node-2.us-east1-b.c.my-project.internal"
    node.public_dns_name = ""
    node.use_dns_names = False

    result = GCENode.network_interfaces.fget(node)

    assert len(result) == 1
    assert result[0].ipv4_public_address is None


def test_gce_node_network_interfaces_no_access_configs():
    """No access_configs means public IP is None."""
    mock_iface = MagicMock()
    mock_iface.network_i_p = "10.0.0.3"
    mock_iface.access_configs = []

    node = MagicMock(spec=GCENode)
    node._instance = MagicMock(network_interfaces=[mock_iface])
    node.private_dns_name = "node-3.us-east1-b.c.my-project.internal"
    node.public_dns_name = ""
    node.use_dns_names = False

    result = GCENode.network_interfaces.fget(node)

    assert result[0].ipv4_public_address is None


def test_gce_node_network_interfaces_multiple_interfaces():
    """Multiple interfaces get correct device_index values."""
    iface0 = MagicMock()
    iface0.network_i_p = "10.0.0.1"
    iface0.access_configs = [MagicMock(nat_i_p="34.1.2.3")]

    iface1 = MagicMock()
    iface1.network_i_p = "10.0.1.1"
    iface1.access_configs = []

    node = MagicMock(spec=GCENode)
    node._instance = MagicMock(network_interfaces=[iface0, iface1])
    node.private_dns_name = "node-1.us-east1-b.c.my-project.internal"
    node.public_dns_name = "34.1.2.3.bc.googleusercontent.com"
    node.use_dns_names = False

    result = GCENode.network_interfaces.fget(node)

    assert len(result) == 2
    assert result[0].device_index == 0
    assert result[1].device_index == 1
    assert result[1].ipv4_public_address is None


@pytest.fixture()
def gce_base_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_GCE_DATACENTER", "us-east1")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB", "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-5-4-0"
    )
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "5.4.0")
    monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
    monkeypatch.setattr(
        "sdcm.sct_config.get_scylla_gce_images_versions",
        lambda version: [MagicMock(self_link="fake-link", name="scylla-5-4-0")],
    )


@pytest.fixture()
def aws_base_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")


@pytest.fixture()
def azure_base_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_AZURE_REGION_NAME", '["eastus"]')
    monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")


def test_use_dns_names_gce_does_not_raise(gce_base_env):
    """use_dns_names=True with cluster_backend=gce must NOT raise ValueError."""
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for gce backend: {exc}")


def test_use_dns_names_aws_does_not_raise(aws_base_env):
    """use_dns_names=True with cluster_backend=aws must NOT raise ValueError."""
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for aws backend: {exc}")


def test_use_dns_names_none_backend_does_not_raise(monkeypatch):
    """use_dns_names=True with cluster_backend=None (e.g. send-email) must NOT raise."""
    monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for None backend: {exc}")


def test_use_dns_names_azure_raises(azure_base_env):
    """use_dns_names=True with cluster_backend=azure must raise ValueError."""
    with pytest.raises(ValueError, match="use_dns_names is not supported for azure backend"):
        SCTConfiguration()
