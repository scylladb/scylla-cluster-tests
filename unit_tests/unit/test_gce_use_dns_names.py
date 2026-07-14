import json
from unittest.mock import MagicMock

import pytest

from sdcm.cluster_gce import GCENode
from sdcm.provision.network_configuration import NetworkInterface, ScyllaNetworkConfiguration
from sdcm.sct_config import SCTConfiguration


def _make_gce_node_mock(
    interfaces,
    remoter=None,
    use_dns_names=True,
    private_dns="node-1.c.my-project.internal",
    public_dns="34.1.2.3.bc.googleusercontent.com",
):
    node = MagicMock(spec=GCENode)
    node._instance = MagicMock(network_interfaces=interfaces)
    node.remoter = remoter
    node.use_dns_names = use_dns_names
    node.private_dns_name = private_dns
    node.public_dns_name = public_dns
    node.network_configuration = {}
    return node


def _make_gce_iface(ip, public_ip=None):
    iface = MagicMock()
    iface.network_i_p = ip
    if public_ip:
        iface.access_configs = [MagicMock(nat_i_p=public_ip)]
    else:
        iface.access_configs = []
    return iface


class FakeRefreshNode:
    def __init__(self, scylla_network_configuration=None, network_interfaces=None):
        self.network_configuration = {"old": "data"}
        self.scylla_network_configuration = scylla_network_configuration
        self.network_interfaces = network_interfaces or []


def test_gce_node_network_interfaces_with_remoter_and_device_names():
    mock_iface = _make_gce_iface("10.0.0.1", "34.1.2.3")
    remoter = MagicMock()
    node = _make_gce_node_mock([mock_iface], remoter=remoter)
    node.network_configuration = {"aa:bb:cc:dd:ee:f0": "ens4"}

    result = GCENode.network_interfaces.fget(node)

    assert len(result) == 1
    assert result[0].ipv4_private_addresses == ["10.0.0.1"]
    assert result[0].ipv4_public_address == "34.1.2.3"
    assert result[0].dns_private_name == "node-1.c.my-project.internal"
    assert result[0].dns_public_name == "34.1.2.3.bc.googleusercontent.com"
    assert result[0].use_dns_names is True
    assert result[0].device_index == 0
    assert result[0].device_name == "ens4"


def test_gce_node_network_interfaces_without_remoter():
    mock_iface = _make_gce_iface("10.0.0.1", "34.1.2.3")
    node = _make_gce_node_mock([mock_iface], remoter=None)

    result = GCENode.network_interfaces.fget(node)

    assert result[0].dns_private_name == ""
    assert result[0].dns_public_name == ""
    assert result[0].device_name == ""


def test_gce_node_network_interfaces_no_public_ip():
    mock_iface = _make_gce_iface("10.0.0.2", "")
    remoter = MagicMock()
    node = _make_gce_node_mock(
        [mock_iface], remoter=remoter, use_dns_names=False, private_dns="node-2.c.my-project.internal", public_dns=""
    )
    node.network_configuration = {"aa:bb:cc:dd:ee:f0": "ens4"}

    result = GCENode.network_interfaces.fget(node)

    assert result[0].ipv4_public_address is None


def test_gce_node_network_interfaces_no_access_configs():
    mock_iface = _make_gce_iface("10.0.0.3")
    remoter = MagicMock()
    node = _make_gce_node_mock(
        [mock_iface], remoter=remoter, use_dns_names=False, private_dns="node-3.c.my-project.internal", public_dns=""
    )
    node.network_configuration = {}

    result = GCENode.network_interfaces.fget(node)

    assert result[0].ipv4_public_address is None
    assert result[0].device_name == ""


def test_gce_node_network_interfaces_multiple_interfaces():
    iface0 = _make_gce_iface("10.0.0.1", "34.1.2.3")
    iface1 = _make_gce_iface("10.0.1.1")
    remoter = MagicMock()
    node = _make_gce_node_mock([iface0, iface1], remoter=remoter, use_dns_names=False)
    node.network_configuration = {"aa:bb:cc:dd:ee:f0": "ens4", "aa:bb:cc:dd:ee:f1": "ens5"}

    result = GCENode.network_interfaces.fget(node)

    assert len(result) == 2
    assert result[0].device_index == 0
    assert result[0].device_name == "ens4"
    assert result[1].device_index == 1
    assert result[1].device_name == "ens5"
    assert result[1].ipv4_public_address is None


def test_gce_network_configuration_parses_ip_json_link():
    node = MagicMock(spec=GCENode)
    node.remoter = MagicMock()
    node.name = "test-node-1"
    ip_json_output = json.dumps(
        [
            {"ifname": "lo", "address": "00:00:00:00:00:00"},
            {"ifname": "ens4", "address": "42:01:0a:80:00:02"},
            {"ifname": "ens5", "address": "42:01:0a:80:01:02"},
        ]
    )
    node.remoter.run.return_value = MagicMock(stdout=ip_json_output)
    node.log = MagicMock()

    result = GCENode.network_configuration.func(node)

    assert result == {"42:01:0a:80:00:02": "ens4", "42:01:0a:80:01:02": "ens5"}


def test_gce_network_configuration_without_remoter():
    node = MagicMock(spec=GCENode)
    node.remoter = None
    node.name = "test-node-2"
    node.log = MagicMock()

    result = GCENode.network_configuration.func(node)

    assert result == {}


def test_gce_refresh_network_interfaces_info():
    node = FakeRefreshNode(scylla_network_configuration=MagicMock(), network_interfaces=[MagicMock()])
    GCENode.refresh_network_interfaces_info(node)

    assert not hasattr(node, "network_configuration") or "network_configuration" not in node.__dict__
    assert node.scylla_network_configuration.network_interfaces == node.network_interfaces


def test_gce_refresh_network_interfaces_info_no_scylla_config():
    node = FakeRefreshNode(scylla_network_configuration=None)
    GCENode.refresh_network_interfaces_info(node)


def test_scylla_network_config_device_with_gce_interfaces():
    """ScyllaNetworkConfiguration.device works with GCE-style interfaces (non-None device_name)."""
    interfaces = [
        NetworkInterface(
            ipv4_public_address="34.1.2.3",
            ipv6_public_addresses=[],
            ipv4_private_addresses=["10.0.0.1"],
            ipv6_private_address="",
            dns_private_name="node-1.c.my-project.internal",
            dns_public_name="",
            device_index=0,
            device_name="ens4",
            mac_address=None,
            use_dns_names=True,
        )
    ]
    scylla_network_config = [
        {"address": "listen_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "rpc_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "broadcast_rpc_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "broadcast_address", "ip_type": "ipv4", "public": False, "nic": 0},
        {"address": "test_communication", "ip_type": "ipv4", "public": True, "nic": 0},
    ]
    config = ScyllaNetworkConfiguration(
        network_interfaces=interfaces,
        scylla_network_config=scylla_network_config,
    )
    assert config.device == "ens4"


def test_scylla_network_config_device_empty_string():
    """ScyllaNetworkConfiguration.device returns empty string when device_name is empty."""
    interfaces = [
        NetworkInterface(
            ipv4_public_address="34.1.2.3",
            ipv6_public_addresses=[],
            ipv4_private_addresses=["10.0.0.1"],
            ipv6_private_address="",
            dns_private_name="",
            dns_public_name="",
            device_index=0,
            device_name="",
            mac_address=None,
            use_dns_names=False,
        )
    ]
    scylla_network_config = [
        {"address": "listen_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "rpc_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "broadcast_rpc_address", "ip_type": "ipv4", "public": False, "nic": 0, "use_dns": False},
        {"address": "broadcast_address", "ip_type": "ipv4", "public": False, "nic": 0},
        {"address": "test_communication", "ip_type": "ipv4", "public": True, "nic": 0},
    ]
    config = ScyllaNetworkConfiguration(
        network_interfaces=interfaces,
        scylla_network_config=scylla_network_config,
    )
    assert config.device == ""


def test_gce_cql_address_uses_scylla_network_configuration():
    node = MagicMock(spec=GCENode)
    node.scylla_network_configuration = MagicMock()
    node.scylla_network_configuration.broadcast_rpc_address = "node-1.c.my-project.internal"
    node.scylla_network_configuration.test_communication = "34.1.2.3"
    node.test_config = MagicMock()
    node.test_config.IP_SSH_CONNECTIONS = "private"
    node.log = MagicMock()

    result = GCENode.cql_address.func(node)

    assert result == "node-1.c.my-project.internal"


def test_gce_cql_address_public_uses_test_communication():
    node = MagicMock(spec=GCENode)
    node.scylla_network_configuration = MagicMock()
    node.scylla_network_configuration.broadcast_rpc_address = "node-1.c.my-project.internal"
    node.scylla_network_configuration.test_communication = "34.1.2.3"
    node.test_config = MagicMock()
    node.test_config.IP_SSH_CONNECTIONS = "public"
    node.log = MagicMock()

    result = GCENode.cql_address.func(node)

    assert result == "34.1.2.3"


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
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for gce backend: {exc}")


def test_use_dns_names_aws_does_not_raise(aws_base_env):
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for aws backend: {exc}")


def test_use_dns_names_none_backend_does_not_raise(monkeypatch):
    monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    try:
        SCTConfiguration()
    except ValueError as exc:
        if "use_dns_names" in str(exc):
            pytest.fail(f"use_dns_names raised ValueError for None backend: {exc}")


def test_use_dns_names_azure_raises(azure_base_env):
    with pytest.raises(ValueError, match="use_dns_names is not supported for azure backend"):
        SCTConfiguration()
