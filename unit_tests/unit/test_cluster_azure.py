# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""AzureNode network interface introspection."""

from unittest.mock import Mock, patch

import pytest

from sdcm.cluster_azure import AzureNode, Ipv6AddressNotFoundError
from sdcm.sct_runner import AzureSctRunner


@pytest.fixture
def node():
    """An AzureNode with just enough wired up to build its network interfaces."""
    node = AzureNode.__new__(AzureNode)
    node._cached_network_interfaces = None
    node.use_dns_names = False
    node.remoter = Mock()
    node._instance = Mock(name="vm", private_dns_name="vm.internal.cloudapp.net")
    node._instance.name = "vm"
    return node


@pytest.fixture
def ipv6_params():
    """Scylla network config that asks for IPv6 on the test communication address."""
    return {"scylla_network_config": [{"address": "test_communication", "ip_type": "ipv6", "public": True, "nic": 0}]}


@pytest.fixture
def ipv6_node(node, ipv6_params):
    """A node whose IPv6 address is resolved before it has a remoter, so the Azure API is the only source."""
    node.name = "azure-node-1"
    node.destroyed = False
    node.scylla_network_configuration = None
    node.parent_cluster = Mock(params=ipv6_params)
    return node


@pytest.fixture
def node_without_ptr(node, monkeypatch):
    """A node whose private IP has no PTR record, so reverse DNS fails."""
    node.name = "db-node-1"
    node._private_ip_address_resolved = True
    node._private_ip_address_cached = "10.180.0.10"
    monkeypatch.setattr(
        "sdcm.cluster_azure.resolve_ip_to_dns",
        Mock(side_effect=ValueError("Unable to resolve IP: [Errno 1] Unknown host")),
    )
    return node


def azure_ip_configuration(private_ip: str, version: str = "IPv4", public_ip: str = None):
    """Mock ipConfiguration with a private address and an optional public IP."""
    return Mock(
        private_ip_address=private_ip,
        private_ip_address_version=version,
        public_ip_address=Mock(ip_address=public_ip) if public_ip else None,
    )


def azure_nic(mac: str, ip_configurations: list):
    """Mock NIC with the given MAC address and ipConfigurations."""
    return Mock(mac_address=mac, ip_configurations=ip_configurations)


def unexpanded_ip_configuration(private_ip: str, version: str = "IPv4"):
    """An ipConfiguration as Azure actually returns it.

    The Public IP comes back as a sub-resource reference: an id, but no `ipAddress` unless the NIC
    was fetched with `expand=IPConfigurations/PublicIPAddress`. `spec` is what makes the attribute
    genuinely absent rather than a Mock that answers to everything.
    """
    return Mock(
        private_ip_address=private_ip,
        private_ip_address_version=version,
        public_ip_address=Mock(spec=["id", "name"]),
    )


def configure(node, nics: list, devices: dict):
    """Wire the provisioner NICs and the device-name map onto the node."""
    node._instance._provisioner = Mock(network_interfaces=Mock(return_value=nics))
    # BaseNode.network_configuration is a cached_property, so an instance attribute shadows it
    node.network_configuration = devices


def ipv6_nic():
    """NIC with a public IPv4 and a routable IPv6 ipConfiguration."""
    return azure_nic(
        "00-0D-3A-11-11-11",
        [
            azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3"),
            azure_ip_configuration("fd00:db8:5c7::4", version="IPv6", public_ip="2603:1030::1"),
        ],
    )


def ipv4_nic():
    """NIC with a single IPv4 ipConfiguration that has a public IP."""
    return azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3")])


def make_sct_runner(params: dict):
    """AzureSctRunner with only the params its network interfaces read."""
    runner = AzureSctRunner.__new__(AzureSctRunner)
    runner.params = params
    return runner


def node_with_os_devices(node, devices: dict, primary_device: str):
    """Node whose OS reports `devices`, with `primary_device` as its primary NIC."""
    node.remoter = Mock()
    node._discover_ipv6_from_os = Mock(return_value=devices)
    node._cached_network_interfaces = [Mock(device_name=primary_device)]
    return node


def test_single_nic_is_reported_with_its_device_name(node):
    """A single NIC is reported with its index, device name and addresses."""
    configure(
        node,
        [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3")])],
        {"00:0d:3a:11:11:11": "eth0"},
    )

    (interface,) = node.network_interfaces
    assert interface.device_index == 0
    assert interface.device_name == "eth0"
    assert interface.ipv4_private_addresses == ["10.0.0.4"]
    assert interface.ipv4_public_address == "20.1.2.3"


def test_interfaces_keep_the_provisioner_device_order(node):
    """Interfaces follow the provisioner's order, not the device names."""
    configure(
        node,
        [
            azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")]),
            azure_nic("00-0D-3A-22-22-22", [azure_ip_configuration("10.0.1.4")]),
        ],
        {"00:0d:3a:11:11:11": "eth0", "00:0d:3a:22:22:22": "eth1"},
    )

    interfaces = node.network_interfaces
    assert [interface.device_index for interface in interfaces] == [0, 1]
    assert [interface.device_name for interface in interfaces] == ["eth0", "eth1"]
    assert [interface.ipv4_private_addresses for interface in interfaces] == [["10.0.0.4"], ["10.0.1.4"]]


def test_mac_addresses_are_normalised_to_the_ip_link_format(node):
    """Azure reports '00-0D-3A-...', ip-link reports '00:0d:3a:...'."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    assert node.network_interfaces[0].mac_address == "00:0d:3a:11:11:11"


def test_ipv6_configuration_is_split_into_private_and_routable_addresses(node):
    """A VNet-local (ULA) IPv6 is private; only a Public IP resource is routable."""
    configure(
        node,
        [
            azure_nic(
                "00-0D-3A-11-11-11",
                [
                    azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3"),
                    azure_ip_configuration("fd00:db8:5c7::4", version="IPv6", public_ip="2603:1030::1"),
                ],
            )
        ],
        {},
    )

    (interface,) = node.network_interfaces
    assert interface.ipv4_private_addresses == ["10.0.0.4"]
    assert interface.ipv6_private_address == "fd00:db8:5c7::4"
    assert interface.ipv6_public_addresses == ["2603:1030::1"]


def test_ipv4_only_nic_reports_no_ipv6_address(node):
    """An IPv4-only NIC has no IPv6 addresses."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    (interface,) = node.network_interfaces
    assert interface.ipv6_private_address == ""
    assert interface.ipv6_public_addresses == []


def test_public_addresses_come_from_the_ip_provider_when_the_nic_omits_them(node):
    """The NIC payload carries no address, so it has to be resolved through the IP provider."""
    configure(
        node,
        [
            azure_nic(
                "00-0D-3A-11-11-11",
                [
                    unexpanded_ip_configuration("10.0.0.4"),
                    unexpanded_ip_configuration("fd00:db8:5c7::4", version="IPv6"),
                ],
            )
        ],
        {},
    )
    addresses = {("IPV4", 0): "20.1.2.3", ("IPV6", 0): "2603:1030::1"}
    node._instance._provisioner.public_ip_address.side_effect = lambda name, version, index: addresses[(version, index)]

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address == "20.1.2.3"
    assert interface.ipv6_public_addresses == ["2603:1030::1"]


def test_an_interface_without_a_public_ip_asks_the_ip_provider_for_nothing(node):
    """A NIC without a public IP reports none and does not query the IP provider."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address is None
    node._instance._provisioner._ip_provider.get.assert_not_called()


def test_interfaces_are_cached_until_invalidated(node):
    """network_interfaces is built once and rebuilt after the cache is invalidated."""
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    node.network_interfaces
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 1

    node._invalidate_network_interfaces_cache()
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 2


def test_ipv6_address_is_taken_from_the_api(ipv6_node):
    """The IPv6 address is read from the API when the NIC publishes it."""
    configure(ipv6_node, [ipv6_nic()], {})

    assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"


def test_ipv6_address_is_found_without_a_remoter(ipv6_node):
    """The IPv6 address is found with no remoter; this used to raise AttributeError on the absent remoter."""
    ipv6_node.remoter = None
    configure(ipv6_node, [ipv6_nic()], {})

    assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"


def test_missing_ipv6_address_fails_loudly(ipv6_node):
    """An IPv6 run whose API publishes no IPv6 address raises Ipv6AddressNotFoundError."""
    ipv6_node.remoter = None
    configure(ipv6_node, [ipv4_nic()], {})

    with pytest.raises(Ipv6AddressNotFoundError):
        ipv6_node._get_ipv6_ip_address()


def test_an_ipv4_only_run_needs_no_address(ipv6_node):
    """No IPv6 in the configuration means no address and, above all, no waiting for one."""
    ipv6_node.remoter = None
    ipv6_node.parent_cluster = Mock(params={})
    configure(ipv6_node, [ipv4_nic()], {})

    assert ipv6_node._get_ipv6_ip_address() == ""


def test_a_destroyed_node_reports_no_address(ipv6_node):
    """`destroy()` drops the remoter; log collection and Argus still read the addresses."""
    ipv6_node.remoter = None
    ipv6_node.destroyed = True
    configure(ipv6_node, [ipv4_nic()], {})

    assert ipv6_node._get_ipv6_ip_address() == ""


def test_os_is_not_queried_for_ipv6_without_a_remoter(ipv6_node):
    """Without a remoter the OS IPv6 discovery returns nothing."""
    ipv6_node.remoter = None

    assert ipv6_node._discover_ipv6_from_os() == {}


def test_ipv4_only_run_gets_no_ipv6():
    """An IPv4-only run gets an IPv4 interface with no IPv6."""
    (interface,) = make_sct_runner({})._network_interfaces()

    assert interface == {"subnet": "default", "public_ip": True, "ipv6": False, "public_ipv6": False}


def test_ipv6_run_gets_a_routable_ipv6(ipv6_params):
    """An IPv6 run gets a public IPv6 interface."""
    (interface,) = make_sct_runner(ipv6_params)._network_interfaces()

    assert interface["ipv6"] is True
    assert interface["public_ipv6"] is True, "the runner reaches the nodes from outside their subnet"


def test_runner_always_has_exactly_one_interface():
    """'scylla_network_config' describes the DB nodes; the runner never needs more than one."""
    params = {
        "scylla_network_config": [
            {"address": "listen_address", "ip_type": "ipv4", "public": False, "nic": nic} for nic in (0, 1, 2)
        ]
    }

    assert len(make_sct_runner(params)._network_interfaces()) == 1


def test_primary_os_ipv6_interface_is_reported(node):
    """A secondary NIC carries a ULA of its own, so the interface has to be chosen, not guessed."""
    devices = {"eth1": ["fd00:db8:5c7:1::4"], "eth0": ["fd00:db8:5c7::4"]}

    assert node_with_os_devices(node, devices, "eth0")._os_ipv6_address() == "fd00:db8:5c7::4"


def test_unknown_primary_device_falls_back_deterministically(node):
    """An unknown primary device falls back to a deterministic address."""
    devices = {"eth1": ["fd00:db8:5c7:1::4"], "eth0": ["fd00:db8:5c7::4"]}

    assert node_with_os_devices(node, devices, "")._os_ipv6_address() == "fd00:db8:5c7::4"


def test_no_os_ipv6_address_is_empty(node):
    """No OS IPv6 addresses give an empty string."""
    assert node_with_os_devices(node, {}, "eth0")._os_ipv6_address() == ""


def test_private_dns_name_falls_back_to_the_node_name_without_a_ptr_record(node_without_ptr):
    """Without a PTR record, private_dns_name falls back to the node name."""
    assert node_without_ptr.private_dns_name == "db-node-1"


def test_private_dns_name_without_a_ptr_record_raises_when_dns_names_are_on(node_without_ptr):
    """With DNS names on, a missing PTR record raises instead of falling back."""
    node_without_ptr.use_dns_names = True
    with pytest.raises(ValueError, match="Unable to resolve IP"):
        _ = node_without_ptr.private_dns_name


@pytest.mark.parametrize(
    "logs_transport,checked",
    [pytest.param("vector", True, id="vector"), pytest.param("syslog-ng", False, id="syslog-ng")],
)
def test_azure_node_checks_vector_only_for_the_vector_transport(logs_transport, checked):
    """configure_remote_logging checks Vector only when Vector is the logs transport."""
    node = AzureNode.__new__(AzureNode)
    node.parent_cluster = Mock(params={"logs_transport": logs_transport})

    with patch.object(AzureNode, "_ensure_vector_is_active") as ensure:
        node.configure_remote_logging()

    assert ensure.called is checked
