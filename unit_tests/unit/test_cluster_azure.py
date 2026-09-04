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

from unittest.mock import Mock

import pytest

from sdcm.cluster_azure import AzureNode, Ipv6AddressNotFoundError


def azure_ip_configuration(private_ip: str, version: str = "IPv4", public_ip: str = None):
    return Mock(
        private_ip_address=private_ip,
        private_ip_address_version=version,
        public_ip_address=Mock(ip_address=public_ip) if public_ip else None,
    )


def azure_nic(mac: str, ip_configurations: list):
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


@pytest.fixture(name="node")
def fixture_node():
    """An AzureNode with just enough wired up to build its network interfaces."""
    node = AzureNode.__new__(AzureNode)
    node._cached_network_interfaces = None
    node.use_dns_names = False
    node.remoter = Mock()
    node._instance = Mock(name="vm", private_dns_name="vm.internal.cloudapp.net")
    node._instance.name = "vm"
    return node


def configure(node, nics: list, devices: dict):
    node._instance._provisioner = Mock(network_interfaces=Mock(return_value=nics))
    # BaseNode.network_configuration is a cached_property, so an instance attribute shadows it
    node.network_configuration = devices


def test_single_nic_is_reported_with_its_device_name(node):
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
    node._instance._provisioner._ip_provider.get.side_effect = lambda name, version, index: Mock(
        ip_address=addresses[(version, index)]
    )

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address == "20.1.2.3"
    assert interface.ipv6_public_addresses == ["2603:1030::1"]


def test_an_interface_without_a_public_ip_asks_the_ip_provider_for_nothing(node):
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    (interface,) = node.network_interfaces
    assert interface.ipv4_public_address is None
    node._instance._provisioner._ip_provider.get.assert_not_called()


def test_interfaces_are_cached_until_invalidated(node):
    configure(node, [azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4")])], {})

    node.network_interfaces
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 1

    node._invalidate_network_interfaces_cache()
    node.network_interfaces
    assert node._instance._provisioner.network_interfaces.call_count == 2


class TestIpv6AddressResolution:
    """`AzureNode._get_ipv6_ip_address` while Azure has not published the address yet.

    With 'test_communication' set to 'ip_type: ipv6', this address is what SCT opens its SSH
    connection to, so it is resolved before the node has a remoter at all - the OS fallback
    cannot be reached and the Azure API is the only source.
    """

    IPV6_PARAMS = {"azure_network_interfaces": [{"ipv6": True, "public_ipv6": True}]}

    @staticmethod
    def ipv6_nic():
        return azure_nic(
            "00-0D-3A-11-11-11",
            [
                azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3"),
                azure_ip_configuration("fd00:db8:5c7::4", version="IPv6", public_ip="2603:1030::1"),
            ],
        )

    @staticmethod
    def ipv4_nic():
        return azure_nic("00-0D-3A-11-11-11", [azure_ip_configuration("10.0.0.4", public_ip="20.1.2.3")])

    @pytest.fixture(name="ipv6_node")
    def fixture_ipv6_node(self, node):
        node.name = "azure-node-1"
        node.destroyed = False
        node.scylla_network_configuration = None
        node.parent_cluster = Mock(params=self.IPV6_PARAMS)
        return node

    def test_address_is_taken_from_the_api(self, ipv6_node):
        configure(ipv6_node, [self.ipv6_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"

    def test_the_address_is_found_without_a_remoter(self, ipv6_node):
        """The regression test: this used to raise AttributeError on the absent remoter."""
        ipv6_node.remoter = None
        configure(ipv6_node, [self.ipv6_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == "2603:1030::1"

    def test_a_missing_address_fails_loudly(self, ipv6_node):
        ipv6_node.remoter = None
        configure(ipv6_node, [self.ipv4_nic()], {})

        with pytest.raises(Ipv6AddressNotFoundError):
            ipv6_node._get_ipv6_ip_address()

    def test_an_ipv4_only_run_needs_no_address(self, ipv6_node):
        """No IPv6 in the configuration means no address and, above all, no waiting for one."""
        ipv6_node.remoter = None
        ipv6_node.parent_cluster = Mock(params={"azure_network_interfaces": [{}]})
        configure(ipv6_node, [self.ipv4_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == ""

    def test_a_destroyed_node_reports_no_address(self, ipv6_node):
        """`destroy()` drops the remoter; log collection and Argus still read the addresses."""
        ipv6_node.remoter = None
        ipv6_node.destroyed = True
        configure(ipv6_node, [self.ipv4_nic()], {})

        assert ipv6_node._get_ipv6_ip_address() == ""

    def test_the_os_is_not_queried_without_a_remoter(self, ipv6_node):
        ipv6_node.remoter = None

        assert ipv6_node._discover_ipv6_from_os() == {}
