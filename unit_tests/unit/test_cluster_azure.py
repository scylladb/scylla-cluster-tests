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

from sdcm.cluster_azure import AzureNode


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
