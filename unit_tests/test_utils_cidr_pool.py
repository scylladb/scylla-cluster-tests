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
# Copyright (c) 2025 ScyllaDB

import ipaddress
import pytest
from unittest.mock import Mock, patch

from sdcm.utils.cidr_pool import CidrPoolManager, CidrAllocationError


DEFAULT_SC_CIDR = "172.31.0.0/24"


def test_init_with_defaults():
    manager = CidrPoolManager()
    assert str(manager.cidr_base) == "172.31.0.0/16"
    assert manager.subnet_size == 24
    assert str(manager.reserved_ranges[0]) == DEFAULT_SC_CIDR


def test_init_with_custom_parameters():
    manager = CidrPoolManager(cidr_base="10.0.0.0/8", subnet_size=20)
    assert str(manager.cidr_base) == "10.0.0.0/8"
    assert manager.subnet_size == 20


def test_generate_next_cidr():
    manager = CidrPoolManager()
    used_cidrs = {"172.31.1.0/24", "172.31.2.0/24", "172.31.3.0/24"}

    result = manager._generate_next_cidr(used_cidrs)

    assert isinstance(result, ipaddress.IPv4Network)
    assert result.prefixlen == 24
    assert manager.cidr_base.supernet_of(result)
    assert str(result) not in used_cidrs
    assert str(result) != DEFAULT_SC_CIDR


def test_generate_next_cidr_exhausted_pool():
    manager = CidrPoolManager(cidr_base="192.168.0.0/30", subnet_size=32)

    all_possible_subnets = set()
    for subnet in ipaddress.ip_network("192.168.0.0/30").subnets(new_prefix=32):
        all_possible_subnets.add(str(subnet))

    result = manager._generate_next_cidr(all_possible_subnets)
    assert result is None


def test_init_with_invalid_cidr():
    with pytest.raises(ValueError):
        CidrPoolManager(cidr_base="not-a-cidr")


def test_generate_next_cidr_invalid_subnet_config():
    with pytest.raises(CidrAllocationError):
        CidrPoolManager(cidr_base="192.168.1.0/24", subnet_size=16)._generate_next_cidr(set())


@patch('sdcm.utils.cidr_pool.AwsRegion')
def test_get_available_cidr_aws(mock_aws_region):
    mock_region_instance = Mock()
    mock_region_instance.get_vpc_peering_routes.return_value = ["172.31.1.0/24", "172.31.2.0/24"]
    mock_aws_region.return_value = mock_region_instance

    manager = CidrPoolManager()
    result = manager.get_available_cidr("aws", "us-east-1")

    assert isinstance(result, str)
    result_network = ipaddress.ip_network(result)
    assert result_network.prefixlen == 24
    assert manager.cidr_base.supernet_of(result_network)
    assert result not in ["172.31.1.0/24", "172.31.2.0/24", "172.31.0.0/24"]

    mock_aws_region.assert_called_once_with("us-east-1")
    mock_region_instance.get_vpc_peering_routes.assert_called_once()


@patch('sdcm.utils.cidr_pool.GceRegion')
def test_get_available_cidr_gce(mock_gce_region):
    mock_region_instance = Mock()
    mock_region_instance.get_peering_routes.return_value = ["172.31.3.0/24", "172.31.4.0/24"]
    mock_gce_region.return_value = mock_region_instance

    manager = CidrPoolManager()
    result = manager.get_available_cidr("gce", "us-central1")

    assert isinstance(result, str)
    result_network = ipaddress.ip_network(result)
    assert result_network.prefixlen == 24
    assert manager.cidr_base.supernet_of(result_network)
    assert result not in ["172.31.3.0/24", "172.31.4.0/24", "172.31.0.0/24"]

    mock_gce_region.assert_called_once_with("us-central1")
    mock_region_instance.get_peering_routes.assert_called_once()


@patch('sdcm.utils.cidr_pool.AwsRegion')
def test_no_available_cidrs_to_get(mock_aws_region):
    mock_region_instance = Mock()
    mock_region_instance.get_vpc_peering_routes.return_value = []
    mock_aws_region.return_value = mock_region_instance

    manager = CidrPoolManager(cidr_base="192.168.0.0/30", subnet_size=32)
    with patch.object(manager, '_generate_next_cidr', return_value=None):
        with pytest.raises(CidrAllocationError) as exc_info:
            manager.get_available_cidr("aws", "us-east-1")

        assert "No available CIDR blocks" in str(exc_info.value)
        assert "aws:us-east-1" in str(exc_info.value)
