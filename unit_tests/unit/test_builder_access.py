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

import pytest

from sdcm.utils.builder_access import builder_ingress_diff, validate_builder_ssh_sources

SOURCES = {"jenkins": ["192.0.2.10/32"], "office": ["198.51.100.10/32"]}


def test_validate_rejects_missing_jenkins_role():
    with pytest.raises(ValueError, match="jenkins"):
        validate_builder_ssh_sources({"office": ["198.51.100.10/32"]})


def test_validate_rejects_world_range():
    with pytest.raises(ValueError, match="0.0.0.0/0"):
        validate_builder_ssh_sources({"jenkins": ["0.0.0.0/0"]})


def test_builder_ingress_diff_keeps_only_allowed_rules():
    current = [
        {
            "FromPort": 22,
            "ToPort": 22,
            "IpProtocol": "tcp",
            "IpRanges": [
                {"CidrIp": "0.0.0.0/0", "Description": "SSH connectivity to the builders"},
                {"CidrIp": "192.0.2.10/32", "Description": "jenkins"},
            ],
            "Ipv6Ranges": [{"CidrIpv6": "::/0", "Description": "SSH connectivity to the builders"}],
        },
        {
            "IpProtocol": "-1",
            "IpRanges": [{"CidrIp": "10.0.0.0/8", "Description": "SCT networks"}],
            "UserIdGroupPairs": [{"GroupId": "sg-builders", "UserId": "1"}],
        },
        {"FromPort": 3000, "ToPort": 3000, "IpProtocol": "tcp", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
    ]
    revoke, authorize = builder_ingress_diff(current, SOURCES, {"10.0.0.0/8": "SCT networks"})
    assert revoke == [
        {"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": "sg-builders"}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "Ipv6Ranges": [{"CidrIpv6": "::/0"}]},
        {"IpProtocol": "tcp", "FromPort": 3000, "ToPort": 3000, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
    ]
    # the allowed CIDRs already in place are neither revoked nor added again
    assert authorize == [
        {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": "198.51.100.10/32", "Description": "office"}],
        },
    ]
