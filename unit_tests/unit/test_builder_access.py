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

from sdcm.utils.builder_access import ssh_ingress_diff, validate_builder_ssh_sources

SOURCES = {"jenkins": ["192.0.2.10/32"], "office": ["198.51.100.10/32"]}


def test_validate_rejects_missing_jenkins_role():
    with pytest.raises(ValueError, match="jenkins"):
        validate_builder_ssh_sources({"office": ["198.51.100.10/32"]})


def test_validate_rejects_world_range():
    with pytest.raises(ValueError, match="0.0.0.0/0"):
        validate_builder_ssh_sources({"jenkins": ["0.0.0.0/0"]})


def test_ssh_ingress_diff_replaces_world_rules():
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
            "UserIdGroupPairs": [],
            "PrefixListIds": [],
        },
        {
            "IpProtocol": "-1",
            "IpRanges": [],
            "Ipv6Ranges": [],
            "UserIdGroupPairs": [{"GroupId": "sg-self"}],
            "PrefixListIds": [],
        },
    ]
    revoke, authorize = ssh_ingress_diff(current, SOURCES)
    revoked_v4 = {r["CidrIp"] for p in revoke for r in p.get("IpRanges", [])}
    revoked_v6 = {r["CidrIpv6"] for p in revoke for r in p.get("Ipv6Ranges", [])}
    revoked_groups = {g["GroupId"] for p in revoke for g in p.get("UserIdGroupPairs", [])}
    assert revoked_v4 == {"0.0.0.0/0"}
    assert revoked_v6 == {"::/0"}
    assert revoked_groups == {"sg-self"}
    # the allowed CIDR already in place is neither revoked nor added again
    assert [(p["FromPort"], p["ToPort"], p["IpProtocol"], p["IpRanges"]) for p in authorize] == [
        (22, 22, "tcp", [{"CidrIp": "198.51.100.10/32", "Description": "office"}]),
    ]
