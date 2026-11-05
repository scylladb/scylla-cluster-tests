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

"""The sources allowed to open SSH to the SCT Jenkins builders.

The addresses live in the QA keystore, keyed by role, because this repository is public.
"""

import ipaddress

from sdcm.keystore import KeyStore

BUILDER_SSH_SOURCES_FILE = "builder_ssh_sources.json"
BUILDER_SSH_PORT = 22
JENKINS_ROLE = "jenkins"


def validate_builder_ssh_sources(raw: object) -> dict[str, list[str]]:
    """Validate the keystore entry and return it with normalised CIDRs.

    Requires the `jenkins` role, no empty roles, and IPv4 CIDRs other than 0.0.0.0/0.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"{BUILDER_SSH_SOURCES_FILE} must map a role to a list of CIDRs, got {type(raw).__name__}")
    if JENKINS_ROLE not in raw:
        raise ValueError(f"{BUILDER_SSH_SOURCES_FILE} has no '{JENKINS_ROLE}' role")
    sources = {}
    for role, values in raw.items():
        if not isinstance(values, list) or not values:
            raise ValueError(f"{BUILDER_SSH_SOURCES_FILE}: role '{role}' must have a non-empty list of CIDRs")
        cidrs = []
        for value in values:
            try:
                network = ipaddress.IPv4Network(value)
            except (ValueError, TypeError) as exc:
                raise ValueError(
                    f"{BUILDER_SSH_SOURCES_FILE}: '{value}' of role '{role}' is not an IPv4 network"
                ) from exc
            if network.prefixlen == 0:
                raise ValueError(
                    f"{BUILDER_SSH_SOURCES_FILE}: '{value}' of role '{role}' opens SSH to the whole internet"
                )
            cidrs.append(str(network))
        sources[role] = cidrs
    return sources


def get_builder_ssh_sources() -> dict[str, list[str]]:
    return validate_builder_ssh_sources(KeyStore().get_json(BUILDER_SSH_SOURCES_FILE))


def flatten_sources(sources: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Return `(cidr, role)` pairs sorted by CIDR."""
    return sorted((cidr, role) for role, cidrs in sources.items() for cidr in cidrs)


ALL_TRAFFIC = "-1"
# the boto3 `IpPermission` list that holds each kind of source
SOURCE_LISTS = {
    "CidrIp": "IpRanges",
    "CidrIpv6": "Ipv6Ranges",
    "GroupId": "UserIdGroupPairs",
    "PrefixListId": "PrefixListIds",
}


def _rule_keys(permission: dict) -> set[tuple]:
    """Split a boto3 `IpPermission` into one `(protocol, from port, to port, kind, source)` key per source."""
    protocol = permission["IpProtocol"]
    ports = (None, None) if protocol == ALL_TRAFFIC else (permission.get("FromPort"), permission.get("ToPort"))
    return {
        (protocol, *ports, kind, entry[kind])
        for kind, list_name in SOURCE_LISTS.items()
        for entry in permission.get(list_name, [])
    }


def _permission(key: tuple, description: str | None = None) -> dict:
    protocol, from_port, to_port, kind, source = key
    entry = {kind: source} | ({"Description": description} if description else {})
    permission = {"IpProtocol": protocol, SOURCE_LISTS[kind]: [entry]}
    if protocol != ALL_TRAFFIC:
        permission |= {"FromPort": from_port, "ToPort": to_port}
    return permission


def builder_ingress_diff(
    current: list[dict], sources: dict[str, list[str]], internal: dict[str, str]
) -> tuple[list[dict], list[dict]]:
    """Return the `(revoke, authorize)` rules that bring the builder group ingress to the allowed set.

    Allowed: SSH from the `sources` CIDRs and all traffic from the `internal` CIDRs (CIDR to description).
    """
    wanted = {
        ("tcp", BUILDER_SSH_PORT, BUILDER_SSH_PORT, "CidrIp", cidr): role for cidr, role in flatten_sources(sources)
    }
    wanted |= {(ALL_TRAFFIC, None, None, "CidrIp", cidr): name for cidr, name in internal.items()}
    present = set().union(*(_rule_keys(permission) for permission in current))
    revoke = [_permission(key) for key in sorted(present - wanted.keys(), key=str)]
    authorize = [_permission(key, wanted[key]) for key in sorted(wanted.keys() - present, key=str)]
    return revoke, authorize
