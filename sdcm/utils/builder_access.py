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
            except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, ValueError, TypeError) as exc:
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


def _is_ssh_permission(permission: dict) -> bool:
    return (
        permission.get("IpProtocol") == "tcp"
        and permission.get("FromPort") == BUILDER_SSH_PORT
        and permission.get("ToPort") == BUILDER_SSH_PORT
    )


def ssh_ingress_diff(current: list[dict], sources: dict[str, list[str]]) -> tuple[list[dict], list[dict]]:
    """Compare the ingress of a security group with the allowed SSH sources.

    Return `(revoke, authorize)` as boto3 `IpPermissions` lists. Only TCP 22 from an allowed
    CIDR is kept; descriptions do not count as a difference.
    """
    allowed = dict(flatten_sources(sources))
    revoke, present = [], set()
    for permission in current:
        if not _is_ssh_permission(permission):
            revoke.append(permission)
            continue
        present.update(r["CidrIp"] for r in permission.get("IpRanges", []) if r["CidrIp"] in allowed)
        # everything in an SSH permission except an allowed IPv4 CIDR goes
        stale = {
            "IpRanges": [r for r in permission.get("IpRanges", []) if r["CidrIp"] not in allowed],
            "Ipv6Ranges": permission.get("Ipv6Ranges", []),
            "UserIdGroupPairs": permission.get("UserIdGroupPairs", []),
            "PrefixListIds": permission.get("PrefixListIds", []),
        }
        if any(stale.values()):
            stale = {key: value for key, value in stale.items() if value}
            revoke.append({"IpProtocol": "tcp", "FromPort": BUILDER_SSH_PORT, "ToPort": BUILDER_SSH_PORT} | stale)
    authorize = [
        {
            "IpProtocol": "tcp",
            "FromPort": BUILDER_SSH_PORT,
            "ToPort": BUILDER_SSH_PORT,
            "IpRanges": [{"CidrIp": cidr, "Description": role}],
        }
        for cidr, role in allowed.items()
        if cidr not in present
    ]
    return revoke, authorize
