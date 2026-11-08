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

"""Hosts of the baremetal hosts config, for code that has no node object for them."""

from sdcm.localhost import LocalHost


def host_ssh_address(host: dict, ip_ssh_connections: str | None, localhost: LocalHost | None) -> dict:
    """Where the runner opens SSH to a host of the hosts config, as `ssh_login_info` keys.

    A host named in the lab access service (`access`) is reached where that service says, through `localhost`: the
    caller must pass one for such a host. Any other host is reached at its public address on a `public` role (its
    private one when it has none), else at its private one.
    Used where SCT has no node object for the host: log collection and cleanup after the run.
    """
    if access := host.get("access"):
        if localhost is None:
            raise ValueError(f"Host {access!r} is reached through the lab access service, which needs a LocalHost")
        # The access service holds the host's credentials and accepts only `none` auth, which the SSH client tries
        # only without a key
        return {"key_file": None, **localhost.lab_access_ssh_address(access)}
    if ip_ssh_connections == "public":
        return {"hostname": host.get("public_ip", host["private_ip"])}
    return {"hostname": host["private_ip"]}
