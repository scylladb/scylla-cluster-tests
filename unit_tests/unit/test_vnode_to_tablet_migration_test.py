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

from types import SimpleNamespace

import pytest

from vnode_to_tablet_migration_test import NodeMigrationStatus, get_nodetool_migrate_to_tablets_status

HOST_1 = "dc1c5a03-8878-49da-a6f3-bd27d6a2d85d"
HOST_2 = "b1428349-2154-41c9-a5c1-dd33c71bd571"
HOST_3 = "017dd39a-3d06-4c8a-8ac4-379f9e595607"


def node_with_output(stdout: str):
    return SimpleNamespace(run_nodetool=lambda cmd: SimpleNamespace(stdout=stdout))


@pytest.mark.parametrize(
    "header, addresses",
    [
        pytest.param("Host ID                              Status", ["", "", ""], id="without-address"),
        pytest.param(
            "Host ID                              Address    Status",
            ["10.0.0.1   ", "10.0.0.2   ", "10.0.0.3   "],
            id="ipv4-address",
        ),
        pytest.param(
            "Host ID                              Address          Status",
            ["fd00::a:1        ", "fd00::a:2        ", "fd00::a:3        "],
            id="ipv6-address",
        ),
    ],
)
def test_migrate_to_tablets_status_parses_node_table(header, addresses):
    stdout = (
        "Keyspace: ks\n"
        "Status: migrating_to_tablets\n"
        "\n"
        "Nodes:\n"
        f"{header}\n"
        f"{HOST_1} {addresses[0]}uses vnodes\n"
        f"{HOST_2} {addresses[1]}migrating to tablets\n"
        f"{HOST_3} {addresses[2]}uses tablets\n"
    )

    status = get_nodetool_migrate_to_tablets_status(node_with_output(stdout), "ks")

    assert status.status == "migrating_to_tablets"
    assert status.nodes == {
        HOST_1: NodeMigrationStatus.USES_VNODES,
        HOST_2: NodeMigrationStatus.MIGRATING,
        HOST_3: NodeMigrationStatus.USES_TABLETS,
    }


def test_migrate_to_tablets_status_without_node_table():
    status = get_nodetool_migrate_to_tablets_status(node_with_output("Keyspace: ks\nStatus: vnodes\n"), "ks")

    assert status.status == "vnodes"
    assert status.nodes == {}


@pytest.mark.parametrize(
    "line",
    [
        pytest.param(f"{HOST_1} unknown state", id="without-address"),
        pytest.param(f"{HOST_1} 10.0.0.1   unknown state", id="with-address"),
    ],
)
def test_migrate_to_tablets_status_rejects_unknown_node_status(line):
    stdout = f"Keyspace: ks\nStatus: migrating_to_tablets\n\nNodes:\nHost ID  Address  Status\n{line}\n"

    with pytest.raises(ValueError, match="unknown state"):
        get_nodetool_migrate_to_tablets_status(node_with_output(stdout), "ks")
