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

from unittest.mock import MagicMock

import pytest

from sdcm.exceptions import RaftTopologyCoordinatorNotFound
from sdcm.utils.raft import common

LEADER_HOST_ID = "11111111-1111-1111-1111-111111111111"
OTHER_HOST_ID = "22222222-2222-2222-2222-222222222222"
NULL_HOST_ID = "00000000-0000-0000-0000-000000000000"


def _make_node(name: str, host_id: str) -> MagicMock:
    node = MagicMock()
    node.name = name
    node.host_id = host_id
    return node


def _make_verification_node(active_nodes: list[MagicMock]) -> MagicMock:
    node = MagicMock()
    node.name = "verification-node"
    node.parent_cluster.get_nodes_up_and_normal.return_value = active_nodes
    return node


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    # Speed up retrying(n=3) failures.
    monkeypatch.setattr("sdcm.utils.decorators.time.sleep", lambda *_, **__: None)


@pytest.fixture
def storage_service_client_mock(monkeypatch):
    """Patch StorageServiceClient to return each node's host id as JSON-quoted stdout."""

    def _factory(active_node):
        client = MagicMock()
        client.get_local_hostid.return_value.stdout = f'"{active_node.host_id}"'
        return client

    mock_cls = MagicMock(side_effect=_factory)
    monkeypatch.setattr(common, "StorageServiceClient", mock_cls)
    return mock_cls


@pytest.fixture
def raft_leader_host_mock(monkeypatch):
    """Patch RaftApi and return get_group0_leader_host_id mock for setup/assertions."""
    raft_api_cls = MagicMock()
    monkeypatch.setattr(common, "RaftApi", raft_api_cls)
    return raft_api_cls.return_value.get_group0_leader_host_id


def test_returns_node_matching_group0_leader(raft_leader_host_mock, storage_service_client_mock):
    leader_node = _make_node("node-1", LEADER_HOST_ID)
    other_node = _make_node("node-2", OTHER_HOST_ID)
    verification_node = _make_verification_node([other_node, leader_node])
    raft_leader_host_mock.return_value = f'"{LEADER_HOST_ID}"'

    result = common.get_topology_coordinator_node(verification_node)

    assert result is leader_node, (
        f"Expected the node whose host id matches the group0 leader ({LEADER_HOST_ID}), got {result}"
    )
    raft_leader_host_mock.assert_called_once_with()
    verification_node.parent_cluster.get_nodes_up_and_normal.assert_called_once_with(verification_node)
    assert storage_service_client_mock.call_count == 2, (
        f"Expected host id to be resolved for both active nodes, got {storage_service_client_mock.call_count} call(s)"
    )


@pytest.mark.parametrize(
    ("leader_host_id", "error_match"),
    [
        # Scylla serializes an unknown leader (election/stepdown) as the nil UUID, not "".
        pytest.param(f'"{NULL_HOST_ID}"', "unknown", id="nil-uuid-leader-host"),
        pytest.param('""', "unknown", id="empty-leader-host"),
        pytest.param(f'"{LEADER_HOST_ID}"', "not found among active nodes", id="leader-not-in-active-nodes"),
    ],
)
def test_raises_when_leader_unusable_or_missing_in_active_nodes(
    raft_leader_host_mock,
    storage_service_client_mock,
    leader_host_id,
    error_match,
):
    verification_node = _make_verification_node([_make_node("node-1", OTHER_HOST_ID)])
    raft_leader_host_mock.return_value = leader_host_id

    with pytest.raises(RaftTopologyCoordinatorNotFound, match=error_match):
        common.get_topology_coordinator_node(verification_node)

    # Retry wrapper re-runs three times for RaftTopologyCoordinatorNotFound.
    assert raft_leader_host_mock.call_count == 3, (
        f"Expected the @retrying(n=3) wrapper to query the leader host 3 times, "
        f"got {raft_leader_host_mock.call_count} call(s)"
    )
    # Validate we no longer use the old CQL history lookup code path.
    verification_node.parent_cluster.cql_connection_patient.assert_not_called()
