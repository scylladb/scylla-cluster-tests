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
from cassandra import InvalidRequest

from sdcm.exceptions import KillNemesis
from sdcm.utils.tablets.common import temporarily_disable_auto_repair

MODULE = "sdcm.utils.tablets.common"
DISABLE_STATEMENT = "ALTER CLUSTER WITH auto_repair_enabled = false"
RESTORE_STATEMENT = "ALTER CLUSTER WITH auto_repair_enabled = 'true'"


@pytest.fixture()
def cql_session():
    """CQL session that reads auto-repair as enabled at CLUSTER scope and records the statements sent."""
    session = MagicMock()
    session.execute.return_value.one.return_value = ("true",)
    return session


@pytest.fixture()
def node(cql_session, monkeypatch):
    """Node whose cluster hands out the CQL session; drain-wait is patched out (not under test here)."""
    node = MagicMock()
    node.parent_cluster.cql_connection_patient.return_value.__enter__.return_value = cql_session
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", lambda *args, **kwargs: True)
    return node


def test_temporarily_disable_auto_repair_disables_then_restores(node, cql_session):
    """Auto-repair must be off while the block runs and back on afterwards.

    A missed restore would leave the whole test running without auto-repair (SCT-905).
    """
    with temporarily_disable_auto_repair(node):
        cql_session.execute.assert_called_with(DISABLE_STATEMENT)
    cql_session.execute.assert_called_with(RESTORE_STATEMENT)


def test_temporarily_disable_auto_repair_restores_when_body_raises(node, cql_session):
    """A failing repair must not leave auto-repair disabled for the rest of the test."""
    with pytest.raises(ValueError, match="repair failed"), temporarily_disable_auto_repair(node):
        raise ValueError("repair failed")
    cql_session.execute.assert_called_with(RESTORE_STATEMENT)


def test_temporarily_disable_auto_repair_restores_when_killed_during_drain(node, cql_session, monkeypatch):
    """A nemesis killed while it waits for in-flight repairs must still restore auto-repair.

    The end of a test stops the nemesis thread with KillNemesis wherever it is, and the cluster
    would otherwise stay with auto-repair off.
    """
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", MagicMock(side_effect=KillNemesis))
    with pytest.raises(KillNemesis):
        with temporarily_disable_auto_repair(node):
            pytest.fail("the repair must not start after a kill")
    cql_session.execute.assert_called_with(RESTORE_STATEMENT)


@pytest.mark.parametrize(
    "row",
    [
        pytest.param(None, id="nothing-stored"),
        pytest.param(("false",), id="already-disabled-or-nested-use"),
    ],
)
def test_temporarily_disable_auto_repair_noop(node, cql_session, row):
    """Nothing may be written when auto-repair is not enabled.

    A nested use would otherwise re-enable auto-repair on exit, under the outer repair.
    """
    cql_session.execute.return_value.one.return_value = row
    with temporarily_disable_auto_repair(node):
        pass
    cql_session.execute.assert_called_once()


def test_temporarily_disable_auto_repair_noop_without_cluster_config(node, cql_session):
    """The repair must run with auto-repair left as is on releases without CQL cluster configuration.

    Master SCT still runs Manager jobs against such releases.
    """
    cql_session.execute.side_effect = InvalidRequest("unconfigured table scylla_clusters")
    with temporarily_disable_auto_repair(node):
        pass
    cql_session.execute.assert_called_once()
