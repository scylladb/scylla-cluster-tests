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

import re
from functools import partial
from unittest.mock import MagicMock

import pytest
from cassandra import InvalidRequest, OperationTimedOut

from sdcm.utils.tablets.common import (
    AUTO_REPAIR_DRAIN_TIMEOUT,
    AUTO_REPAIR_OPTION,
    temporarily_disable_auto_repair,
)

MODULE = "sdcm.utils.tablets.common"
DISABLE = f"ALTER CLUSTER WITH {AUTO_REPAIR_OPTION} = false"
RESTORE = f"ALTER CLUSTER WITH {AUTO_REPAIR_OPTION} = true"


def run_cluster_config_cql(cluster_configs, rejected_statements, statement):
    """Mimic system_schema.scylla_clusters: ALTER CLUSTER changes what the SELECT reads back, unless
    the statement is still due to be rejected per ``rejected_statements``."""
    if rejected_statements.get(statement):
        rejected_statements[statement] -= 1
        raise OperationTimedOut(f"rejected: {statement}")
    if statement.startswith("SELECT"):
        result = MagicMock()
        result.one.return_value = MagicMock(configs=dict(cluster_configs)) if cluster_configs else None
        return result
    option, value = re.fullmatch(r"ALTER CLUSTER WITH (\w+) = (\w+)", statement).groups()
    cluster_configs[option] = value
    return MagicMock()


@pytest.fixture()
def cluster_configs():
    """Options stored at CLUSTER scope; empty means nothing is stored there."""
    return {}


@pytest.fixture()
def rejected_statements():
    """How many more times the session rejects a statement; empty means it accepts everything."""
    return {}


@pytest.fixture()
def drain_wait(monkeypatch):
    """Drain-wait patched out (not under test here), always reporting the repair tasks drained."""
    wait = MagicMock(return_value=True)
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", wait)
    return wait


@pytest.fixture()
def db_cluster(monkeypatch, drain_wait, cluster_configs, rejected_statements):
    """Three-node cluster with CQL cluster configuration, retrying a failed restore without a pause."""
    monkeypatch.setattr(f"{MODULE}.AUTO_REPAIR_RESTORE_STEP", 0)
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(name=f"node-{i}") for i in range(3)]
    session = cluster.cql_connection_patient.return_value.__enter__.return_value
    session.execute.side_effect = partial(run_cluster_config_cql, cluster_configs, rejected_statements)
    return cluster


@pytest.fixture()
def db_cluster_without_cluster_config(db_cluster):
    """``db_cluster`` on a Scylla release without CQL cluster configuration."""
    session = db_cluster.cql_connection_patient.return_value.__enter__.return_value
    session.execute.side_effect = InvalidRequest("unconfigured table scylla_clusters")
    return db_cluster


def alter_cluster_statements(db_cluster):
    session = db_cluster.cql_connection_patient.return_value.__enter__.return_value
    return [args[0] for args, _ in session.execute.call_args_list if args[0].startswith("ALTER")]


def test_cluster_scope_disables_then_restores(db_cluster, cluster_configs, drain_wait):
    """Auto-repair must be off at CLUSTER scope while the block runs, with in-flight repairs drained
    first, and back on afterwards — a missed restore would leave the whole test without auto-repair."""
    cluster_configs[AUTO_REPAIR_OPTION] = "true"
    with temporarily_disable_auto_repair(db_cluster):
        assert cluster_configs[AUTO_REPAIR_OPTION] == "false"
    assert cluster_configs == {AUTO_REPAIR_OPTION: "true"}
    assert alter_cluster_statements(db_cluster) == [DISABLE, RESTORE]
    drain_wait.assert_called_once_with(db_cluster.data_nodes, timeout=AUTO_REPAIR_DRAIN_TIMEOUT)


def test_cluster_scope_restores_when_body_raises(db_cluster, cluster_configs):
    """A failing repair must not leave auto-repair disabled at CLUSTER scope for the rest of the test."""
    cluster_configs[AUTO_REPAIR_OPTION] = "true"
    with pytest.raises(ValueError, match="repair failed"), temporarily_disable_auto_repair(db_cluster):
        raise ValueError("repair failed")
    assert cluster_configs == {AUTO_REPAIR_OPTION: "true"}


def test_cluster_scope_disable_failure_raises(db_cluster, cluster_configs, rejected_statements, drain_wait):
    """The repair must not run alongside auto-repair (SCT-905): when the disable is rejected, the
    block must not run, and nothing is left to restore."""
    cluster_configs[AUTO_REPAIR_OPTION] = "true"
    rejected_statements[DISABLE] = 1
    body = MagicMock()
    with pytest.raises(OperationTimedOut, match="rejected"), temporarily_disable_auto_repair(db_cluster):
        body()
    body.assert_not_called()
    drain_wait.assert_not_called()
    assert alter_cluster_statements(db_cluster) == [DISABLE]
    assert cluster_configs == {AUTO_REPAIR_OPTION: "true"}


def test_cluster_scope_restore_retried(db_cluster, cluster_configs, rejected_statements):
    """A rejected restore must be retried: each later use reads false and skips the disable, so
    nothing else would re-enable auto-repair for the rest of the test."""
    cluster_configs[AUTO_REPAIR_OPTION] = "true"
    rejected_statements[RESTORE] = 2
    with temporarily_disable_auto_repair(db_cluster):
        pass
    assert alter_cluster_statements(db_cluster) == [DISABLE, RESTORE, RESTORE, RESTORE]
    assert cluster_configs == {AUTO_REPAIR_OPTION: "true"}


@pytest.mark.parametrize(
    "stored_configs",
    [
        pytest.param({AUTO_REPAIR_OPTION: "false"}, id="disabled-at-cluster-scope-or-nested-use"),
        pytest.param({}, id="nothing-stored"),
    ],
)
def test_cluster_scope_noop(db_cluster, cluster_configs, stored_configs):
    """Nothing may be stored when auto-repair is not enabled at CLUSTER scope — a stored false would
    outlive the block, and an inner nested use restoring true would re-enable it under the outer repair."""
    cluster_configs.update(stored_configs)
    with temporarily_disable_auto_repair(db_cluster):
        pass
    assert alter_cluster_statements(db_cluster) == []
    assert cluster_configs == stored_configs


def test_noop_without_cluster_config(db_cluster_without_cluster_config, drain_wait):
    """Master SCT still runs Manager jobs against releases without CQL cluster configuration: the
    repair must run with auto-repair left as is, instead of failing on the missing table."""
    with temporarily_disable_auto_repair(db_cluster_without_cluster_config):
        pass
    assert alter_cluster_statements(db_cluster_without_cluster_config) == []
    drain_wait.assert_not_called()
