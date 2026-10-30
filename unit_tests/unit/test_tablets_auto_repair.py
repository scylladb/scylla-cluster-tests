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
from cassandra import InvalidRequest

from sdcm.utils.tablets.common import (
    AUTO_REPAIR_DRAIN_TIMEOUT,
    AUTO_REPAIR_OPTION,
    temporarily_disable_auto_repair,
)

MODULE = "sdcm.utils.tablets.common"


def run_cluster_config_cql(cluster_configs, statement):
    """Mimic system_schema.scylla_clusters: ALTER CLUSTER changes what the SELECT reads back."""
    if statement.startswith("SELECT"):
        result = MagicMock()
        result.one.return_value = MagicMock(configs=dict(cluster_configs)) if cluster_configs else None
        return result
    option, value = re.fullmatch(r"ALTER CLUSTER WITH (\w+) = (\w+)", statement).groups()
    if value == "NULL":
        cluster_configs.pop(option, None)
    else:
        cluster_configs[option] = value
    return MagicMock()


@pytest.fixture()
def cluster_configs():
    """Options stored at CLUSTER scope; empty means nothing is stored there."""
    return {}


@pytest.fixture()
def drain_wait(monkeypatch):
    """Drain-wait patched out (not under test here), always reporting the repair tasks drained."""
    wait = MagicMock(return_value=True)
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", wait)
    return wait


@pytest.fixture()
def db_cluster(drain_wait, cluster_configs):
    """Three-node cluster with CQL cluster configuration and auto-repair enabled in scylla.yaml."""
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(name=f"node-{i}") for i in range(3)]
    for node in cluster.data_nodes:
        node.get_scylla_config_param.return_value = "true"
    session = cluster.cql_connection_patient.return_value.__enter__.return_value
    session.execute.side_effect = partial(run_cluster_config_cql, cluster_configs)
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


@pytest.mark.parametrize(
    ("stored_configs", "restore_statement"),
    [
        pytest.param(
            {AUTO_REPAIR_OPTION: "true"},
            f"ALTER CLUSTER WITH {AUTO_REPAIR_OPTION} = true",
            id="enabled-at-cluster-scope",
        ),
        pytest.param({}, f"ALTER CLUSTER WITH {AUTO_REPAIR_OPTION} = NULL", id="enabled-by-scylla-yaml"),
    ],
)
def test_cluster_scope_disables_then_restores(
    db_cluster, cluster_configs, drain_wait, stored_configs, restore_statement
):
    """Auto-repair must be off at CLUSTER scope while the block runs and the stored value restored
    afterwards: NULL when only scylla.yaml enabled it, which ALTER CLUSTER overrides on every node,
    and the system.config path must not be used, since MS2 ignores it once a CQL scope stores a value."""
    cluster_configs.update(stored_configs)
    with temporarily_disable_auto_repair(db_cluster):
        assert cluster_configs[AUTO_REPAIR_OPTION] == "false"
    assert cluster_configs == stored_configs
    assert alter_cluster_statements(db_cluster) == [
        f"ALTER CLUSTER WITH {AUTO_REPAIR_OPTION} = false",
        restore_statement,
    ]
    drain_wait.assert_called_once_with(db_cluster.data_nodes, timeout=AUTO_REPAIR_DRAIN_TIMEOUT)
    for node in db_cluster.data_nodes:
        node.set_scylla_config_param.assert_not_called()


def test_cluster_scope_restores_when_body_raises(db_cluster, cluster_configs):
    """A failing repair must not leave auto-repair disabled at CLUSTER scope for the rest of the test."""
    cluster_configs[AUTO_REPAIR_OPTION] = "true"
    with pytest.raises(ValueError, match="repair failed"), temporarily_disable_auto_repair(db_cluster):
        raise ValueError("repair failed")
    assert cluster_configs == {AUTO_REPAIR_OPTION: "true"}


@pytest.mark.parametrize(
    ("stored_configs", "yaml_value"),
    [
        pytest.param({AUTO_REPAIR_OPTION: "false"}, "true", id="disabled-at-cluster-scope-or-nested-use"),
        pytest.param({}, "false", id="disabled-in-scylla-yaml"),
    ],
)
def test_cluster_scope_noop(db_cluster, cluster_configs, stored_configs, yaml_value):
    """Nothing may be stored when auto-repair is already off — a stored false would outlive the
    block, and an inner nested use restoring true would re-enable it under the outer repair."""
    cluster_configs.update(stored_configs)
    for node in db_cluster.data_nodes:
        node.get_scylla_config_param.return_value = yaml_value
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
    for node in db_cluster_without_cluster_config.data_nodes:
        node.set_scylla_config_param.assert_not_called()
