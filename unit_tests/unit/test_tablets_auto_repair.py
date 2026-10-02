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

from unittest.mock import MagicMock, call

import pytest

from sdcm.utils.tablets.common import AUTO_REPAIR_PARAM, temporarily_disable_auto_repair

MODULE = "sdcm.utils.tablets.common"


@pytest.fixture()
def db_cluster(monkeypatch):
    """Three-node cluster with auto-repair enabled; drain-wait is patched out (not under test here)."""
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(name=f"node-{i}") for i in range(3)]
    for node in cluster.data_nodes:
        node.get_scylla_config_param.return_value = "true"
        node.set_scylla_config_param.return_value = True
    monkeypatch.setattr(f"{MODULE}.wait_no_active_repair_tasks", lambda *args, **kwargs: True)
    return cluster


def test_temporarily_disable_auto_repair_disables_then_restores(db_cluster):
    """Auto-repair must be off while the block runs and back on afterwards, on every node —
    a missed restore would leave the whole test running without auto-repair (SCT-905).
    The value must also be persisted to scylla.yaml, or a node restart/config reload inside
    the block would silently re-enable auto-repair."""
    with temporarily_disable_auto_repair(db_cluster):
        for node in db_cluster.data_nodes:
            node.set_scylla_config_param.assert_called_once_with(AUTO_REPAIR_PARAM, "false")
            assert node.remote_scylla_yaml().__enter__().auto_repair_enabled_default is False
    for node in db_cluster.data_nodes:
        assert node.set_scylla_config_param.call_args_list == [
            call(AUTO_REPAIR_PARAM, "false"),
            call(AUTO_REPAIR_PARAM, "true"),
        ]
        assert node.remote_scylla_yaml().__enter__().auto_repair_enabled_default is True


def test_temporarily_disable_auto_repair_restores_when_body_raises(db_cluster):
    """A failing repair must not leave auto-repair disabled for the rest of the test."""
    with pytest.raises(ValueError, match="repair failed"), temporarily_disable_auto_repair(db_cluster):
        raise ValueError("repair failed")
    for node in db_cluster.data_nodes:
        node.set_scylla_config_param.assert_called_with(AUTO_REPAIR_PARAM, "true")


@pytest.mark.parametrize(
    "config_value",
    [
        pytest.param(None, id="option-absent-on-old-scylla"),
        pytest.param("false", id="already-disabled"),
    ],
)
def test_temporarily_disable_auto_repair_noop(db_cluster, config_value):
    """Nothing may be written when auto-repair is off or unsupported — a blind write would fail
    on pre-2026.1 Scylla and would wrongly enable auto-repair on exit."""
    for node in db_cluster.data_nodes:
        node.get_scylla_config_param.return_value = config_value
    with temporarily_disable_auto_repair(db_cluster):
        pass
    for node in db_cluster.data_nodes:
        node.set_scylla_config_param.assert_not_called()


def test_temporarily_disable_auto_repair_nested_toggles_once(db_cluster):
    """A nested or sibling (parallel-nemesis) use exiting first must not re-enable auto-repair
    while the other is still repairing — only the outermost use disables and restores."""
    with temporarily_disable_auto_repair(db_cluster), temporarily_disable_auto_repair(db_cluster):
        pass
    for node in db_cluster.data_nodes:
        assert node.set_scylla_config_param.call_args_list == [
            call(AUTO_REPAIR_PARAM, "false"),
            call(AUTO_REPAIR_PARAM, "true"),
        ]


def test_temporarily_disable_auto_repair_skips_unreachable_node(db_cluster):
    """A down/isolated node (remove-node and isolation nemeses) must not fail the repair, and
    no restore may be attempted on it."""
    down_node, *up_nodes = db_cluster.data_nodes
    down_node.get_scylla_config_param.return_value = None
    with temporarily_disable_auto_repair(db_cluster):
        pass
    down_node.set_scylla_config_param.assert_not_called()
    for node in up_nodes:
        assert node.set_scylla_config_param.call_args_list == [
            call(AUTO_REPAIR_PARAM, "false"),
            call(AUTO_REPAIR_PARAM, "true"),
        ]
