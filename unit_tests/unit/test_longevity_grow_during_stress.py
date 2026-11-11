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

"""Where `test_custom_time` grows the cluster relative to the main load (`grow_cluster_during_stress`).

The scale test is meant to add nodes while the stress runs. By default the cluster is grown first and
the stress starts after it, which every other `cluster_target_size` user keeps.
"""

from unittest.mock import MagicMock

import pytest

from longevity_test import LongevityTest

LongevityTest.__test__ = False


class _OrderRecordingLongevityTest(LongevityTest):
    def __init__(self, params):  # skip the full ClusterTester.__init__
        self.params = params
        self.calls = []
        self.partitions_attrs = None
        self.validate_large_collections = False
        self.db_cluster = MagicMock()
        self.db_cluster.start_nemesis.side_effect = lambda: self.calls.append("nemesis")
        for name in (
            "get_nemesis_class",
            "download_artifacts_from_s3",
            "pre_create_alternator_tables",
            "run_pre_create_keyspace",
            "run_pre_create_schema",
            "kafka_configure",
            "verify_stress_thread",
        ):
            setattr(self, name, MagicMock())
        self._get_scan_operation_params = MagicMock(return_value=None)
        self._get_tombstone_gc_verification_params = MagicMock(return_value=None)

    def run_prepare_write_cmd(self):
        self.calls.append("prepare")

    def assemble_and_run_all_stress_cmd(self, stress_queue, stress_cmd, keyspace_num):
        self.calls.append("stress")

    def _grow_cluster_to_target_size(self):
        self.calls.append("grow")


def _params(**overrides):
    params = {
        "prepare_write_cmd": ["cassandra-stress write n=10"],
        "stress_cmd": ["cassandra-stress write duration=1m"],
        "keyspace_num": 1,
        "cluster_target_size": [4],
        "nemesis_during_prepare": False,
    }
    params.update(overrides)
    return params


@pytest.mark.parametrize(
    "grow_during_stress, expected_order",
    [
        pytest.param(True, ["prepare", "stress", "grow", "nemesis"], id="grow-under-stress"),
        pytest.param(False, ["prepare", "grow", "stress", "nemesis"], id="grow-before-stress"),
        pytest.param(None, ["prepare", "grow", "stress", "nemesis"], id="unset-keeps-old-order"),
    ],
)
def test_growth_order_relative_to_stress(grow_during_stress, expected_order):
    test = _OrderRecordingLongevityTest(_params(grow_cluster_during_stress=grow_during_stress))

    test.test_custom_time()

    assert test.calls == expected_order


def _cluster_with_nodes(sizes_per_dc):
    cluster = MagicMock()
    cluster.data_nodes = [MagicMock(dc_idx=dc_idx) for dc_idx, size in enumerate(sizes_per_dc) for _ in range(size)]

    def add_nodes(count, dc_idx, **_):
        new_nodes = [MagicMock(dc_idx=dc_idx) for _ in range(count)]
        cluster.data_nodes.extend(new_nodes)
        return new_nodes

    cluster.add_nodes.side_effect = add_nodes
    return cluster


def test_grow_adds_batches_until_target(monkeypatch):
    monkeypatch.setattr("longevity_test.InfoEvent", MagicMock())
    monkeypatch.setattr("longevity_test.adaptive_timeout", MagicMock())
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": [7], "add_node_cnt": 3, "n_db_nodes": [2]}
    test.db_cluster = _cluster_with_nodes([2])
    test.monitors = MagicMock()

    test._grow_cluster_to_target_size()

    # 2 -> 5 -> 7: full batches of add_node_cnt, then only what is left to reach the target
    assert [call.kwargs["count"] for call in test.db_cluster.add_nodes.call_args_list] == [3, 2]
    assert len(test.db_cluster.data_nodes) == 7
    assert test.monitors.reconfigure_scylla_monitoring.call_count == 2


def test_grow_without_target_is_noop():
    test = LongevityTest.__new__(LongevityTest)
    test.params = {"cluster_target_size": None}
    test.db_cluster = MagicMock()

    test._grow_cluster_to_target_size()

    test.db_cluster.add_nodes.assert_not_called()
