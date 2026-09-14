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

"""
Unit tests for the stress-event classes silenced while the load is killed at the end of a
nemesis run (PerformanceRegressionTest._stop_load_when_nemesis_threads_end).

Killing the load raises a CRITICAL failure event from whichever stress tool is running, so the
severity filter must match that tool: cassandra-stress and cql-stress-cassandra-stress publish
unrelated sibling event classes.
"""

import pytest

# import the module, not the class: pytest would collect an imported unittest.TestCase subclass
import performance_regression_test as perf_regression_module
from sdcm.sct_events.loaders import CassandraStressEvent, CqlStressCassandraStressEvent

_stress_event_classes = perf_regression_module.PerformanceRegressionTest._stress_event_classes

CS_CMD = (
    "cassandra-stress write no-warmup cl=QUORUM duration=2850m -mode cql3 native "
    "-rate 'threads=250 fixed=20332/s' -col 'size=FIXED(128) n=FIXED(8)'"
)
CQL_STRESS_CMD = (
    "cql-stress-cassandra-stress write no-warmup cl=QUORUM duration=2850m "
    "-mode connectionsPerShard=250 cql3 native -rate 'threads=500 fixed=12500/s' "
    "-col 'size=FIXED(1024) n=FIXED(1)'"
)


@pytest.mark.parametrize(
    "stress_cmd, expected",
    [
        pytest.param(CS_CMD, [CassandraStressEvent], id="cassandra-stress-str"),
        pytest.param([CS_CMD], [CassandraStressEvent], id="cassandra-stress-list"),
        pytest.param(CQL_STRESS_CMD, [CqlStressCassandraStressEvent], id="cql-stress-str"),
        pytest.param([CQL_STRESS_CMD], [CqlStressCassandraStressEvent], id="cql-stress-list"),
    ],
)
def test_single_tool_returns_its_own_event_class(stress_cmd, expected):
    """A cql-stress command must not be silenced through CassandraStressEvent, and vice versa."""
    assert _stress_event_classes(stress_cmd) == expected


def test_commands_are_deduplicated():
    """The per-loader commands of one workload are the same tool - filter it once."""
    assert _stress_event_classes([CQL_STRESS_CMD] * 4) == [CqlStressCassandraStressEvent]


def test_mixed_commands_return_both_classes_in_order():
    assert _stress_event_classes([CS_CMD, CQL_STRESS_CMD]) == [
        CassandraStressEvent,
        CqlStressCassandraStressEvent,
    ]


@pytest.mark.parametrize("stress_cmd", [None, [], "", "scylla-bench -workload=sequential -mode=write"])
def test_unknown_or_empty_command_falls_back_to_cassandra_stress(stress_cmd):
    """Keep the pre-existing behaviour for anything this helper does not recognise."""
    assert _stress_event_classes(stress_cmd) == [CassandraStressEvent]
