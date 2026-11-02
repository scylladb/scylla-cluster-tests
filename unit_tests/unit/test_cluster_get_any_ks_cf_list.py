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

"""Tests for BaseCluster.get_any_ks_cf_list empty-table filtering."""

import unittest.mock
from types import SimpleNamespace

import pytest
from invoke import Result

from sdcm.cluster import BaseCluster
from unit_tests.lib.fake_cluster import DummyScyllaCluster

# trimmed 'nodetool cfstats' output of Scylla 2026.2 (Java nodetool of 5.4/2024.1 prints the same lines)
CFSTATS_HEADER = "Total number of tables: 98\n----------------\n"
CFSTATS_BY_KEYSPACE = {
    "ks1": """\
Keyspace : ks1
\tRead Count: 18
\tRead Latency: 0.000000000000000E+00 ms
\tWrite Count: 7
\tWrite Latency: 0.000000000000000E+00 ms
\tPending Flushes: 0
\t\tTable: t1_v_idx_index
\t\tSSTable count: 1
\t\tSpace used (live): 5700
\t\tNumber of partitions (estimate): 2
\t\tDropped Mutations: 0

\t\tTable: MixedCase
\t\tSSTable count: 1
\t\tSpace used (live): 5525
\t\tNumber of partitions (estimate): 1
\t\tDropped Mutations: 0

\t\tTable: t1
\t\tSSTable count: 1
\t\tSpace used (live): 5543
\t\tNumber of partitions (estimate): 2
\t\tDropped Mutations: 0

\t\tTable: mv1
\t\tSSTable count: 1
\t\tSpace used (live): 5602
\t\tNumber of partitions (estimate): 2
\t\tDropped Mutations: 0

\t\tTable: empty_t
\t\tSSTable count: 0
\t\tSpace used (live): 0
\t\tNumber of partitions (estimate): 0
\t\tDropped Mutations: 0

----------------
""",
    "ks2": """\
Keyspace : ks2
\tRead Count: 17
\tRead Latency: 0.000000000000000E+00 ms
\tWrite Count: 1
\tWrite Latency: 0.000000000000000E+00 ms
\tPending Flushes: 0
\t\tTable: t2
\t\tSSTable count: 1
\t\tSpace used (live): 5518
\t\tNumber of partitions (estimate): 1
\t\tDropped Mutations: 0

\t\tTable: empty_t2
\t\tSSTable count: 0
\t\tSpace used (live): 0
\t\tNumber of partitions (estimate): 0
\t\tDropped Mutations: 0

----------------
""",
}

CFSTATS_TABLES = [("ks1", "t1"), ("ks1", "t1_v_idx_index"), ("ks1", "MixedCase"), ("ks1", "mv1"), ("ks1", "empty_t")]
CFSTATS_TABLES += [("ks2", "t2"), ("ks2", "empty_t2")]
CFSTATS_ALL = CFSTATS_HEADER + CFSTATS_BY_KEYSPACE["ks1"] + CFSTATS_BY_KEYSPACE["ks2"]


def test_parse_cfstats_partitions():
    assert BaseCluster._parse_cfstats_partitions(CFSTATS_ALL) == {
        "ks1.t1_v_idx_index": 2,
        "ks1.MixedCase": 1,
        "ks1.t1": 2,
        "ks1.mv1": 2,
        "ks1.empty_t": 0,
        "ks2.t2": 1,
        "ks2.empty_t2": 0,
    }


def _get_any_ks_cf_list(db_node, tables=CFSTATS_TABLES):
    # system_schema.columns returns a row per column, MVs and index views included
    rows = [
        SimpleNamespace(keyspace_name=ks, table_name=table, type=col_type)
        for ks, table in tables
        for col_type in ("int", "text")
    ]
    db_cluster = DummyScyllaCluster([])
    db_cluster.cql_connection_patient = unittest.mock.MagicMock()
    with unittest.mock.patch("sdcm.cluster.PageFetcher") as page_fetcher:
        page_fetcher.return_value.request_all.return_value.all_data.return_value = rows
        return sorted(db_cluster.get_any_ks_cf_list(db_node))


def test_get_any_ks_cf_list_runs_cfstats_once():
    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.return_value = Result(stdout=CFSTATS_ALL)

    assert _get_any_ks_cf_list(db_node) == ['ks1."MixedCase"', "ks1.mv1", "ks1.t1", "ks1.t1_v_idx_index", "ks2.t2"]
    db_node.run_nodetool.assert_called_once()
    assert db_node.run_nodetool.call_args.kwargs["sub_cmd"] == "cfstats"
    assert "args" not in db_node.run_nodetool.call_args.kwargs


@pytest.mark.parametrize(
    "run_nodetool",
    [
        pytest.param({"side_effect": TimeoutError("nodetool timed out")}, id="call-failed"),
        pytest.param({"return_value": Result(stdout=CFSTATS_HEADER)}, id="no-estimates-in-output"),
    ],
)
def test_get_any_ks_cf_list_keeps_all_tables_when_cfstats_failed(run_nodetool):
    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.configure_mock(**run_nodetool)

    assert _get_any_ks_cf_list(db_node) == [
        'ks1."MixedCase"',
        "ks1.empty_t",
        "ks1.mv1",
        "ks1.t1",
        "ks1.t1_v_idx_index",
        "ks2.empty_t2",
        "ks2.t2",
    ]


def test_get_any_ks_cf_list_cfstats_timeout_scales_with_tables():
    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.return_value = Result(stdout=CFSTATS_ALL)

    _get_any_ks_cf_list(db_node)
    assert db_node.run_nodetool.call_args.kwargs["timeout"] == 300

    _get_any_ks_cf_list(db_node, [(f"ks{i}", "t") for i in range(5000)])
    assert db_node.run_nodetool.call_args.kwargs["timeout"] == 500
