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

from invoke import Result

from sdcm.cluster import BaseCluster
from unit_tests.lib.fake_cluster import DummyScyllaCluster

# trimmed 'nodetool cfstats <ks>' output of Scylla 2026.2 (Java nodetool of 5.4/2024.1 prints the same lines)
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


def test_parse_cfstats_partitions():
    output = CFSTATS_HEADER + CFSTATS_BY_KEYSPACE["ks1"] + CFSTATS_BY_KEYSPACE["ks2"]

    assert BaseCluster._parse_cfstats_partitions(output) == {
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


def test_get_any_ks_cf_list_runs_cfstats_once_per_keyspace():
    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.side_effect = lambda sub_cmd, args, **_: Result(
        stdout=CFSTATS_HEADER + CFSTATS_BY_KEYSPACE[args]
    )

    assert _get_any_ks_cf_list(db_node) == ['ks1."MixedCase"', "ks1.mv1", "ks1.t1", "ks1.t1_v_idx_index", "ks2.t2"]
    assert sorted((call.kwargs["sub_cmd"], call.kwargs["args"]) for call in db_node.run_nodetool.call_args_list) == [
        ("cfstats", "ks1"),
        ("cfstats", "ks2"),
    ]


def test_get_any_ks_cf_list_drops_tables_of_keyspace_whose_cfstats_failed():
    def run_nodetool(sub_cmd, args, **_):
        if args == "ks2":
            raise TimeoutError("nodetool timed out")
        return Result(stdout=CFSTATS_HEADER + CFSTATS_BY_KEYSPACE[args])

    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.side_effect = run_nodetool

    assert _get_any_ks_cf_list(db_node) == ['ks1."MixedCase"', "ks1.mv1", "ks1.t1", "ks1.t1_v_idx_index"]


def test_get_any_ks_cf_list_cfstats_timeout_scales_with_keyspace_tables():
    db_node = unittest.mock.MagicMock()
    db_node.run_nodetool.return_value = Result(stdout=CFSTATS_HEADER)
    tables = CFSTATS_TABLES + [("big_ks", f"t{i}") for i in range(5000)]

    _get_any_ks_cf_list(db_node, tables)

    assert {call.kwargs["args"]: call.kwargs["timeout"] for call in db_node.run_nodetool.call_args_list} == {
        "ks1": 300,
        "ks2": 300,
        "big_ks": 500,
    }
