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

"""Unit tests for the tombstone-aware sstable filtering used by destroy-then-repair nemeses."""

import json
from unittest.mock import MagicMock

import pytest

from sdcm.utils.sstable.sstable_utils import SstableUtils

SSTABLE = "/var/lib/scylla/data/keyspace1/standard1-abcd/me-1-big-Data.db"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def node():
    """A mock DB node good enough for SstableUtils statistics dumping."""
    db_node = MagicMock()
    db_node.is_enterprise = True
    db_node.scylla_version = "2026.0.0"
    db_node.add_install_prefix.side_effect = lambda path: path
    return db_node


@pytest.fixture()
def sstable_utils(node):
    """SstableUtils bound to the mock ``node`` for keyspace1.standard1."""
    return SstableUtils(db_node=node, ks_cf="keyspace1.standard1")


@pytest.fixture()
def sstable_utils_with_mock_status(sstable_utils, monkeypatch):
    """SstableUtils with _get_sstables_tombstone_status mocked to mark /data/dirty-Data.db as dirty."""
    dirty = "/data/dirty-Data.db"
    monkeypatch.setattr(
        sstable_utils,
        "_get_sstables_tombstone_status",
        lambda sstables: {s: (s == dirty) for s in sstables},
    )
    return sstable_utils


# ---------------------------------------------------------------------------
# Data builders (construct return values inline - not stateful fixtures)
# ---------------------------------------------------------------------------


def result(ok=True, stdout="", stderr="", exit_status=0):
    res = MagicMock()
    res.ok = ok
    res.stdout = stdout
    res.stderr = stderr
    res.exit_status = exit_status
    return res


def batch_statistics_json(status_by_sstable):
    """Build a batched dump-statistics JSON in the exact format the Scylla tool emits.

    A dirty sstable carries a populated {local_deletion_time: cell_count} map (fractional keys are
    real - see the gemini ks1.table1 output); a clean one, written without any deletion, carries an
    empty map, as a plain cassandra-stress write load produces.

    Captured from ``scylla sstable dump-statistics`` on Scylla 2026.3 (gemini ks1.table1). This is
    the single place the tool's output format is pinned - when a Scylla version changes the format,
    update this builder, and the parsing in ``SstableUtils._dump_statistics_chunk`` with it.
    """
    return json.dumps(
        {
            "sstables": {
                sstable: {
                    "offsets": {"validation": 36, "compaction": 89, "stats": 117},
                    "validation": {"partitioner": "org.apache.cassandra.dht.Murmur3Partitioner"},
                    "compaction": {"cardinality": [255, 255, 255, 254]},
                    "stats": {
                        "min_local_deletion_time": 1787205554 if dirty else 2147483647,
                        "max_local_deletion_time": 2147483647,
                        "min_ttl": 0,
                        "max_ttl": 0,
                        "estimated_tombstone_drop_time": (
                            {"1787205554.4691358": 81, "1787205580": 48} if dirty else {}
                        ),
                    },
                    "serialization_header": {"static_columns": [], "regular_columns": []},
                }
                for sstable, dirty in status_by_sstable.items()
            }
        }
    )


@pytest.mark.parametrize(
    "input_sstables, expected_output",
    [
        pytest.param(["/data/clean-Data.db", "/data/dirty-Data.db"], ["/data/clean-Data.db"], id="mixed"),
        pytest.param(["/data/dirty-Data.db"], [], id="all-dirty"),
        pytest.param(["/data/clean-Data.db"], ["/data/clean-Data.db"], id="all-clean"),
        pytest.param([], [], id="empty"),
    ],
)
def test_filter_out_sstables_with_tombstones(sstable_utils_with_mock_status, input_sstables, expected_output):
    """Verify filter_out_sstables_with_tombstones removes dirty sstables and preserves clean ones."""
    assert sstable_utils_with_mock_status.filter_out_sstables_with_tombstones(input_sstables) == expected_output


# ---------------------------------------------------------------------------
# _get_sstables_tombstone_status (batched path)
# ---------------------------------------------------------------------------


def test_batch_status_single_process_for_many_sstables(node, sstable_utils):
    """Verify batched tombstone detection invokes a single dump-statistics process for multiple sstables."""
    clean = ["/data/c1-Data.db", "/data/c2-Data.db"]
    dirty = ["/data/d1-Data.db"]
    node.remoter.run.return_value = result(
        ok=True,
        stdout=batch_statistics_json({clean[0]: False, clean[1]: False, dirty[0]: True}),
    )

    status = sstable_utils._get_sstables_tombstone_status(clean + dirty)

    assert status == {clean[0]: False, clean[1]: False, dirty[0]: True}
    node.remoter.run.assert_called_once_with(
        'sudo bash -c "SCYLLA_CONF=/etc/scylla /usr/bin/scylla sstable dump-statistics '
        "--keyspace keyspace1 --table standard1 --sstables "
        '/data/c1-Data.db /data/c2-Data.db /data/d1-Data.db"',
        verbose=False,
    )


def test_batch_status_chunks_respect_batch_size(monkeypatch, node, sstable_utils):
    """Verify batched tombstone detection splits large sstable lists into chunks of SSTABLE_DUMP_BATCH_SIZE."""
    monkeypatch.setattr(type(sstable_utils), "SSTABLE_DUMP_BATCH_SIZE", 2)
    sstables = [f"/data/{i}-Data.db" for i in range(5)]
    node.remoter.run.side_effect = lambda cmd, **kw: result(
        ok=True, stdout=batch_statistics_json({s: False for s in sstables if s in cmd})
    )

    status = sstable_utils._get_sstables_tombstone_status(sstables)

    assert status == {s: False for s in sstables}
    assert node.remoter.run.call_count == 3


def test_batch_status_missing_entry_raises(node, sstable_utils):
    """Verify an sstable absent from the dump output raises instead of being silently classified.

    The check runs offline (Scylla stopped on the node), so a missing entry can only be a tooling or
    code problem - never a benign compaction race - and must not be swallowed.
    """
    present = "/data/present-Data.db"
    missing = "/data/missing-Data.db"
    node.remoter.run.return_value = result(ok=True, stdout=batch_statistics_json({present: False}))

    with pytest.raises(KeyError):
        sstable_utils._get_sstables_tombstone_status([present, missing])


@pytest.mark.parametrize(
    "stdout, expected_exception",
    [
        pytest.param("not-json", json.JSONDecodeError, id="bad-json"),
        pytest.param(json.dumps({"unexpected": {}}), KeyError, id="no-sstables-wrapper"),
        pytest.param(json.dumps({"sstables": {SSTABLE: {}}}), KeyError, id="stats-section-absent"),
        pytest.param(json.dumps({"sstables": {SSTABLE: {"stats": {}}}}), KeyError, id="histogram-field-absent"),
    ],
)
def test_batch_status_raises_on_unusable_dump(node, sstable_utils, stdout, expected_exception):
    """Verify unusable dump-statistics output fails loudly instead of degrading to 'has tombstones'."""
    node.remoter.run.return_value = result(ok=True, stdout=stdout)

    with pytest.raises(expected_exception):
        sstable_utils._get_sstables_tombstone_status([SSTABLE])


# ---------------------------------------------------------------------------
# get_sstables
# ---------------------------------------------------------------------------

TABLE_DIR = "/var/lib/scylla/data/keyspace1/standard1-abcd"


def test_get_sstables_skips_sstables_without_toc(node, sstable_utils):
    """Verify an incomplete sstable, with no TOC.txt, is left out.

    A Scylla stopped in the middle of writing or deleting an sstable leaves it without TOC.txt,
    and the "scylla sstable" tool fails on it (SCT-1045).
    """
    node.remoter.sudo.return_value = result(
        stdout="\n".join(
            [
                f"{TABLE_DIR}/me-1-big-Data.db",
                f"{TABLE_DIR}/me-1-big-TOC.txt",
                f"{TABLE_DIR}/me-2-big-Data.db",
                f"{TABLE_DIR}/me-3-big-TOC.txt",
                f"{TABLE_DIR}/me-3-big-Data.db",
            ]
        )
    )

    assert sstable_utils.get_sstables() == [f"{TABLE_DIR}/me-1-big-Data.db", f"{TABLE_DIR}/me-3-big-Data.db"]
    node.remoter.sudo.assert_called_once_with(
        "find /var/lib/scylla/data/keyspace1/standard1-*/ -maxdepth 1 -type f"
        r" \( -name '*-big-Data.db' -o -name '*-big-TOC.txt' \)",
        verbose=True,
        ignore_status=True,
    )


def test_get_sstables_filters_by_time_only_data_files(node, sstable_utils):
    """Verify the time filter applies to the Data.db files, so an old TOC.txt still counts."""
    node.remoter.sudo.return_value = result(stdout=f"{TABLE_DIR}/me-1-big-Data.db\n{TABLE_DIR}/me-1-big-TOC.txt")

    assert sstable_utils.get_sstables(from_minutes_ago=30) == [f"{TABLE_DIR}/me-1-big-Data.db"]
    assert r"\( -name '*-big-Data.db' -cmin -30 -o -name '*-big-TOC.txt' \)" in node.remoter.sudo.call_args.args[0]


def test_get_sstables_returns_nothing_on_find_error(node, sstable_utils):
    """Verify a failed find, e.g. for a table without a data directory, returns no sstables."""
    node.remoter.sudo.return_value = result(
        ok=False, stdout="", stderr="find: '/var/lib/scylla/data/keyspace1/standard1-*/': No such file or directory"
    )

    assert sstable_utils.get_sstables() == []
