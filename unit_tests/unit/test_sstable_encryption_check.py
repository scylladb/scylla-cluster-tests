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

"""Unit tests for SstableUtils.check_that_sstables_are_encrypted read-failure classification."""

from unittest.mock import MagicMock

import pytest
from invoke import Result

from sdcm.exceptions import SstablesNotFound
from sdcm.utils.sstable import sstable_utils
from sdcm.utils.sstable.sstable_utils import SstableUtils

TABLE_DIR = "/var/lib/scylla/data/keyspace1/standard1-0db27f60c5cf11f184e040f9a6e8c2bb"
ENCRYPTED_SSTABLE = f"{TABLE_DIR}/mt-3h4l_0219_5lwio2r3i5kiqk479z-big-Data.db"
FAILED_SSTABLE = f"{TABLE_DIR}/mt-3h4l_0218_208b42ahjsml3d91p3-big-Data.db"

ENCRYPTED_STDERR = (
    "[shard 0:main] seastar - Exiting on unhandled exception: sstables::malformed_sstable_exception "
    "(Buffer improperly sized to hold requested data. Got: 2. Expected: 4 in sstable "
    f"{TABLE_DIR}/mt-3h4l_0219_5lwio2r3i5kiqk479z-big-Statistics.db)"
)
# Real stderr from longevity-100gb-4h-test build 1536 (Argus run 7473ad4d), Scylla 2026.4.0~dev
DELETED_STDERR = (
    f"WARN  2026-10-12 01:04:19,852 [shard 0:main] scylla-sstable - Skipping SSTable {FAILED_SSTABLE},"
    " it was deleted while being loaded: std::filesystem::filesystem_error: filesystem error: open failed:"
    f" No such file or directory [{TABLE_DIR}/mt-3h4l_0218_208b42ahjsml3d91p3-big-Statistics.db]"
)
MISORDERED_STDERR = (
    f"Could not load SSTable: {FAILED_SSTABLE}: sstables::malformed_sstable_exception ({FAILED_SSTABLE}:"
    " first and last keys of summary are misordered)"
)


@pytest.fixture()
def make_sstable_utils(monkeypatch):
    """Build SstableUtils whose metadata dump fails with the given stderr and whose files exist as given."""
    monkeypatch.setattr(sstable_utils, "get_sstable_metadata_dump_command", lambda *_: "scylla sstable")

    def _make(stderr_by_sstable, existing):
        def _sudo(cmd, **_):
            sstable = cmd.split()[-1]
            if cmd.startswith("test -e "):
                return Result(command=cmd, exited=0 if sstable in existing else 1)
            return Result(command=cmd, stdout="", stderr=stderr_by_sstable[sstable], exited=1)

        node = MagicMock()
        node.remoter.sudo.side_effect = _sudo
        return SstableUtils(db_node=node, ks_cf="keyspace1.standard1")

    return _make


def test_missing_sstable_is_skipped(make_sstable_utils):
    utils = make_sstable_utils(
        {ENCRYPTED_SSTABLE: ENCRYPTED_STDERR, FAILED_SSTABLE: DELETED_STDERR}, existing={ENCRYPTED_SSTABLE}
    )

    utils.check_that_sstables_are_encrypted(sstables=[ENCRYPTED_SSTABLE, FAILED_SSTABLE])

    checked = [call.args[0] for call in utils.db_node.remoter.sudo.call_args_list]
    assert checked == [
        f"scylla sstable {ENCRYPTED_SSTABLE}",
        f"scylla sstable {FAILED_SSTABLE}",
        f"test -e {FAILED_SSTABLE}",
    ]


def test_read_error_on_existing_sstable_fails_check(make_sstable_utils):
    utils = make_sstable_utils(
        {ENCRYPTED_SSTABLE: ENCRYPTED_STDERR, FAILED_SSTABLE: DELETED_STDERR},
        existing={ENCRYPTED_SSTABLE, FAILED_SSTABLE},
    )

    with pytest.raises(AssertionError, match=r"Results out of 2: True=1, False=0, None=1"):
        utils.check_that_sstables_are_encrypted(sstables=[ENCRYPTED_SSTABLE, FAILED_SSTABLE])


def test_no_sstables_left_raises_not_found(make_sstable_utils):
    utils = make_sstable_utils(
        {ENCRYPTED_SSTABLE: DELETED_STDERR, FAILED_SSTABLE: MISORDERED_STDERR}, existing={FAILED_SSTABLE}
    )

    with pytest.raises(SstablesNotFound, match="1 deleted and 1 misordered out of 2"):
        utils.check_that_sstables_are_encrypted(sstables=[ENCRYPTED_SSTABLE, FAILED_SSTABLE])
