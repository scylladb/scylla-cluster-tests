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

"""Where the SSH log followers start reading a node's journal.

A node created for the run has a journal of this run alone, read from its beginning. A physical host's journal outlives
its runs, so the node names the point this run starts at (BaseNode.journal_since), and the log file says so.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.utils.remote_logger import SSHGeneralFileLogger, SSHGeneralSystemdLogger, SSHScyllaSystemdLogger

TEST_ID = "7fba7abb-891c-4199-b61d-bf9ca12d671d"
RUN_START = "@1791382530"  # 2026-10-07 14:15:30 UTC


def _logger(logger_class, tmp_path, journal_since):
    node = MagicMock(journal_since=journal_since)
    node.name = "db-node-7fba7abb-0"
    node.test_config.test_id.return_value = TEST_ID
    logger = logger_class(node=node, target_log_file=str(tmp_path / "system.log"))
    logger._is_ready_to_retrieve = lambda: True
    return logger


def _read_since(logger, reads: int = 2) -> list[str | None]:
    """Run the journal task for `reads` reads (the follower reconnecting in between) and return their `since`."""
    seen = []

    def retrieve(since):
        seen.append(since)
        if len(seen) == reads:
            logger._termination_event.set()

    logger._retrieve = retrieve
    logger._journal_thread()
    return seen


@pytest.mark.parametrize("logger_class", [SSHScyllaSystemdLogger, SSHGeneralSystemdLogger])
def test_a_reused_host_is_read_from_the_start_of_the_run(logger_class, tmp_path):
    logger = _logger(logger_class, tmp_path, journal_since=RUN_START)

    assert _read_since(logger)[0] == RUN_START
    assert (tmp_path / "system.log").read_text() == (
        f"==== SCT test {TEST_ID} starts on db-node-7fba7abb-0: reading its journal from 2026-10-07 14:15:30 UTC, "
        "older entries there belong to earlier runs on this host ====\n"
    )


@pytest.mark.parametrize("logger_class", [SSHScyllaSystemdLogger, SSHGeneralSystemdLogger])
def test_a_new_node_is_read_from_the_beginning(logger_class, tmp_path):
    logger = _logger(logger_class, tmp_path, journal_since=None)

    assert _read_since(logger)[0] is None
    assert not (tmp_path / "system.log").exists()


def test_a_log_file_follower_ignores_the_start_of_the_run(tmp_path):
    """tail reads the whole file whatever it is told, so there is nothing to mark."""
    logger = _logger(SSHGeneralFileLogger, tmp_path, journal_since=RUN_START)

    assert _read_since(logger)[0] is None
    assert not (tmp_path / "system.log").exists()


def test_a_reconnect_reads_on_from_a_time_free_of_zones(tmp_path, monkeypatch):
    """journalctl reads a date without a zone in the host's local time, and systemd < 220 rejects one with a zone."""
    monkeypatch.setattr("sdcm.utils.remote_logger.time.time", lambda: 1791382590.7)
    logger = _logger(SSHGeneralSystemdLogger, tmp_path, journal_since=None)

    assert _read_since(logger)[1] == "@1791382590"
