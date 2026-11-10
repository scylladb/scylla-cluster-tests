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

"""Each log follower must use its own script and pid file on the remote node.

A step of a performance test runs a write and a read stress command on one loader, and each
command has an HDR logger that follows its own HDR file. Both loggers start at the same moment.
With script and pid files named after the SCT process alone, the second logger overwrote the
script of the first before it ran, so both copied the same HDR file and the step reported one
command twice and the other not at all.
"""

import re
from unittest.mock import MagicMock

from sdcm.utils.remote_logger import HDRHistogramFileLogger


def _sent_command(logger) -> str:
    logger._retrieve(since=None)
    return logger._remoter.run.call_args.kwargs["cmd"]


def test_two_hdr_loggers_on_one_node_follow_their_own_files(tmp_path):
    node = MagicMock()
    write_logger = HDRHistogramFileLogger(
        node=node, remote_log_file="/home/ubuntu/hdrh-write.hdr", target_log_file=str(tmp_path / "write.hdr")
    )
    read_logger = HDRHistogramFileLogger(
        node=node, remote_log_file="/home/ubuntu/hdrh-read.hdr", target_log_file=str(tmp_path / "read.hdr")
    )

    write_cmd, read_cmd = _sent_command(write_logger), _sent_command(read_logger)

    def remote_files(cmd):
        return re.search(r"cat <<'EOF' > (\S+)", cmd).group(1), re.search(r"echo \$! > (\S+);", cmd).group(1)

    write_script, write_pid = remote_files(write_cmd)
    read_script, read_pid = remote_files(read_cmd)
    assert write_script != read_script
    assert write_pid != read_pid
    assert "tail -f /home/ubuntu/hdrh-write.hdr" in write_cmd
    assert "tail -f /home/ubuntu/hdrh-read.hdr" in read_cmd
    # The same logger keeps its names, so stop() finds the pid that _retrieve() wrote.
    assert remote_files(_sent_command(write_logger)) == (write_script, write_pid)
