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

"""Run commands on a node through the cloud API, for log collection when SSH is down.

The AWS counterpart is `SSMCommandRunner`. This one wraps `VmInstance.run_command()`, which goes
through the cloud's VM agent on Azure (Run Command) and OCI (Instance Agent Run Command).
"""

import base64
import logging
import math
import uuid
import zlib
from pathlib import Path

from invoke.runners import Result

LOGGER = logging.getLogger(__name__)

# keep the end of a log: for a node that failed to come up, the end is what matters
MAX_LOG_BYTES = 8 * 1024 * 1024


class ProvisionerCommandRunner:
    """Run commands through `VmInstance.run_command()`, reading large output back in slices.

    A cloud agent returns only so much output per call (Azure: the last 4 KB), so a log is staged
    on the node gzipped and base64-encoded, and read back `chunk_size` characters per call.
    """

    def __init__(self, instance, chunk_size: int, max_chunks: int):
        self.instance = instance
        self.instance_id = instance.name
        self.chunk_size = chunk_size
        self.max_chunks = max_chunks

    def run(self, cmd: str, ignore_status: bool = True, **_) -> Result:
        result = self.instance.run_command(cmd)
        if not ignore_status and result.exited:
            raise RuntimeError(f"{cmd!r} failed on {self.instance_id}: {result.stderr}")
        return result

    def run_command_and_save_output(self, command: str, local_output_file: str, **_) -> Result:
        staged = f"/tmp/sct-oob-{uuid.uuid4().hex}"
        try:
            size_result = self.run(
                f"({command}) 2>&1 | tail -c {MAX_LOG_BYTES} | gzip -c | base64 -w0 > {staged}; wc -c < {staged}"
            )
            size = int(size_result.stdout.strip().splitlines()[-1])
            chunks = math.ceil(size / self.chunk_size)
            if chunks > self.max_chunks:
                LOGGER.warning(
                    "%s: %s needs %d calls, reading the first %d only",
                    self.instance_id,
                    command,
                    chunks,
                    self.max_chunks,
                )
            encoded = "".join(
                self.run(f"dd if={staged} bs={self.chunk_size} skip={index} count=1 status=none").stdout.strip()
                for index in range(min(chunks, self.max_chunks))
            )
        finally:
            self.run(f"rm -f {staged}")
        # base64 of a truncated read may end mid-quantum; decompressobj returns what arrived
        encoded = encoded[: len(encoded) - len(encoded) % 4]
        output = zlib.decompressobj(wbits=zlib.MAX_WBITS | 16).decompress(base64.b64decode(encoded))
        Path(local_output_file).write_bytes(output)
        return Result(stdout="", stderr="", exited=0, command=command)


def oob_runner_for(instance) -> ProvisionerCommandRunner | None:
    """A runner for the instance, or None when its provisioner has no command channel without SSH."""
    provisioner = getattr(instance, "_provisioner", None)
    if chunk_size := getattr(provisioner, "oob_chunk_size", None):
        return ProvisionerCommandRunner(instance, chunk_size=chunk_size, max_chunks=provisioner.oob_max_chunks)
    return None
