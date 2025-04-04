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
# Copyright (c) 2022 ScyllaDB


from functools import partial

from sdcm.remote.libssh2_client import Result
from sdcm.rest.compaction_manager_client import CompactionManagerClient


class FakeRemoter:

    def run(self,
            cmd: str,
            timeout: int,
            retry: int,
            ) -> Result:
        return Result(stdout=cmd, stderr="", exited=0)


class FakeNode:

    @property
    def remoter(self):
        return FakeRemoter()


def test_compaction_manager_stop_compaction():
    client = CompactionManagerClient(FakeNode())
    result = partial(client.stop_compaction, compaction_type="reshape")()

    assert result.stdout == 'curl -v -X POST "http://localhost:10000/compaction_manager/stop_compaction?type=RESHAPE"'
