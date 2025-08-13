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
# Copyright (c) 2020 ScyllaDB

import json
from typing import Optional, List
from sdcm.remote.remote_base import StreamWatcher, Result
from sdcm.remote.remote_cmd_runner import RemoteCmdRunner
from unit_tests.lib.data_pickle import Pickler


class RemoterRecorder(RemoteCmdRunner):
    """
    Example:
    >>> from unit_tests.lib.remoter_recorder import RemoterRecorder
    >>> import getpass
    >>> remoter = RemoterRecorder(hostname='127.0.0.1', user=getpass.getuser(), key_file='~/.ssh/scylla_test_id_ed25519')
    >>> remoter.run('echo "Do something with remoter"')
    >>> remoter.save_responses_to_file('/tmp/test1_remoter.json')
    """
    responses = {}

    def run(self, cmd: str, timeout: Optional[float] = None,  # pylint: disable=too-many-arguments
            ignore_status: bool = False, verbose: bool = True, new_session: bool = False,
            log_file: Optional[str] = None, retry: int = 1, watchers: Optional[List[StreamWatcher]] = None,
            change_context: bool = False, suppress_errors: bool = False) -> Result:
        try:
            output = super().run(cmd, timeout, ignore_status, verbose, new_session, log_file, retry, watchers, change_context)
        except Exception as exc:
            output = exc
            responses = self.responses.get(cmd, None)
            if responses is None:
                self.responses[cmd] = responses = []
            responses.append(output)
            raise
        responses = self.responses.get(cmd, None)
        if responses is None:
            self.responses[cmd] = responses = []
        responses.append(output)
        return output

    def get_responses(self):
        return json.dumps(Pickler.from_data(self.responses))

    def save_responses_to_file(self, filepath):
        Pickler.save_to_file(filepath, self.responses)
