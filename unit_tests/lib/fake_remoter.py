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

import re
from typing import Dict, Pattern

from invoke import Result

from sdcm.remote import RemoteCmdRunnerBase


class FakeRemoter(RemoteCmdRunnerBase):
    """Fake remoter that responds to commands as described in `result_map` class attribute."""

    result_map: Dict[Pattern, Result] = {}

    def run(self,
            cmd: str,
            timeout=None,
            ignore_status=False,
            verbose=True,
            new_session=False,
            log_file=None,
            retry=1,
            watchers=None,
            timestamp_logs=False,
            change_context=False
            ) -> Result:
        for pattern, result in self.result_map.items():
            if re.match(pattern, cmd) is not None:
                if ignore_status is True:
                    return result
                else:
                    if result.failed:
                        raise Exception(f"Exception occurred when running command: {cmd}")
                    return result
        raise ValueError(f"No fake result specified for command: {cmd}."
                         f"Set {self.__class__.__name__}.result_map variable with Dict[Pattern, Result] mapping")

    def _create_connection(self):
        pass

    def _close_connection(self):
        pass

    def is_up(self, timeout: float = 30):
        return True

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool) -> bool:
        return True
