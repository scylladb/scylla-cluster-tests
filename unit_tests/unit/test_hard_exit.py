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

from unittest.mock import Mock

import pytest

from sdcm.utils import hard_exit
from sdcm.utils.hard_exit import STUCK_THREAD_EXIT_CODE, exit_process, request_hard_exit


@pytest.fixture(autouse=True)
def _reset_hard_exit_state(monkeypatch):
    """Ensure `_hard_exit_reason` never leaks between tests."""
    monkeypatch.setattr(hard_exit, "_hard_exit_reason", None)


def test_exit_process_uses_sys_exit_when_not_armed(monkeypatch):
    os_exit_mock = Mock()
    monkeypatch.setattr(hard_exit.os, "_exit", os_exit_mock)

    with pytest.raises(SystemExit) as exc_info:
        exit_process(3)

    assert exc_info.value.code == 3
    os_exit_mock.assert_not_called()


def test_exit_process_hard_exits_with_distinct_code_when_armed(monkeypatch):
    os_exit_mock = Mock()
    monkeypatch.setattr(hard_exit.os, "_exit", os_exit_mock)

    request_hard_exit("stuck nemesis thread")
    exit_process(3)

    os_exit_mock.assert_called_once_with(STUCK_THREAD_EXIT_CODE)
    assert 3 not in os_exit_mock.call_args.args
