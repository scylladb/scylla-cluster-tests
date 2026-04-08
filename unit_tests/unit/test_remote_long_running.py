from pathlib import Path

import pytest

from sdcm.remote import LOCALRUNNER
from sdcm.remote.remote_long_running import run_long_running_cmd
from sdcm.remote.libssh2_client import UnexpectedExit


@pytest.fixture(scope="function", autouse=True)
def fixture_check_tmp_files_cleared():
    yield
    assert list(Path("/tmp").glob("remoter_*")) == []


def test_long_command_failing():
    with pytest.raises(UnexpectedExit, match=r".*Exit code: 127.*") as exc_info:
        run_long_running_cmd(LOCALRUNNER, cmd="sleep 1 && bbb", timeout=100)

    assert exc_info.value.result.command == "sleep 1 && bbb"
    assert "line 1: bbb: command not found" in exc_info.value.result.stderr
    assert exc_info.value.result.stdout == ""
    assert exc_info.value.result.exited == 127
    assert exc_info.value.result.return_code == 127
    assert exc_info.value.result.duration > 1


def test_long_command_success():
    result = run_long_running_cmd(LOCALRUNNER, cmd="sleep 1 && echo 'cmd is done'", timeout=100)
    assert result.ok
    assert result.stdout == "cmd is done\n"
    assert result.stderr == ""
    assert result.exited == 0
    assert result.return_code == 0
    assert result.duration > 1
