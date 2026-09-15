import time
from pathlib import Path

import pytest

from sdcm.remote import LOCALRUNNER
from sdcm.remote.remote_long_running import run_long_running_cmd
from sdcm.remote.libssh2_client import UnexpectedExit
from sdcm.remote.libssh2_client.exceptions import CommandTimedOut


@pytest.fixture(scope="function", autouse=True)
def fixture_check_tmp_files_cleared():
    yield
    assert list(Path("/tmp").glob("remoter_*")) == []


def test_long_command_failing():
    with pytest.raises(UnexpectedExit, match=r".*Exit code: 127.*") as exc_info:
        run_long_running_cmd(LOCALRUNNER, cmd="sleep 1 && bbb", timeout=100)

    assert exc_info.value.result.command == "sleep 1 && bbb"
    assert "bbb" in exc_info.value.result.stderr  # message is locale-dependent; only check the command name
    assert exc_info.value.result.stdout == ""
    assert exc_info.value.result.exited == 127
    assert exc_info.value.result.return_code == 127
    assert exc_info.value.result.duration > 1


def test_long_command_timeout():
    start = time.perf_counter()
    try:
        with pytest.raises(CommandTimedOut) as exc_info:
            run_long_running_cmd(LOCALRUNNER, cmd="sleep 30.123", timeout=2)
    finally:
        # the timed-out command keeps running in the background and would recreate its
        # exit-code log after the cleanup callback ran — kill it and sweep the leftovers
        LOCALRUNNER.run("pkill -f 'sleep 30.123'", ignore_status=True, verbose=False)
        time.sleep(0.5)
        LOCALRUNNER.run("rm -f /tmp/remoter_*", ignore_status=True, verbose=False)

    # the timeout is a deadline: it must not be amplified by poll retries
    assert time.perf_counter() - start < 15
    assert exc_info.value.timeout == 2
    assert exc_info.value.result.command == "sleep 30.123"


def test_long_command_success():
    result = run_long_running_cmd(LOCALRUNNER, cmd="sleep 1 && echo 'cmd is done'", timeout=100)
    assert result.ok
    assert result.stdout == "cmd is done\n"
    assert result.stderr == ""
    assert result.exited == 0
    assert result.return_code == 0
    assert result.duration > 1
