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

"""Tests for the comm allow-list in `utils/upload_sct_coredump.sh`."""

import os
import shutil
import stat
import subprocess
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(not shutil.which("zstd"), reason="zstd binary not found")

SCRIPT = Path(__file__).parents[2] / "utils" / "upload_sct_coredump.sh"
HELPER_SCRIPT = SCRIPT.parent / "upload_sct_coredump_inside_hydra.sh"
BASH = shutil.which("bash")

# Stand-in for docker/env/hydra.sh. The real script re-runs its command through
# `eval '${CMD_TO_RUN}'` inside the container; plain `eval` on the single argv word we receive
# reproduces the same unescaping (no docker, no privilege boundary) since there's no container
# layer to cross in the test. The archive+upload step is now one hydra call that runs the real
# `upload_sct_coredump_inside_hydra.sh` (symlinked into work_dir by the work_dir fixture), which
# in turn calls the stubbed `./sct.py upload` below - so this stub never special-cases "upload".
HYDRA_STUB = """#!/bin/bash
set -e

args=("$@")
if [[ "${args[0]}" == "--execute-on-runner" ]]; then
    echo "execute-on-runner ${args[1]}" >> "$HYDRA_STUB_LOG"
    args=("${args[@]:2}")
fi

echo "exec ${args[0]}" >> "$HYDRA_STUB_LOG"
eval "${args[0]}"
"""

# Transparent: the script sudo's into the tar step only because that's what root-owned coredumps
# in /var/lib/systemd/coredump need on a real host; our fake coredumps are owned by the test user.
SUDO_STUB = """#!/bin/bash
exec "$@"
"""

# Stand-in for `./sct.py upload`, called by the real upload_sct_coredump_inside_hydra.sh. Logs the
# call the same way hydra's own "upload" step used to, before the archive/upload/delete sequence
# moved inside a single hydra call.
SCT_PY_STUB = """#!/bin/bash
echo "$*" >> "$HYDRA_STUB_LOG"
exit 0
"""


# systemd-coredump's xescape() escapes '.', ' ' and '/' out of comm as \xNN before building the
# filename, so e.g. a "python3.14" process never leaves a literal dot in the comm field - it
# shows up as "python3\x2e14".
_XESCAPE_TABLE = str.maketrans({".": "\\x2e", " ": "\\x20", "/": "\\x2f"})


def _make_core(core_dir, comm, age_seconds, uid=1000):
    """Create a fake `core.<escaped comm>.<uid>.<bootid>.<pid>.<ts>.zst` file with a controlled
    mtime; comm is escaped the way systemd-coredump escapes it before building the real filename.
    """
    escaped_comm = comm.translate(_XESCAPE_TABLE)
    name = f"core.{escaped_comm}.{uid}.deadbeefcafebabedeadbeefcafebabe.4242.{int(time.time())}.zst"
    path = core_dir / name
    path.write_text("not a real coredump, just bytes for the tarball\n")
    mtime = time.time() - age_seconds
    os.utime(path, (mtime, mtime))
    return path


@pytest.fixture()
def work_dir(tmp_path):
    """cwd for the script under test.

    Holds a stub docker/env/hydra.sh, the real upload_sct_coredump_inside_hydra.sh symlinked in
    at the relative path the host script invokes it through (so that path resolves against this
    cwd the way it does on a real agent), and a stub ./sct.py the helper script uploads through.
    """
    hydra_dir = tmp_path / "docker" / "env"
    hydra_dir.mkdir(parents=True)
    hydra_stub_path = hydra_dir / "hydra.sh"
    hydra_stub_path.write_text(HYDRA_STUB)
    hydra_stub_path.chmod(hydra_stub_path.stat().st_mode | stat.S_IEXEC)

    utils_dir = tmp_path / "utils"
    utils_dir.mkdir()
    (utils_dir / "upload_sct_coredump_inside_hydra.sh").symlink_to(HELPER_SCRIPT)

    sct_py_path = tmp_path / "sct.py"
    sct_py_path.write_text(SCT_PY_STUB)
    sct_py_path.chmod(sct_py_path.stat().st_mode | stat.S_IEXEC)

    return tmp_path


@pytest.fixture()
def core_dir(tmp_path):
    path = tmp_path / "coredumps"
    path.mkdir()
    return path


@pytest.fixture(scope="module")
def bin_dir(tmp_path_factory):
    bindir = tmp_path_factory.mktemp("bin")
    sudo_path = bindir / "sudo"
    sudo_path.write_text(SUDO_STUB)
    sudo_path.chmod(sudo_path.stat().st_mode | stat.S_IEXEC)
    return bindir


@pytest.fixture()
def sct_test_id():
    return uuid.uuid4().hex


def run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, include_comm=None, use_runner=False):
    log_path = work_dir / "hydra_calls.log"
    log_path.write_text("")

    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "COREDUMPS_DIR": str(core_dir),
        "COREDUMPS_SINCE_EPOCH": str(since_epoch),
        "SCT_TEST_ID": sct_test_id,
        "HYDRA_STUB_LOG": str(log_path),
    }
    if include_comm is not None:
        env["COREDUMPS_INCLUDE_COMM"] = include_comm
    if use_runner:
        (work_dir / "sct_runner_ip").write_text("203.0.113.5")

    result = subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=str(work_dir),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    calls = log_path.read_text().splitlines()
    return result, calls


def _tarball(work_dir, sct_test_id):
    # Matches upload_sct_coredump_inside_hydra.sh's `$(pwd)/sct-coredumps-${SCT_TEST_ID:0:8}.tar.zst`,
    # where pwd is work_dir (the host script's cwd, inherited by the hydra stub and the helper
    # script it execs). The helper always removes it on exit, so tests only use this path to
    # assert it is gone, never that it persists.
    return work_dir / f"sct-coredumps-{sct_test_id[:8]}.tar.zst"


@pytest.mark.parametrize(
    "comm",
    ["python3.14", "python3", "scylla", "scylla-server", "java"],
    ids=["escaped-dotted-version", "python3", "scylla", "scylla-server", "java-exact"],
)
def test_allowed_comm_is_uploaded(work_dir, bin_dir, core_dir, sct_test_id, comm):
    # "python3.14" is a realistic comm: _make_core escapes it to "python3\x2e14" (core.<comm> has
    # no literal dot), which "python*" (-> "python[^.]*") still matches and keeps.
    _make_core(core_dir, comm, age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch)

    assert result.returncode == 0, result.stderr
    assert "skipping" not in result.stdout
    tarball = _tarball(work_dir, sct_test_id)
    upload_calls = [call for call in calls if call.startswith("upload ")]
    assert len(upload_calls) == 1
    assert str(tarball) in upload_calls[0]
    # The helper archives, uploads, and removes the archive in the same hydra call; it never
    # survives for the host script to see.
    assert not tarball.exists(), result.stdout + result.stderr


@pytest.mark.parametrize("comm", ["s1-agent", "sshd", "mypython"], ids=["s1-agent", "sshd", "mypython"])
def test_foreign_comm_is_skipped_and_reported(work_dir, bin_dir, core_dir, sct_test_id, comm):
    core = _make_core(core_dir, comm, age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch)

    assert result.returncode == 0, result.stderr
    assert "skipping 1 coredump(s)" in result.stdout
    assert core.name in result.stdout
    assert not any(call.startswith("upload ") for call in calls)
    assert not _tarball(work_dir, sct_test_id).exists()


def test_all_skipped_means_no_upload_and_no_tarball(work_dir, bin_dir, core_dir, sct_test_id):
    _make_core(core_dir, "sshd", age_seconds=60)
    _make_core(core_dir, "s1-agent", age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch)

    assert result.returncode == 0, result.stderr
    assert "skipping 2 coredump(s)" in result.stdout
    assert "were filtered out by COREDUMPS_INCLUDE_COMM" in result.stdout
    assert not any(call.startswith("upload ") for call in calls)
    assert not any("upload_sct_coredump_inside_hydra.sh" in call for call in calls)
    assert not _tarball(work_dir, sct_test_id).exists()


def test_no_new_cores_means_nothing_to_upload(work_dir, bin_dir, core_dir, sct_test_id):
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch)

    assert result.returncode == 0, result.stderr
    assert "no coredumps newer than" in result.stdout
    assert "skipping" not in result.stdout
    assert not any(call.startswith("upload ") for call in calls)
    assert not _tarball(work_dir, sct_test_id).exists()


def test_old_allowed_core_is_ignored_and_not_reported_as_skipped(work_dir, bin_dir, core_dir, sct_test_id):
    # Older than SINCE_EPOCH: must drop out before the allow-list split even runs, so it's not
    # counted or printed as "skipped" - it was never a candidate in the first place.
    _make_core(core_dir, "python3", age_seconds=7200)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch)

    assert result.returncode == 0, result.stderr
    assert "no coredumps newer than" in result.stdout
    assert "skipping" not in result.stdout
    assert not any(call.startswith("upload ") for call in calls)
    assert not _tarball(work_dir, sct_test_id).exists()


@pytest.mark.parametrize(
    "include_comm, comms",
    [
        pytest.param("s1-agent", ["s1-agent"], id="override-single-entry"),
        pytest.param("java,s1-agent", ["java", "s1-agent"], id="override-multi-entry"),
    ],
)
def test_coredumps_include_comm_override_widens_the_allow_list(
    work_dir, bin_dir, core_dir, sct_test_id, include_comm, comms
):
    for comm in comms:
        _make_core(core_dir, comm, age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, include_comm=include_comm)

    assert result.returncode == 0, result.stderr
    assert "skipping" not in result.stdout
    upload_calls = [call for call in calls if call.startswith("upload ")]
    assert len(upload_calls) == 1
    assert not _tarball(work_dir, sct_test_id).exists()


def test_execute_on_runner_path_gives_the_same_split(work_dir, bin_dir, core_dir, sct_test_id):
    allowed = _make_core(core_dir, "scylla-server", age_seconds=60)
    foreign = _make_core(core_dir, "sshd", age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, use_runner=True)

    assert result.returncode == 0, result.stderr
    assert "skipping 1 coredump(s)" in result.stdout
    assert foreign.name in result.stdout
    assert allowed.name not in result.stdout
    runner_calls = [call for call in calls if call.startswith("execute-on-runner")]
    assert len(runner_calls) == 2, calls  # listing, archive+upload+delete
    assert all(call == "execute-on-runner 203.0.113.5" for call in runner_calls)
    upload_calls = [call for call in calls if call.startswith("upload ")]
    assert len(upload_calls) == 1
    assert not _tarball(work_dir, sct_test_id).exists()


def test_empty_include_comm_falls_back_to_default(work_dir, bin_dir, core_dir, sct_test_id):
    """An explicitly empty COREDUMPS_INCLUDE_COMM is `${VAR:-default}`-equivalent to unset."""
    _make_core(core_dir, "scylla-server", age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, include_comm="")

    assert result.returncode == 0, result.stderr
    assert "skipping" not in result.stdout
    upload_calls = [call for call in calls if call.startswith("upload ")]
    assert len(upload_calls) == 1
    assert not _tarball(work_dir, sct_test_id).exists()


def test_wildcard_does_not_cross_into_fields_after_comm(work_dir, bin_dir, core_dir, sct_test_id):
    """'*' must translate to '[^.]*', confined to the comm field, not '.*' which can cross the
    dot separator and absorb the uid field that follows comm in 'core.<comm>.<uid>.<bootid>...'.

    With the buggy '.*' translation, entry "sshd*0" (-> "sshd.*0") matches
    "core.sshd.0.<bootid>...": ".*" happily consumes the literal "." before uid "0", so a core
    whose comm is just "sshd" (uid 0) is wrongly treated as allow-listed and uploaded.
    """
    core = _make_core(core_dir, "sshd", age_seconds=60, uid=0)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, include_comm="sshd*0")

    assert result.returncode == 0, result.stderr
    assert "skipping 1 coredump(s)" in result.stdout
    assert core.name in result.stdout
    assert not any(call.startswith("upload ") for call in calls)
    assert not _tarball(work_dir, sct_test_id).exists()


@pytest.mark.parametrize(
    "include_comm",
    [
        pytest.param("a'b", id="single-quote"),
        pytest.param("a$b", id="dollar-sign"),
        pytest.param("a`b", id="backtick"),
        pytest.param("a,,b", id="double-comma"),
        pytest.param("a,", id="trailing-comma"),
        pytest.param("my.app", id="literal-dot"),
    ],
)
def test_invalid_include_comm_fails_fast_without_any_hydra_call(work_dir, bin_dir, core_dir, sct_test_id, include_comm):
    _make_core(core_dir, "scylla-server", age_seconds=60)
    since_epoch = int(time.time() - 3600)

    result, calls = run_script(work_dir, bin_dir, core_dir, sct_test_id, since_epoch, include_comm=include_comm)

    assert result.returncode != 0
    assert "ERROR" in result.stderr
    assert "COREDUMPS_INCLUDE_COMM" in result.stderr
    assert not calls, calls
    assert not _tarball(work_dir, sct_test_id).exists()
