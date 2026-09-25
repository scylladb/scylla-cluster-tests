"""Tests for the builder-side coredump upload scripts."""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
HOST_SCRIPT = REPO_ROOT / "utils" / "upload_sct_coredump.sh"
HELPER_SCRIPT = REPO_ROOT / "utils" / "upload_sct_coredump_inside_hydra.sh"
BASH = shutil.which("bash") or "/bin/bash"

SCT_TEST_ID = "0123abcd-4567-89ef-0123-456789abcdef"
SINCE_EPOCH = 1700000000

# first call prints one coredump path; all calls are logged with \x1f-separated args
FAKE_HYDRA = """#!/bin/bash
(IFS=$'\\x1f'; echo "$*") >> "${FAKE_HYDRA_LOG}"
[[ $(wc -l < "${FAKE_HYDRA_LOG}") -eq 1 ]] && echo /var/lib/systemd/coredump/core.1234
exit 0
"""

# fake ./sct.py upload logs the archive path only if the archive exists
FAKE_SCT_PY = """#!/bin/bash
[[ -s "${@: -1}" ]] && echo "${@: -1}" >> "${FAKE_UPLOAD_LOG}"
exit "${FAKE_UPLOAD_RC:-0}"
"""

FAKE_SUDO = """#!/bin/bash
exec "$@"
"""


def _write_executable(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def test_host_script_collects_new_coredumps_in_one_hydra_run(tmp_path):
    """Archive, upload and removal share one hydra run.

    On the runner path every hydra call re-syncs the checkout with rsync --delete, so an archive
    left in the checkout by one call would be gone before a second call could upload it.
    """
    _write_executable(tmp_path / "docker" / "env" / "hydra.sh", FAKE_HYDRA)
    (tmp_path / "sct_runner_ip").write_text("10.0.0.5")
    hydra_log = tmp_path / "hydra.calls"

    result = subprocess.run(
        [BASH, str(HOST_SCRIPT)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "SCT_TEST_ID": SCT_TEST_ID,
            "COREDUMPS_SINCE_EPOCH": str(SINCE_EPOCH),
            "FAKE_HYDRA_LOG": str(hydra_log),
        },
    )

    assert result.returncode == 0, result.stderr
    calls = [line.split("\x1f") for line in hydra_log.read_text().splitlines()]
    assert calls[1:] == [
        [
            "--execute-on-runner",
            "10.0.0.5",
            f"bash ./utils/upload_sct_coredump_inside_hydra.sh /var/lib/systemd/coredump {SINCE_EPOCH}",
        ]
    ]


@pytest.mark.parametrize("upload_rc", [pytest.param("0", id="upload-succeeds"), pytest.param("1", id="upload-fails")])
def test_helper_removes_the_archive_after_the_upload(tmp_path, upload_rc):
    """The raw coredumps stay on the host, so the archive is removed whether or not the upload worked."""
    checkout = tmp_path / "checkout"
    _write_executable(checkout / "sct.py", FAKE_SCT_PY)
    _write_executable(tmp_path / "bin" / "sudo", FAKE_SUDO)
    coredump_dir = tmp_path / "coredump"
    coredump_dir.mkdir()
    (coredump_dir / "core.1234").write_bytes(b"\x7fELF")
    upload_log = tmp_path / "upload.calls"

    result = subprocess.run(
        [BASH, str(HELPER_SCRIPT), str(coredump_dir), str(SINCE_EPOCH)],
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "PATH": f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "SCT_TEST_ID": SCT_TEST_ID,
            "FAKE_UPLOAD_LOG": str(upload_log),
            "FAKE_UPLOAD_RC": upload_rc,
        },
    )

    assert (result.returncode == 0) == (upload_rc == "0"), result.stderr
    assert upload_log.exists(), "the archive was never uploaded"
    assert list(checkout.glob("sct-coredumps-*")) == []
