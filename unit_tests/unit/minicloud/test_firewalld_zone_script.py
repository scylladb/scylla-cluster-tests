"""Tests for host-side firewalld zone assignment in `scripts/minicloud-firewalld-zone.sh`."""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[3] / "scripts" / "minicloud-firewalld-zone.sh"
# resolve bash before reducing PATH to only stub binaries
BASH = shutil.which("bash")

FAKE_FIREWALL_CMD = """#!/bin/bash
echo "firewall-cmd${FAKE_CALLER:+ (sudo)} $*" >> "${FAKE_LOG}"
# FAKE_POLKIT_DENIES: reject read queries unless invoked through the sudo stub, like Fedora
# polkit does for non-console users
deny() {
    [[ -n "${FAKE_POLKIT_DENIES:-}" && -z "${FAKE_CALLER:-}" ]]
}
case "$1" in
  --state)
      deny && { echo "Authorization failed." >&2; exit 4; }
      echo running; exit 0 ;;
  --get-zone-of-interface=*)
      deny && { echo "Authorization failed." >&2; exit 4; }
      echo "${FAKE_ZONE:-FedoraServer}"; exit 0 ;;
  --zone=trusted)
      [[ -n "${FAKE_CHANGE_FAILS:-}" ]] && { echo "Authorization failed" >&2; exit 1; }
      echo success; exit 0 ;;
esac
exit 3
"""

FAKE_SUDO_TRANSPARENT = """#!/bin/bash
shift
FAKE_CALLER=sudo exec "$@"
"""

FAKE_SUDO_NEEDS_PASSWORD = """#!/bin/bash
echo "sudo: a password is required" >&2
exit 1
"""

FAKE_IP = """#!/bin/bash
echo minicloud0
"""


def _run(tmp_path, sudo=FAKE_SUDO_TRANSPARENT, **env):
    """Run the script against stub executables; returns (CompletedProcess, firewall-cmd calls)."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)

    for name, body in {"firewall-cmd": FAKE_FIREWALL_CMD, "sudo": sudo, "ip": FAKE_IP}.items():
        stub = bindir / name
        stub.write_text(body)
        stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

    call_log = tmp_path / "firewall-cmd.calls"
    call_log.touch()
    result = subprocess.run(
        [BASH, str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PATH": str(bindir), "FAKE_LOG": str(call_log), **env},
    )
    return result, call_log.read_text().splitlines()


def test_unzoned_tun_is_moved_into_the_trusted_zone(tmp_path):
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "firewall-cmd (sudo) --zone=trusted --change-interface=minicloud0" in calls


def test_polkit_denied_reads_fall_back_to_sudo(tmp_path):
    # if polkit blocks unprivileged reads, the script must retry with sudo and still apply the zone
    result, calls = _run(tmp_path, FAKE_POLKIT_DENIES="1")
    assert result.returncode == 0, result.stderr
    assert "firewall-cmd (sudo) --zone=trusted --change-interface=minicloud0" in calls


def test_boot_unit_topology_needs_no_sudo(tmp_path):
    result, calls = _run(tmp_path, sudo=FAKE_SUDO_NEEDS_PASSWORD, FAKE_ZONE="trusted")
    assert result.returncode == 0, result.stderr
    assert not [call for call in calls if "--change-interface" in call]


@pytest.mark.parametrize(
    "kwargs, expected_stderr",
    [
        ({"sudo": FAKE_SUDO_NEEDS_PASSWORD}, "sudo: a password is required"),
        ({"FAKE_CHANGE_FAILS": "1"}, "Authorization failed"),
    ],
    ids=["no-passwordless-sudo", "change-interface-rejected"],
)
def test_failure_to_apply_the_zone_is_loud(tmp_path, kwargs, expected_stderr):
    result, _ = _run(tmp_path, **kwargs)
    assert result.returncode != 0, f"expected a non-zero exit, got: {result.stdout}"
    assert "could not move minicloud0" in result.stderr
    assert expected_stderr in result.stderr
    assert "AuthenticationError" in result.stderr
