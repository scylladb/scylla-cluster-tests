"""Tests for the package manager lock wait snippets (sdcm/utils/package_lock.py).

The snippets run for real in bash, against a temporary lock file held by a background process,
the same way an orphaned apt-get holds /var/lib/dpkg/lock-frontend after an SSH timeout.
"""

import shutil
import subprocess

import pytest

from sdcm.utils.apt import apt_cmd, apt_lock_wait
from sdcm.utils.package_lock import lock_wait
from sdcm.utils.rpm import rpm_lock_wait

pytestmark = pytest.mark.skipif(not shutil.which("fuser"), reason="needs fuser (psmisc)")


@pytest.fixture
def held_lock(tmp_path):
    """A lock file held open by a background process, like an orphaned apt-get."""
    lock_file = tmp_path / "lock-frontend"
    lock_file.touch()
    holder = subprocess.Popen(["bash", "-c", f"exec 3>{lock_file}; exec sleep 300"])
    yield lock_file, holder
    holder.kill()
    holder.wait()


def run_snippet(snippet):
    """Run a snippet with ``bash -ce`` like the monitor prereqs script; cloud-init uses ``bash -cxe``."""
    return subprocess.run(["bash", "-ce", snippet], capture_output=True, text=True, timeout=60, check=False)


@pytest.mark.parametrize(
    "snippet",
    [
        pytest.param(apt_lock_wait(), id="apt"),
        pytest.param(apt_lock_wait(kill_stale=True), id="apt-kill-stale"),
        pytest.param(rpm_lock_wait(), id="rpm"),
    ],
)
def test_snippets_are_quote_free(snippet):
    """They get embedded into ``bash -c '...'`` commands and cloud-init scripts."""
    assert "'" not in snippet


def test_free_lock_passes_silently(tmp_path):
    """lock_wait on a free lock returns 0 without logging a holder."""
    lock_file = tmp_path / "lock"
    lock_file.touch()

    result = run_snippet(f"{lock_wait('apt', str(lock_file), lock_timeout=2, poll=1)} && echo done")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "done"


def test_held_lock_logs_holder_and_keeps_it_without_kill_stale(held_lock):
    """A held lock is reported with its holder after the timeout, and the holder survives without kill_stale."""
    lock_file, holder = held_lock

    result = run_snippet(f"{lock_wait('apt', str(lock_file), lock_timeout=2, poll=1)} && echo done")

    assert result.returncode == 0, result.stderr
    assert "apt lock held by:" in result.stdout
    assert "apt lock still held after 2s by:" in result.stdout
    assert str(holder.pid) in result.stdout
    assert "sleep 300" in result.stdout
    assert "done" in result.stdout
    assert holder.poll() is None, "the holder must not be killed without kill_stale"


def test_held_lock_kill_stale_kills_holder_and_repairs(held_lock, tmp_path):
    """kill_stale terminates the lock holder and then runs the repair command."""
    lock_file, holder = held_lock
    repaired = tmp_path / "repaired"

    snippet = lock_wait("apt", str(lock_file), lock_timeout=2, poll=1, kill_stale=True, repair=f"touch {repaired}")
    result = run_snippet(f"{snippet} && echo done")

    assert result.returncode == 0, result.stderr
    assert str(holder.pid) in result.stdout
    assert "killing the stale apt lock holders" in result.stdout
    assert holder.wait(timeout=10) != 0
    assert repaired.exists()
    assert "done" in result.stdout


def test_apt_cmd_kill_stale_implies_lock_wait():
    """apt_cmd with kill_stale waits for the lock, kills stale holders and repairs dpkg without prompts."""
    cmd = apt_cmd("dist-upgrade scylla -y", lock_timeout=240, kill_stale=True)

    assert cmd.startswith("bash -c '")
    assert "seq 1 48" in cmd
    assert "fuser -k -TERM" in cmd
    # the repair must not stop on a conffile prompt (scylla.yaml) with the SSH stdin left open
    assert "dpkg --force-confdef --force-confold --configure -a </dev/null" in cmd
    assert "apt-get" in cmd and "dist-upgrade scylla -y" in cmd


def test_apt_cmd_lock_wait_does_not_kill_by_default():
    """apt_cmd with lock_wait only waits for the lock and never kills holders."""
    assert "fuser -k" not in apt_cmd("update", lock_wait=True)
