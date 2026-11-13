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

"""Run the boot-time package lists refresh against a fake apt-get.

A slow mirror kept `apt-get update` running past the SCT wait for cloud-init, which failed provisioning.
"""

import os
import subprocess
import time

from sdcm.provision.common import utils

# Shell functions take precedence over PATH, so the script cannot touch the host's package manager.
HOST_STUBS = """\
yum() { return 1; }
fuser() { return 1; }
rm() { :; }
backoff() { echo 0; }
"""


def run_update_repo_cache(tmp_path, fake_apt_get: str) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    apt_get = bin_dir / "apt-get"
    # `timeout` runs apt-get as a program, so it is a file on PATH, not a shell function
    apt_get.write_text(f'#!/bin/bash\ncase "$*" in *update*) {fake_apt_get} ;; esac\n')
    apt_get.chmod(0o755)
    return subprocess.run(
        ["bash", "-eux"],
        input=HOST_STUBS + utils.update_repo_cache(),
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
        cwd=tmp_path,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )


def test_hanging_apt_get_update_gives_up_after_the_time_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "APT_UPDATE_TIME_BUDGET", 2)

    started = time.monotonic()
    # exec, so `timeout` kills the sleep itself and nothing keeps the output pipes open
    result = run_update_repo_cache(tmp_path, fake_apt_get="exec sleep 300")

    assert result.returncode == 0, result.stderr
    assert time.monotonic() - started < 20
    assert "apt-get update did not succeed in 2 seconds" in result.stdout


def test_failing_apt_get_update_is_retried_until_it_succeeds(tmp_path):
    result = run_update_repo_cache(tmp_path, fake_apt_get='echo x >> calls; [ "$(wc -l < calls)" -ge 3 ] || exit 100')

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "calls").read_text().count("x") == 3
    assert "did not succeed" not in result.stdout
