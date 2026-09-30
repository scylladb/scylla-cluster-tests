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

"""Runs the vector.dev install script on every OS family SCT provisions nodes on."""

import re
import shutil
import subprocess
from contextlib import contextmanager

import pytest

from sdcm.remote.base import shell_script_cmd
from sdcm.provision.common import utils as provision_utils
from sdcm.provision.common.utils import (
    configure_backoff_timeout,
    install_vector_service,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.need_network,
    pytest.mark.xdist_group("docker_heavy"),
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not available"),
    pytest.mark.skip("docker-heavy distro matrix with network downloads, run manually with -p no:skipping"),
]

DISTRO_IMAGES = [
    "rockylinux:8",
    "rockylinux:9",
    "rockylinux/rockylinux:10",
    "quay.io/centos/centos:stream9",
    "oraclelinux:8",
    "oraclelinux:9",
    "oraclelinux:10",
    "amazonlinux:2023",
    "ubuntu:22.04",
    "ubuntu:24.04",
    "ubuntu:26.04",
    "debian:12",
    "debian:13",
]

COMMAND_TIMEOUT = 600

SETUP_SCRIPT = """
mkdir -p /usr/local/bin
printf '%s\\n' '#!/bin/sh' 'exit 0' > /usr/local/bin/systemctl
chmod +x /usr/local/bin/systemctl
if ! command -v curl > /dev/null 2>&1; then
    if command -v apt-get > /dev/null 2>&1; then
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl ca-certificates
    else
        yum install -y -q curl
    fi
fi
"""


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=COMMAND_TIMEOUT, check=False)


def _docker_exec(container_id: str, script: str, wrapper: str = "bash") -> subprocess.CompletedProcess:
    """Execute script in container. Use wrapper='provisioning' for remoter-style execution."""
    if wrapper == "provisioning":
        return _run(["docker", "exec", container_id, "sh", "-c", shell_script_cmd(script, quote="'")])
    return _run(["docker", "exec", container_id, wrapper, "-cxe", script])


def _tail(result: subprocess.CompletedProcess, limit: int = 2000) -> str:
    return f"stdout:\n{result.stdout[-limit:]}\nstderr:\n{result.stderr[-limit:]}"


def _current_latest_version() -> str:
    """Ask the package host which release "latest" points at right now."""
    result = _run(
        [
            "curl",
            "-sI",
            "-o",
            "/dev/null",
            "-w",
            "%{redirect_url}",
            f"{provision_utils.VECTOR_LATEST_SOURCE}/vector-latest-1.x86_64.rpm",
        ]
    )
    match = re.search(r"vector-([0-9][0-9.]*)-1\.", result.stdout)
    assert match, f"cannot resolve the latest vector release: {result.stdout!r}"
    return match.group(1)


@contextmanager
def running_container(image: str):
    pull = _run(["docker", "pull", "-q", image])
    if pull.returncode != 0:
        pytest.skip(f"cannot pull {image}: {pull.stderr.strip()}")

    created = _run(["docker", "run", "-d", "--rm", image, "sleep", "infinity"])
    assert created.returncode == 0, f"cannot start {image}: {created.stderr}"

    container_id = created.stdout.strip()
    try:
        yield container_id
    finally:
        _run(["docker", "rm", "-f", container_id])


@pytest.mark.parametrize("image", DISTRO_IMAGES)
def test_vector_installs_and_runs(image):
    with running_container(image) as container_id:
        setup = _docker_exec(container_id, SETUP_SCRIPT)
        assert setup.returncode == 0, f"test setup failed on {image}: {_tail(setup)}"

        script = configure_backoff_timeout() + install_vector_service()
        install = _docker_exec(container_id, script, wrapper="provisioning")
        assert install.returncode == 0, f"install script failed on {image}: {_tail(install)}"

        version = _run(["docker", "exec", container_id, "vector", "--version"])
        assert version.returncode == 0, f"installed vector does not run on {image}: {_tail(version)}"
        assert _current_latest_version() in version.stdout

        unit_present = _run(
            [
                "docker",
                "exec",
                container_id,
                "sh",
                "-c",
                "test -f /usr/lib/systemd/system/vector.service || test -f /lib/systemd/system/vector.service",
            ]
        )
        assert unit_present.returncode == 0, f"vector.service unit file missing on {image}"


@pytest.mark.parametrize("image", ["rockylinux:9", "debian:13"])
def test_unusable_preinstalled_vector_is_replaced(image):
    """A package that is present but cannot run must not count as installed."""
    with running_container(image) as container_id:
        setup = _docker_exec(container_id, SETUP_SCRIPT)
        assert setup.returncode == 0, f"test setup failed on {image}: {_tail(setup)}"

        script = configure_backoff_timeout() + install_vector_service()
        assert _docker_exec(container_id, script).returncode == 0

        break_binary = "printf '%s\\n' '#!/bin/sh' 'exit 3' > /usr/bin/vector; chmod +x /usr/bin/vector"
        assert _docker_exec(container_id, break_binary).returncode == 0

        result = _docker_exec(container_id, script, wrapper="provisioning")
        assert result.returncode == 0, f"second run failed on {image}: {_tail(result)}"

        version = _run(["docker", "exec", container_id, "vector", "--version"])
        assert version.returncode == 0, f"vector still unusable on {image}: {_tail(version)}"
        assert _current_latest_version() in version.stdout


@pytest.mark.parametrize("image", ["rockylinux:9"])
def test_a_failing_package_manager_aborts_provisioning(image):
    """A package manager that reports failure must be fatal, not silently accepted."""
    tool = "rpm" if image.startswith(("rockylinux", "oraclelinux", "amazonlinux", "quay.io")) else "dpkg"
    shadow = (
        f"printf '%s\\n' '#!/bin/sh' '/usr/bin/{tool} \"$@\"' 'exit 1' > /usr/local/bin/{tool}; "
        f"chmod +x /usr/local/bin/{tool}"
    )

    with running_container(image) as container_id:
        setup = _docker_exec(container_id, SETUP_SCRIPT)
        assert setup.returncode == 0, f"test setup failed on {image}: {_tail(setup)}"
        assert _docker_exec(container_id, shadow).returncode == 0

        result = _docker_exec(
            container_id, configure_backoff_timeout() + install_vector_service(), wrapper="provisioning"
        )
        assert result.returncode != 0, f"a failing {tool} was accepted on {image}: {_tail(result)}"
        assert "ERROR: vector.dev installation failed" in result.stdout
        assert "install output" in result.stdout


def test_falls_back_to_a_known_release_when_the_latest_alias_is_unreachable(monkeypatch):
    """A latest-alias failure must fall back to a known release."""
    monkeypatch.setattr(provision_utils, "VECTOR_LATEST_SOURCE", "https://10.255.255.1/vector/latest")
    script = configure_backoff_timeout() + install_vector_service()

    with running_container("debian:13") as container_id:
        assert _docker_exec(container_id, SETUP_SCRIPT).returncode == 0

        result = _docker_exec(container_id, script, wrapper="provisioning")
        assert result.returncode == 0, f"fallback install failed: {_tail(result)}"

        version = _run(["docker", "exec", container_id, "vector", "--version"])
        assert provision_utils.VECTOR_FALLBACK_VERSION in version.stdout
