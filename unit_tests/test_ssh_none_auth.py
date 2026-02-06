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
# Copyright (c) 2025 ScyllaDB

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

from sdcm.remote.remote_base import RemoteCmdRunnerBase
from sdcm.utils.decorators import retrying


@pytest.fixture(scope="module")
def ssh_server_container():
    """Start a Docker container with SSH server configured for 'none' authentication."""
    exposed_port = 22
    docker_dir = Path(__file__).parent / "test_data" / "auth_none_ssh_docker"
    image_tag = "ssh_server_none_auth_test:latest"

    subprocess.run(["docker", "build", "-t", image_tag, docker_dir], check=True)
    container_name = f"ssh_server_none_auth_test_{os.getpid()}"
    container_id = (
        subprocess.check_output(["docker", "run", "-d", "--name", container_name, image_tag]).decode().strip()
    )

    # get container IP
    inspect_cmd = ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_id]
    for _ in range(10):
        ip = subprocess.check_output(inspect_cmd).decode().strip()
        if ip:
            break
        time.sleep(0.5)
    else:
        subprocess.run(["docker", "rm", "-f", container_id], check=True)
        raise RuntimeError("Could not get container IP address")

    @retrying(n=20, sleep_time=0.5)
    def wait_for_ssh_ready():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            s.connect((ip, exposed_port))

    try:
        wait_for_ssh_ready()
        yield {
            "hostname": ip,
            "user": "test",
            "port": exposed_port,
            "key_file": None,
            "password": None,
        }
    finally:
        subprocess.run(["docker", "rm", "-f", container_id], check=True)


@pytest.mark.parametrize("ssh_transport", ["libssh2", "fabric"])
@pytest.mark.integration
def test_ssh_none_auth(ssh_server_container, ssh_transport):
    """Test 'none' authentication for SSH transports.

    Verifies that both libssh2 and fabric can authenticate using the 'none' method.
    """
    ssh_login_info = ssh_server_container
    RemoteCmdRunnerBase.set_default_ssh_transport(ssh_transport)
    remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)

    res = remoter.run("ls -l /", retry=0)
    assert res.ok, f"SSH command failed: {res}"
    output = getattr(res, "stdout", str(res))
    for dirname in ["bin", "etc", "usr"]:
        assert dirname in output, f"Expected directory '{dirname}' not found in output: {output}"
