import os
import pytest
import subprocess
import time
import socket
from pathlib import Path

from sdcm.remote.remote_cmd_runner import RemoteCmdRunnerBase
from sdcm.utils.decorators import retrying


@pytest.fixture(scope="module")
def ssh_server_container():
    exposed_port = 22

    docker_dir = Path(__file__).parent / "test_data" / "auth_none_ssh_docker"

    image_tag = "ssh_server_test:latest"
    subprocess.run(["docker", "build", "-t", image_tag, docker_dir], check=True)
    container_name = f"ssh_server_test_{os.getpid()}"
    run_cmd = [
        "docker", "run", "-d", "--name", container_name, image_tag
    ]
    container_id = subprocess.check_output(run_cmd).decode().strip()

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
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        try:
            s.connect((ip, exposed_port))
            s.close()
        except (OSError, ConnectionRefusedError):
            raise RuntimeError(f"SSH port 22 not open on container {container_id} ({ip})")

    try:
        wait_for_ssh_ready()
        yield {"hostname": ip, "user": "test", "port": exposed_port, "key_file": None, "password": None}
    finally:
        subprocess.run(["docker", "rm", "-f", container_id], check=True)


@pytest.mark.parametrize("ssh_transport", ["libssh2", "fabric"])
@pytest.mark.integration
def test_ssh_server(ssh_server_container, ssh_transport):
    ssh_login_info = ssh_server_container
    RemoteCmdRunnerBase.set_default_ssh_transport(ssh_transport)
    remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)

    res = remoter.run("ls -l /", retry=0)
    # Assert that the command executed successfully and output contains expected directories
    assert getattr(res, "exit_code", 0) == 0, f"SSH command failed: {getattr(res, 'stderr', res)}"
    output = getattr(res, "stdout", str(res))
    # Check for some standard directories in the root
    for dirname in ["bin", "etc", "usr"]:
        assert dirname in output, f"Expected directory '{dirname}' not found in output: {output}"
