import os
import pytest
import subprocess
import time
import socket

from sdcm.remote.remote_cmd_runner import RemoteCmdRunnerBase
from sdcm.utils.decorators import retrying


@pytest.fixture(scope="module")
def ssh_server_container(tmp_path_factory):
    exposed_port = 22
    dockerfile_content = f"""
    FROM ubuntu:25.10
    RUN apt-get update && apt-get install -y openssh-server
    RUN echo 'root:root' | chpasswd
    RUN useradd -m test
    RUN passwd -d test
    RUN echo 'PermitRootLogin yes' > /etc/ssh/sshd_config \
        && echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config \
        && echo 'PermitEmptyPasswords yes' >> /etc/ssh/sshd_config \
        && echo 'AuthenticationMethods none' >> /etc/ssh/sshd_config \
        && echo 'UsePAM no' >> /etc/ssh/sshd_config
    EXPOSE {exposed_port}
    CMD ["/usr/sbin/sshd", "-D"]
    """

    tmpdir = tmp_path_factory.mktemp("ssh_server")
    dockerfile_path = os.path.join(tmpdir, "Dockerfile")
    with open(dockerfile_path, "w") as f:
        f.write(dockerfile_content)
    image_tag = "ssh_server_test:latest"
    subprocess.run(["docker", "build", "-t", image_tag, tmpdir], check=True)
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
        yield {"hostname": ip, "user": "test", "port": exposed_port}
    finally:
        subprocess.run(["docker", "rm", "-f", container_id], check=True)


@pytest.mark.parametrize("ssh_transport", ["libssh2", "fabric"])
@pytest.mark.integration
def test_ssh_server(ssh_server_container, ssh_transport):
    ssh_login_info = ssh_server_container
    RemoteCmdRunnerBase.set_default_ssh_transport(ssh_transport)
    remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)

    res = remoter.run("ls -l /", retry=0)
    print(res)
