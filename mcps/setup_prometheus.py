import json
import os
import socket
import subprocess
import sys
import time

# TODO: make prometheus port be configurable
PROMETHEUS_PORT = 9090

# TODO: check that existing docker container matches the 'test_id'


def check_port_in_use(port: int = PROMETHEUS_PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(('localhost', port)) == 0


def run_subprocess(command_and_args: list):
    result = subprocess.run(
        command_and_args,
        check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Command failed:\n" +
            f"Command: {command_and_args}\n"
            f"Return code: {result.returncode}\n" +
            f"STDOUT: {result.stdout}\n" +
            f"STDERR: {result.stderr}"
        )
    return result


def run_prometheus_container(sct_base_dir, test_id):
    print(f"Port {PROMETHEUS_PORT} is free. Starting Prometheus...")
    cmd = [
        "script", "-q", "-c",
        f"{sct_base_dir}/docker/env/hydra.sh investigate show-monitor {test_id}",
    ]
    results = run_subprocess(cmd)
    print(f"Prometheus docker container has successfully been created")


def get_docker_gateway_ip_address():
    cmd = ["docker", "network", "inspect", "bridge", "--format={{(index .IPAM.Config 0).Gateway}}"]
    results = run_subprocess(cmd)
    ip_addr = results.stdout.strip()
    print(f"Docker gateway IP address is '{ip_addr}'")
    return ip_addr


def get_prometheus_url(sct_root_dir, test_id):
    if not check_port_in_use(PROMETHEUS_PORT):
        run_prometheus_container(sct_root_dir, test_id)
    else:
        print(f"The '{PROMETHEUS_PORT}' port is used, skip creation of prometheus container")
    # TODO: check here that prometheus container is really running

    docker_gateway_ip_address = get_docker_gateway_ip_address()
    return f"http://{docker_gateway_ip_address}:{PROMETHEUS_PORT}"


def find_project_root(target_files):
    """
    Traverse upwards from start_path to find the root directory containing any of the target_files.
    Returns the absolute path to the root directory, or None if not found.
    """
    start_path = os.getcwd()
    current_dir = os.path.abspath(start_path)
    while True:
        for target in target_files:
            if os.path.isfile(os.path.join(current_dir, target)):
                return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            # Reached filesystem root
            return None
        current_dir = parent_dir


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 setup_prometheus.py <test_id>")
        sys.exit(1)
    sct_root_dir = find_project_root(target_files=("sct.py", "pyproject.toml"))
    test_id = sys.argv[1]
    url = get_prometheus_url(sct_root_dir, test_id)
    print(f"Prometheus URL: {url}")
