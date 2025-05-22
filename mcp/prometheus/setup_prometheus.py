import json
import socket
import subprocess
import sys
import time

# TODO: make prometheus port be configurable
PROMETHEUS_PORT = 9090
NGROK_CONTAINER_NAME = "prometheus-tunnel-by-ngrok"
NGROK_API_URL = "http://localhost:4040/api/tunnels"

# TODO: check that existing docker container matches the 'test_id'


def check_port_in_use(port=PROMETHEUS_PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(('localhost', port)) == 0

def run_prometheus_container(sct_base_dir, test_id):
    print(f"Port {PROMETHEUS_PORT} is free. Starting Prometheus...")
    cmd = f"{sct_base_dir}/docker/env/hydra.sh investigate show-monitor {test_id}"
    result = subprocess.run(
        ["script", "-q", "-c", cmd],
        check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Prometheus containder creation failed:\n" +
            f"Return code: {result.returncode}\n" +
            f"STDOUT: {result.stdout}\n" +
            f"STDERR: {result.stderr}"
        )

def container_exists(name=NGROK_CONTAINER_NAME):
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    return name in result.stdout.strip().splitlines()

def start_ngrok_container(ngrok_auth_token):
    print("Starting ngrok container...")
    subprocess.run([
        "docker", "run", "--rm", "-d", "--name", NGROK_CONTAINER_NAME,
        "--network=host",
        "-e",
        f"NGROK_AUTHTOKEN={ngrok_auth_token}",
        "ngrok/ngrok:alpine",
        "http",
        f"localhost:{PROMETHEUS_PORT}"
    ], check=True)
    # Give ngrok time to start
    time.sleep(4)

def get_ngrok_url():
    for _ in range(10):
        try:
            response = subprocess.check_output(["curl", "-s", NGROK_API_URL])
            data = json.loads(response)
            return data["tunnels"][0]["public_url"]
        except Exception:
            time.sleep(1)
    raise RuntimeError("Failed to retrieve ngrok public URL.")

def get_url(base_sct_dir, ngrok_auth_token, test_id):
    if not check_port_in_use(PROMETHEUS_PORT):
        run_prometheus_container(base_sct_dir, test_id)
    else:
        print(f"The '{PROMETHEUS_PORT}' port is used, skip creation of prometheus container")

    if not container_exists(NGROK_CONTAINER_NAME):
        start_ngrok_container(ngrok_auth_token)

    # TODO: check here that prometheus container is really running

    url = get_ngrok_url()
    if url.startswith("http"):
        return url
    else:
        raise RuntimeError("Could not get NGROK url for Prometheus. Check prometheus setup.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 setup_prometheus.py <base-sct-dir> <ngrok-auth-token> <test_id>")
        sys.exit(1)
    base_sct_dir = sys.argv[1]
    ngrok_auth_token = sys.argv[2]
    test_id = sys.argv[3]
    url = get_url(base_sct_dir, ngrok_auth_token, test_id)
    print(f"Public Prometheus URL: {url}")
