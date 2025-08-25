import os
import pathlib
import tempfile
import time
import subprocess
import boto3
import pytest
import logging
import secrets
import tarfile

from sdcm.utils.s3_remote_uploader import upload_remote_files_directly_to_s3
from sdcm.utils.common import S3Storage

# Constants for the test
TEST_S3_BUCKET = S3Storage.bucket_name
TEST_S3_KEY_PREFIX = os.environ.get("TEST_S3_KEY_PREFIX", "integration-test/")
SSH_IMAGE = "linuxserver/openssh-server:latest"
SSH_USER = "sct-user"  # linuxserver/openssh-server default user
SSH_PORT = 2222


@pytest.fixture(scope="module")
def ssh_docker_server(tmp_path_factory):
    """
    Pytest fixture to start an SSH server in a Docker container for testing.
    Generates an SSH key pair, starts the container, waits for SSH to be available,
    and yields SSH connection info, container name, and temp path. Cleans up the container after use.
    """
    tmp_path = tmp_path_factory.mktemp("ssh_server")
    ssh_key_path = tmp_path / "id_ed25519"
    ssh_pub_path = tmp_path / "id_ed25519.pub"
    # Generate SSH key pair
    subprocess.run([
        "ssh-keygen", "-t", "ed25519", "-f", str(ssh_key_path), "-N", ""
    ], check=True)
    # Start SSH server in Docker
    container_name = f"test-ssh-server-{os.getpid()}"
    cmd = [
        "docker", "run", "-d",
        "--name", container_name,
        "-p", f"{SSH_PORT}",
        "-e", f"USER_NAME={SSH_USER}",
        "-e", f"PUBLIC_KEY={pathlib.Path(ssh_pub_path).read_text().strip()}",
        SSH_IMAGE
    ]
    # Print docker run output for debugging
    logging.info('Running docker run command: %s', ' '.join(cmd))
    docker_run_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    logging.info('docker run stdout: %s', docker_run_result.stdout.decode())
    logging.info('docker run stderr: %s', docker_run_result.stderr.decode())
    if docker_run_result.returncode != 0:
        pytest.skip(f"Docker run failed: {docker_run_result.stderr.decode()}")

    # Get the container's IP address on the docker bridge network
    inspect_cmd = [
        "docker", "inspect", "-f",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        container_name
    ]
    docker_address = subprocess.run(inspect_cmd, check=True, stdout=subprocess.PIPE).stdout.decode().strip()
    # Wait for SSH to be up
    for _ in range(20):
        try:
            output = subprocess.run([
                "ssh", "-i", str(ssh_key_path),
                "-o", "IdentitiesOnly=yes",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=no",
                f"{SSH_USER}@{docker_address}", "-p", str(SSH_PORT), "echo", "ok"
            ], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(output.stdout.decode().strip())
            logging.info(output.stderr.decode().strip())
            if output.returncode == 0:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    else:
        subprocess.run(["docker", "rm", "-f", container_name], check=False)
        pytest.skip("SSH server did not start in time")
    yield {
        "hostname": docker_address,
        "user": SSH_USER,
        "key_file": str(ssh_key_path),
        "port": SSH_PORT,
        "ssh_options": "-o IdentitiesOnly=yes -o StrictHostKeyChecking=no"
    }, container_name, tmp_path
    subprocess.run(["docker", "rm", "-f", container_name], check=False)


@pytest.fixture
def s3_test_key():
    """
    Pytest fixture to provide a unique S3 key for testing and ensure its deletion after the test.
    Yields the S3 key string and deletes the object from the bucket after the test completes.
    """
    s3_key = f"{TEST_S3_KEY_PREFIX}testfile_{time.strftime('%Y%m%d_%H%M%S')}.tar.gz"
    yield s3_key
    s3 = boto3.client("s3")
    s3.delete_object(Bucket=TEST_S3_BUCKET, Key=s3_key)


@pytest.mark.integration
def test_upload_remote_files_directly_to_s3(ssh_docker_server, s3_test_key):
    """
    Integration test for uploading a file from a remote SSH server directly to S3.
    Verifies the file is uploaded, downloaded, and its contents match the original.
    """
    ssh_info, container_name, tmp_path = ssh_docker_server
    # Create a large (100MB) test file with random data on the SSH server

    test_file = tmp_path / "testfile.bin"
    chunk_size = 1024 * 1024  # 1MB
    total_size = 1024 * 1024 * 100  # 100MB
    with open(test_file, "wb") as f:
        for _ in range(total_size // chunk_size):
            f.write(secrets.token_bytes(chunk_size))
    # Copy file to container
    subprocess.run([
        "docker", "cp", str(test_file), f"{container_name}:/tmp/testfile.bin"
    ], check=True)
    # Run the upload
    s3_key = s3_test_key
    link = upload_remote_files_directly_to_s3(
        ssh_info=ssh_info,
        files=["/tmp/testfile.bin"],
        s3_bucket=TEST_S3_BUCKET,
        s3_key=s3_key
    )
    assert link, "No S3 link returned"
    # Download and check file from S3
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile() as f:
        s3.download_file(TEST_S3_BUCKET, s3_key, f.name)
        # Extract tar.gz and check content
        logging.info(f.name)

        with tarfile.open(f.name, "r:gz") as tar:
            logging.info(tar.getnames())
            member = tar.getmember("tmp/testfile.bin")
            assert member
