"""Host-side networking setup (TUN device and routes) for minicloud VM connectivity."""

import logging
import os
import subprocess

from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError

LOGGER = logging.getLogger(__name__)


def setup_host_networking(config: MinicloudConfig) -> None:
    """Extract and run minicloud-setup.sh on the host to configure TUN device and routes.

    minicloud requires a persistent TUN device (minicloud0) with IP 10.127.0.1/24 on the host
    for VM networking (IMDS, DNS, and host↔VM connectivity). The setup script is bundled inside
    the container image and must be run with sudo on the host before the container starts.
    """
    tun_name = "minicloud0"
    result = subprocess.run(
        ["ip", "addr", "show", tun_name],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0 and b"10.127.0.1" in result.stdout:
        LOGGER.info("Host networking already configured (%s has 10.127.0.1)", tun_name)
        return

    LOGGER.info("Configuring host networking for minicloud...")
    image = config.docker_image
    setup_script_path = os.path.join(config.state_dir, "minicloud-setup.sh")
    os.makedirs(config.state_dir, exist_ok=True)

    extract = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "cat", image, "/usr/local/bin/minicloud-setup.sh"],
        capture_output=True,
        check=False,
    )
    if extract.returncode != 0:
        # Fatal: without minicloud0 the guests boot but are unreachable — the container
        # would still pass API health checks and fail 20 minutes later on SSH timeouts.
        raise MinicloudError(
            f"could not extract minicloud-setup.sh from image {image}: "
            f"{extract.stderr.decode(errors='replace').strip()}"
        )

    with open(setup_script_path, "wb") as fh:
        fh.write(extract.stdout)
    os.chmod(setup_script_path, 0o755)

    # -n: without passwordless sudo this must fail immediately with a readable error rather
    # than block forever on a password prompt nobody is there to answer in CI.
    run_result = subprocess.run(
        ["sudo", "-n", setup_script_path],
        capture_output=True,
        check=False,
    )
    if run_result.returncode != 0:
        raise MinicloudError(
            f"minicloud-setup.sh failed (exit {run_result.returncode}): "
            f"{run_result.stderr.decode(errors='replace').strip()} — "
            f"the {tun_name} TUN device is required for VM networking (IMDS, DNS, host<->VM). "
            f"Passwordless sudo is required (the script runs via 'sudo -n')."
        )
    LOGGER.info("Host networking configured successfully")
