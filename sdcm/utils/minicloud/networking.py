"""Host-side networking setup (TUN device, routes and firewall zone) for minicloud VM connectivity."""

import logging
import os
import re
import shutil
import subprocess

from sdcm.utils.minicloud.config import (
    MINICLOUD_HOST_VPC_ROUTES,
    MINICLOUD_TUN_ADDR,
    MinicloudConfig,
    MinicloudError,
)

LOGGER = logging.getLogger(__name__)

TUN_NAME = "minicloud0"
FIREWALLD_TRUSTED_ZONE = "trusted"
# What minicloud-setup.sh hardcoded before scylladb/minicloud#187 made the ranges
# configurable. Its presence on the TUN device means the host was configured either by an
# older image or before this override existed - both black-hole the QA infra (Argus,
# argus-proxy live in 10.0.0.0/16) from any host inside AWS, so it must never survive.
LEGACY_BLANKET_ROUTE = "10.0.0.0/8"


def _route_on_tun(route: str) -> bool:
    result = subprocess.run(
        ["ip", "route", "show", route],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0 and f"dev {TUN_NAME}".encode() in result.stdout


def _host_networking_matches() -> bool:
    """True when the TUN device carries the desired address and exactly the desired routes.

    A long-lived host (lab agent) configured by an older image still has the legacy
    10.0.0.0/8 route - that must trigger reconfiguration, not an early return, or the sweep
    minicloud-setup.sh does on re-run never happens and the stale route lives forever.
    """
    result = subprocess.run(
        ["ip", "addr", "show", TUN_NAME],
        capture_output=True,
        check=False,
    )
    # Full `inet <addr>/<prefix>` token, not a substring: "10.127.0.1" would also accept
    # 10.127.0.10/24, skip setup, and leave guests unreachable on a wrong TUN endpoint.
    tun_token = re.compile(rf"inet {re.escape(MINICLOUD_TUN_ADDR)}(\s|$)".encode())
    if result.returncode != 0 or not tun_token.search(result.stdout):
        return False
    if _route_on_tun(LEGACY_BLANKET_ROUTE):
        return False
    return all(_route_on_tun(route) for route in MINICLOUD_HOST_VPC_ROUTES)


def _ensure_tun_in_firewalld_trusted_zone() -> None:
    """Ensure minicloud TUN device is in firewalld trusted zone so guest IMDS works.

    The assignment is runtime-only and is re-applied on every run because `firewall-cmd --reload`
    clears runtime zone bindings.
    """
    if shutil.which("firewall-cmd") is None:
        return

    firewalld_cmd = ["sudo", "-n", "firewall-cmd"]

    def run_firewalld(*args: str):
        return subprocess.run([*firewalld_cmd, *args], capture_output=True, check=False)

    state = run_firewalld("--state")
    if state.returncode != 0:
        output = (state.stdout + state.stderr).decode(errors="replace").strip()
        if "not running" in output:
            LOGGER.debug("firewalld is not running - no zone assignment needed for %s", TUN_NAME)
        else:
            LOGGER.warning("could not query firewalld state (%s) - skipping the %s zone assignment", output, TUN_NAME)
        return

    zone = run_firewalld(f"--get-zone-of-interface={TUN_NAME}")
    if zone.returncode == 0 and zone.stdout.decode(errors="replace").strip() == FIREWALLD_TRUSTED_ZONE:
        return

    # --change-interface handles both unzoned interfaces and zone moves
    change = run_firewalld(f"--zone={FIREWALLD_TRUSTED_ZONE}", f"--change-interface={TUN_NAME}")
    if change.returncode != 0:
        raise MinicloudError(
            f"could not move {TUN_NAME} into firewalld {FIREWALLD_TRUSTED_ZONE} zone (exit {change.returncode}): "
            f"{change.stderr.decode(errors='replace').strip()} - an unzoned {TUN_NAME} falls into the default zone, "
            f"which rejects the guests' IMDS traffic (DNAT'd through the TUN into the host INPUT chain): guests would "
            f"boot and get DHCP but never fetch their SSH key, failing every login with AuthenticationError. Fix "
            f"manually with 'sudo firewall-cmd --zone={FIREWALLD_TRUSTED_ZONE} --change-interface={TUN_NAME}'."
        )

    LOGGER.info(
        "moved %s into firewalld's trusted zone (runtime-only, re-applied every run) so guests can reach IMDS",
        TUN_NAME,
    )


def setup_host_networking(config: MinicloudConfig) -> None:
    """Extract and run minicloud-setup.sh on the host to configure TUN device and routes.

    minicloud requires a persistent TUN device (minicloud0) on the host for VM networking
    (IMDS, DNS, and host↔VM connectivity). The setup script is bundled inside the container
    image and must be run with sudo on the host before the container starts.

    The routed ranges are narrowed to MINICLOUD_HOST_VPC_ROUTES via the script's environment
    overrides (scylladb/minicloud#187) instead of its historical 10.0.0.0/8 default: the
    emulated VPCs live in the shifted 10.160.0.0/11 (see MINICLOUD_REGION_INDEX_OFFSET), and
    a blanket route would black-hole every real private IP - Argus included - on a host that
    itself sits in an AWS VPC, i.e. every sct-runner.

    On firewalld hosts the TUN device is also moved into the trusted zone (runtime-only, on every run).
    """
    if _host_networking_matches():
        LOGGER.info(
            "Host networking already configured (%s has %s and the expected routes)", TUN_NAME, MINICLOUD_TUN_ADDR
        )
        _ensure_tun_in_firewalld_trusted_zone()
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
        # sudo accepts leading VAR=value assignments; env set on this process would be
        # stripped by sudo's env_reset before the script could read it.
        [
            "sudo",
            "-n",
            f"MINICLOUD_TUN_ADDR={MINICLOUD_TUN_ADDR}",
            f"MINICLOUD_VPC_ROUTES={' '.join(MINICLOUD_HOST_VPC_ROUTES)}",
            setup_script_path,
        ],
        capture_output=True,
        check=False,
    )
    if run_result.returncode != 0:
        raise MinicloudError(
            f"minicloud-setup.sh failed (exit {run_result.returncode}): "
            f"{run_result.stderr.decode(errors='replace').strip()} — "
            f"the {TUN_NAME} TUN device is required for VM networking (IMDS, DNS, host<->VM). "
            f"Passwordless sudo is required (the script runs via 'sudo -n')."
        )

    # A pre-#187 script ignores the overrides silently and installs its hardcoded 10.0.0.0/8
    # - fail now with the reason, not 20 minutes later as Argus connect-timeouts.
    if not _host_networking_matches():
        raise MinicloudError(
            f"minicloud-setup.sh from image {image} did not apply the requested network "
            f"ranges (routes {' '.join(MINICLOUD_HOST_VPC_ROUTES)} on {TUN_NAME}, no "
            f"{LEGACY_BLANKET_ROUTE}). The image predates the MINICLOUD_VPC_ROUTES override "
            f"(scylladb/minicloud#187) - upgrade minicloud_docker to a release that includes it."
        )
    _ensure_tun_in_firewalld_trusted_zone()
    LOGGER.info("Host networking configured successfully (routes: %s)", ", ".join(MINICLOUD_HOST_VPC_ROUTES))
