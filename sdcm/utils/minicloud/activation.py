"""Activating a run against minicloud: reachability probe, env wiring, param validation."""

import os

import requests

from sdcm.utils.minicloud.config import MINICLOUD_HEALTH_ACTION, MinicloudError
from sdcm.utils.minicloud.endpoint import MINICLOUD_PORT, get_minicloud_endpoint
from sdcm.utils.session import create_retry_session


def check_minicloud_reachability(endpoint: str | None = None, timeout: int = 5) -> bool:
    """Check if minicloud is reachable at the given endpoint.

    Uses DescribeVpcs as the probe: it is implemented by minicloud, served locally
    (no passthrough to real AWS), and cheap. Do not use DescribeRegions — minicloud
    rejects it as an unimplemented action and logs a warning on every probe.

    Returns True if minicloud responds, raises RuntimeError with actionable message if not.
    """
    endpoint = endpoint or get_minicloud_endpoint()
    try:
        # retries=0: this is a liveness probe — failure IS the signal, and callers
        # (_wait_for_health, ensure_minicloud_ready) already poll in their own loops.
        response = create_retry_session(retries=0).post(
            endpoint,
            data={"Action": MINICLOUD_HEALTH_ACTION, "Version": "2016-11-15"},
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"minicloud at {endpoint} answered the {MINICLOUD_HEALTH_ACTION} health probe with "
                f"HTTP {response.status_code} instead of 200: {response.text[:500]}"
            )
        return True
    except requests.ConnectionError as exc:
        raise RuntimeError(
            f"minicloud is not reachable at {endpoint}. "
            f"Ensure minicloud is running: minicloud --port {MINICLOUD_PORT}\n"
            f"Connection error: {exc}"
        ) from exc
    except requests.Timeout as exc:
        raise RuntimeError(
            f"minicloud at {endpoint} timed out after {timeout}s. Is it overloaded or starting up?"
        ) from exc
    except requests.RequestException as exc:
        # Everything else the probe can raise (InvalidURL from a malformed
        # minicloud_endpoint_url, TooManyRedirects, ...) must still come out as
        # RuntimeError: callers retry on that alone, and an escaping RequestException
        # would bypass the retry loop and abort the run.
        raise RuntimeError(f"minicloud probe against {endpoint} failed: {exc}") from exc


def set_minicloud_endpoint_env(endpoint: str, backend: str) -> None:
    """Point the cloud SDKs at the minicloud endpoint for this process.

    Overwrites (never setdefault): a stale AWS_ENDPOINT_URL from an earlier container on
    a different port must not survive, and on GCE backends GCE_ENDPOINT_URL has to be set
    here too — env changes made by an earlier ``start-minicloud`` process never propagate
    to the next hydra invocation.
    """
    os.environ["AWS_ENDPOINT_URL"] = endpoint
    os.environ["SCT_MINICLOUD_ENDPOINT_URL"] = endpoint
    if backend in ("gce", "gce-siren"):
        os.environ["GCE_ENDPOINT_URL"] = endpoint


def validate_minicloud_params(params) -> None:
    """Fail fast when a minicloud run is missing the configurations/minicloud.yaml overlay.

    The overlay is the single delivery mechanism for the params a minicloud run requires
    (spot instances, public-IP SSH, KMS and iotune are all unsupported by the emulator).
    Env exports cannot substitute for it — SCTConfiguration is built before the manager
    starts — so a config list without the overlay would otherwise fail far from the cause:
    spot-provisioning errors, SSH to unreachable public IPs, KMS calls against an endpoint
    that serves no KMS API.
    """
    problems = []
    if params.get("instance_provision") != "on_demand":
        problems.append(f"instance_provision={params.get('instance_provision')!r} (need 'on_demand'; no spot market)")
    if params.get("ip_ssh_connections") != "private":
        problems.append(f"ip_ssh_connections={params.get('ip_ssh_connections')!r} (need 'private')")
    if not params.get("enterprise_disable_kms"):
        problems.append("enterprise_disable_kms is off (minicloud implements no KMS endpoint)")
    if params.get("force_run_iotune"):
        problems.append("force_run_iotune is on (iotune is pointless against emulated storage)")
    if problems:
        raise MinicloudError(
            "minicloud is active but the run is missing its parameter overlay: "
            + "; ".join(problems)
            + ". Add configurations/minicloud.yaml after the test-case yaml in the config list."
        )
