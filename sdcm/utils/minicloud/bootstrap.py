"""ensure_minicloud_ready: adopt a healthy container or auto-start a fresh one."""

import logging
import time

from sdcm.utils.minicloud.activation import check_minicloud_reachability, set_minicloud_endpoint_env
from sdcm.utils.minicloud.config import MinicloudConfig
from sdcm.utils.minicloud.endpoint import get_minicloud_endpoint
from sdcm.utils.minicloud.manager import MinicloudManager

LOGGER = logging.getLogger(__name__)


def ensure_minicloud_ready(backend: str = "aws", params=None) -> None:
    """Ensure minicloud is running and the endpoint env vars are set.

    If minicloud is already running, validates reachability and sets env vars.
    If not running after retries, auto-starts it with keep_alive=True.

    Retries reachability with exponential backoff to handle transient
    unavailability (e.g., minicloud briefly overloaded or restarting).

    Never call this from evidence-collection or cleanup paths: the auto-start
    force-removes a dead container. Those paths must use
    ``check_minicloud_reachability`` and fail closed instead.
    """
    endpoint = get_minicloud_endpoint(params)
    max_retries = 4
    for attempt in range(max_retries):
        try:
            check_minicloud_reachability(endpoint, timeout=5)
            set_minicloud_endpoint_env(endpoint, backend)
            return
        except RuntimeError:
            if attempt < max_retries - 1:
                wait = 2**attempt
                LOGGER.warning(
                    "Minicloud not reachable at %s (attempt %d/%d), retrying in %ds...",
                    endpoint,
                    attempt + 1,
                    max_retries,
                    wait,
                )
                time.sleep(wait)

    LOGGER.info("Minicloud not reachable after %d attempts — auto-starting", max_retries)
    cfg = MinicloudConfig.from_env(params=params)
    if not cfg.backend:
        cfg.backend = backend
    manager = MinicloudManager(cfg)
    manager.keep_alive = True
    # Full preflight: this path is also the FIRST start for standalone run-test /
    # provision-resources invocations, where nothing has validated credentials or host
    # memory yet. Skipping validation here deferred bad credentials into opaque
    # image-download/S3 failures mid-provisioning.
    manager.preflight_check(params=params)
    manager.start()
    if backend in ("aws", "aws-siren"):
        manager.prepare_regions()
    elif backend in ("gce", "gce-siren"):
        manager.prepare_gce_network()
