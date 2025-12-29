import os
import sys
from datetime import datetime, timedelta
from functools import partial
from typing import Literal, Callable

from argus.client.sct.client import ArgusSCTClient

from sdcm.keystore import KeyStore
from sdcm.utils.log import setup_stdout_logger

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
LOGGER = setup_stdout_logger()
DEFAULT_KEEP_HOURS = 14


def argus_client_factory() -> Callable[[str], ArgusSCTClient]:
    """Factory function to create ArgusSCTClient instances with pre-configured credentials."""
    creds = KeyStore().get_argus_rest_credentials_per_provider()
    return partial(
        ArgusSCTClient, auth_token=creds["token"], base_url=creds["baseUrl"], extra_headers=creds.get("extra_headers")
    )


argus_client = argus_client_factory()


def update_argus_resource_status(test_id: str, resource_name: str, action: Literal["terminate", "stop"]):
    if not test_id and not resource_name:
        LOGGER.error("Skip update Argus due missing test_id and resource_name")
        return
    try:
        client = argus_client(test_id)
        client.terminate_resource(name=resource_name, reason=f"cloud-cleanup: {action} resource due to expiration")
    except Exception as exc:  # noqa: BLE001 catching all to make sure it does not break the main process
        LOGGER.error("Failed to update Argus resource status: %s", exc)
        return


def should_keep(creation_time: datetime, keep_hours: int) -> bool:
    """
    Determine if a resource should be kept based on creation time and keep duration.

    Args:
        creation_time: The datetime when the resource was created
        keep_hours: Number of hours to keep the resource. If <= 0, always keep.

    Returns:
        True if the resource should be kept, False if it can be cleaned up
    """
    if keep_hours <= 0:
        return True
    try:
        keep_date = creation_time + timedelta(hours=keep_hours)
        now = datetime.utcnow()
        return now < keep_date
    except (TypeError, ValueError) as exc:
        LOGGER.info("error while defining if should keep: %s. Keeping.", exc)
        return True


def get_keep_hours_from_tags(tags_dict: dict, default: int = DEFAULT_KEEP_HOURS) -> int:
    """
    Extract keep duration from resource tags.

    Args:
        tags_dict: Dictionary containing resource tags/metadata
        default: Default number of hours to keep if not specified

    Returns:
        Number of hours to keep the resource. Returns -1 for "alive" (keep forever).
    """
    keep = tags_dict.get("keep", "").lower() if tags_dict else None
    if keep == "alive":
        return -1
    try:
        return int(keep)
    except (ValueError, TypeError):
        return default
