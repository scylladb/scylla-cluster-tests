"""Azure credential, TLS wait and address-space helpers for the minicloud azure backend."""

import logging
import os

import requests
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient

from sdcm.keystore import KeyStore
from sdcm.utils.minicloud.config import (
    MINICLOUD_AZURE_REGION_INDEX_OFFSET,
    MINICLOUD_AZURE_REGIONS,
    MINICLOUD_AZURE_SCRATCH_RG_LOCATION,
    MINICLOUD_HEALTH_INTERVAL,
    MINICLOUD_HEALTH_TIMEOUT,
    MinicloudConfig,
    MinicloudError,
)
from sdcm.utils.minicloud.endpoint import AZURE_BACKENDS
from sdcm.utils.minicloud.tls import build_ca_bundle
from sdcm.wait import wait_for

LOGGER = logging.getLogger(__name__)

MINICLOUD_TLS_PROBE_TIMEOUT = 2


def write_azure_env_file(path: str, creds: dict) -> None:
    """Write the service-principal identity as a docker env-file readable by the owner only."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        os.fchmod(fh.fileno(), 0o600)
        fh.write(f"AZURE_TENANT_ID={creds['tenant_id']}\n")
        fh.write(f"AZURE_CLIENT_ID={creds['client_id']}\n")
        fh.write(f"AZURE_CLIENT_SECRET={creds['client_secret']}\n")


def azure_vnet_address_space(region: str) -> str:
    """Return the minicloud VNet address space for the region.

    Each region gets its own /16, so guest addresses do not overlap.
    """
    try:
        index = MINICLOUD_AZURE_REGIONS.index(region)
    except ValueError:
        raise MinicloudError(
            f"region '{region}' is not supported by the minicloud azure backend, "
            f"supported regions: {MINICLOUD_AZURE_REGIONS}"
        ) from None

    return f"10.{MINICLOUD_AZURE_REGION_INDEX_OFFSET + index}.0.0/16"


def check_azure_credentials(creds: dict) -> None:
    """Fail early on a bad service principal, instead of on the first passthrough request."""
    try:
        ClientSecretCredential(creds["tenant_id"], creds["client_id"], creds["client_secret"]).get_token(
            "https://management.azure.com/.default"
        )
    except Exception as exc:
        raise MinicloudError(
            f"Azure service-principal credentials from KeyStore azure.json are rejected by Entra ID: {exc}"
        ) from exc


def ensure_scratch_resource_group(creds: dict, name: str, location: str) -> None:
    """Create minicloud's scratch resource group in real Azure, if missing.

    Minicloud needs it to export images through passthrough.
    """
    credential = ClientSecretCredential(creds["tenant_id"], creds["client_id"], creds["client_secret"])
    client = ResourceManagementClient(credential, creds["subscription_id"])
    client.resource_groups.create_or_update(
        name, {"location": location, "tags": {"purpose": "minicloud-scratch", "keep": "alive"}}
    )
    LOGGER.info("Scratch resource group %s is ready in %s", name, location)


def setup_azure_credentials(config: MinicloudConfig, backend: str) -> None:
    """Load the service principal from KeyStore, validate it and hand it to the container via an env-file."""
    if backend not in AZURE_BACKENDS:
        return

    try:
        creds = KeyStore().get_azure_credentials()
    except Exception as exc:
        raise MinicloudError(
            "failed to download Azure credentials from KeyStore - the azure backend "
            "requires them for passthrough to real Azure"
        ) from exc

    check_azure_credentials(creds)
    ensure_scratch_resource_group(creds, config.azure_scratch_resource_group, MINICLOUD_AZURE_SCRATCH_RG_LOCATION)
    os.makedirs(config.state_dir, exist_ok=True)
    write_azure_env_file(config.azure_env_file, creds)
    config.azure_subscription_id = creds["subscription_id"]


def wait_for_tls(config: MinicloudConfig, timeout: int = MINICLOUD_HEALTH_TIMEOUT) -> str:
    """Wait for minicloud's Azure API to answer over TLS and return the CA bundle path."""
    wait_for(
        lambda: os.path.isfile(config.minicloud_cert_path),
        step=MINICLOUD_HEALTH_INTERVAL,
        timeout=timeout,
        text=f"minicloud TLS certificate {config.minicloud_cert_path}",
    )
    # built outside the wait: a bundle that cannot be built fails at once instead of being retried
    bundle = build_ca_bundle(config)

    def tls_endpoint_answers() -> bool:
        url = f"{config.tls_endpoint}/subscriptions?api-version=2020-01-01"
        return requests.get(url, verify=bundle, timeout=MINICLOUD_TLS_PROBE_TIMEOUT).status_code == 200

    wait_for(
        tls_endpoint_answers,
        step=MINICLOUD_HEALTH_INTERVAL,
        timeout=timeout,
        text=f"minicloud TLS endpoint {config.tls_endpoint}",
    )
    return bundle
