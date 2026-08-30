# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import os
import re
import json
import random
import logging
import time
import uuid
from functools import cached_property
from typing import Any, Callable, List, Literal, TYPE_CHECKING

import google.api_core.exceptions
from google.api_core import retry as api_core_retry
from google.oauth2 import service_account
from google.cloud import compute_v1
from google.cloud.compute_v1 import Image
from google.cloud import storage
from google.api_core.extended_operation import ExtendedOperation
from googleapiclient.discovery import build

from sdcm.keystore import KeyStore
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container

if TYPE_CHECKING:
    from sdcm.provision.provisioner import VmArch

# NOTE: we cannot use neither 'slim' nor 'alpine' versions because we need the 'beta' component be installed.
GOOGLE_CLOUD_SDK_IMAGE = "google/cloud-sdk:437.0.1"

LOGGER = logging.getLogger(__name__)


def gce_instance_name(node_prefix: str, dc_idx: int, node_index: int) -> str:
    """Generate a GCE instance name.

    This is the single source of truth for GCE instance naming convention.
    Used by both cluster_gce.py and gce_region_definition_builder.py.

    Args:
        node_prefix: The node prefix (e.g., 'user-db-node-a1b2c3d4')
        dc_idx: The datacenter index
        node_index: The node index within the datacenter

    Returns:
        The lowercase instance name in format: {node_prefix}-{dc_idx}-{node_index}
    """
    name = f"{node_prefix}-{dc_idx}-{node_index}".lower()
    # Name must start with a lowercase letter followed by up to 63
    # lowercase letters, numbers, or hyphens, and cannot end with a hyphen
    assert len(name) <= 63, f"Max length of instance name is 63, got {len(name)}: {name}"
    return name


def vmarch_to_gcp(arch: "VmArch") -> str:
    """Convert VmArch enum to GCP architecture format.

    Args:
        arch: VmArch enum value

    Returns:
        GCP architecture string (X86_64 or ARM64)

    Raises:
        ValueError: If architecture is not supported
    """
    # Lazy import to avoid circular dependency:
    # gce_utils -> provision.provisioner -> provision.__init__ -> provision.gce -> gce_utils
    from sdcm.provision.provisioner import VmArch  # noqa: PLC0415  # pylint: disable=import-outside-toplevel

    if arch is VmArch.X86:
        return "X86_64"
    elif arch is VmArch.ARM:
        return "ARM64"
    else:
        raise ValueError(f"Unsupported architecture: {arch}")


# Regions where SCT has infrastructure (VPCs, firewall rules, etc.)
SUPPORTED_REGIONS = [
    "us-east1",
    "us-east4",
    "us-west1",
    "us-central1",
]


SUPPORTED_PROJECTS = {"gcp-sct-project-1", "gcp-local-ssd-latency"} | {
    os.environ.get("SCT_GCE_PROJECT", "gcp-sct-project-1")
}


def _get_zone_letters_for_region(region: str) -> list[str]:
    """Query GCE Regions API to get available zone letters for a region."""
    try:
        regions_client, _ = get_gce_compute_regions_client()
        region_info = regions_client.get(project=KeyStore().get_gcp_credentials()["project_id"], region=region)
        return [z.rsplit("/", 1)[-1].split("-")[-1] for z in region_info.zones]
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to get zones from GCE API for region %s", region)
        return []


def random_zone(region: str) -> str:
    zone_letters = _get_zone_letters_for_region(region)
    if not zone_letters:
        raise Exception(f"No zones found for region: {region}")
    return random.choice(zone_letters)


def get_alternative_zones(region: str, exhausted_zone: str, machine_types: list[str] | None = None) -> list[str]:
    """Return alternative zone letters for a region, excluding the exhausted zone.

    Used by runtime fallback when provisioning fails with ZoneResourcesExhaustedError.
    If machine_types are provided, only zones supporting ALL of them are returned.
    """
    exhausted_letter = exhausted_zone[-1] if len(exhausted_zone) > 1 else exhausted_zone

    if machine_types:
        resolver = GceZoneResolver()
        common_zones = resolver.get_common_zones(
            region=region,
            machine_types=machine_types,
            preferred_zones=resolver.get_zones_for_region(region),
        )
        # Extract letters from full zone names (e.g., "us-east4-a" -> "a")
        valid_letters = [zone.split("-")[-1] for zone in common_zones]
        alternatives = [z for z in valid_letters if z != exhausted_letter]
    else:
        zone_letters = _get_zone_letters_for_region(region)
        if not zone_letters:
            return []
        alternatives = [z for z in zone_letters if z != exhausted_letter]

    return alternatives


def get_gce_compute_instances_client() -> tuple[compute_v1.InstancesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.InstancesClient(credentials=credentials), info


def get_gce_service_accounts() -> list[dict] | None:
    """Get GCP service accounts for instance creation (needed for KMS/API access).

    Returns:
        List of service account dicts with 'email' and 'scopes' keys, or None if not available.
    """
    try:
        return KeyStore().get_gcp_service_accounts()
    except Exception:  # noqa: BLE001
        return None


def get_gce_compute_images_client() -> tuple[compute_v1.ImagesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.ImagesClient(credentials=credentials), info


def get_gce_compute_addresses_client() -> tuple[compute_v1.AddressesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.AddressesClient(credentials=credentials), info


def get_gce_compute_regions_client() -> tuple[compute_v1.RegionsClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.RegionsClient(credentials=credentials), info


def get_gce_storage_client() -> tuple[storage.Client, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return storage.Client(credentials=credentials), info


def create_gce_storage_bucket(name: str, region: str, object_lock_enabled: bool = False) -> storage.Bucket:
    """Create a GCS bucket.

    Args:
        name: bucket name
        region: GCS region (e.g., 'us-east1')
        object_lock_enabled: if True, enables object retention (object lock) on the bucket.
                             Requires uniform bucket-level access (set automatically).

    Returns:
        the created Bucket object
    """
    storage_client, _ = get_gce_storage_client()

    bucket = storage_client.bucket(name)
    if object_lock_enabled:
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True

    storage_client.create_bucket(
        bucket,
        location=region,
        enable_object_retention=object_lock_enabled,
    )
    LOGGER.info("Created GCS bucket gs://%s in %s (object_lock_enabled=%s)", name, region, object_lock_enabled)
    return bucket


def gce_override_object_retention(bucket_name: str, path: str) -> None:
    """Override governance-mode object retention locks on blobs in a GCS bucket.

    Processes all matching blobs individually — if one blob fails, the rest are
    still attempted. Raises after all blobs have been processed if any failed.

    Args:
        bucket_name: the name of the GCS bucket
        path: path prefix to match blobs (empty string means all blobs)
    """
    storage_client, _ = get_gce_storage_client()

    if path.startswith("/"):
        path = path[1:]

    blobs = list(storage_client.list_blobs(bucket_or_name=bucket_name, prefix=path))
    if not blobs:
        LOGGER.warning("No blobs found in gs://%s/%s to unlock", bucket_name, path)
        return

    LOGGER.info("Overriding retention on %d blob(s) in gs://%s/%s", len(blobs), bucket_name, path)
    failed = []
    for blob in blobs:
        try:
            blob.retention.mode = None
            blob.retention.retain_until_time = None
            blob.patch(override_unlocked_retention=True)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to override retention on gs://%s/%s: %s", bucket_name, blob.name, exc)
            failed.append((blob.name, exc))
    if failed:
        raise RuntimeError(f"Failed to override retention on {len(failed)}/{len(blobs)} blob(s) in gs://{bucket_name}")


def get_gce_compute_disks_client() -> tuple[compute_v1.DisksClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.DisksClient(credentials=credentials), info


def get_gce_compute_machine_types_client() -> tuple[compute_v1.MachineTypesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.MachineTypesClient(credentials=credentials), info


class GceZoneResolver:
    """Resolves available zones for machine types in a GCE project/region."""

    def __init__(self, project: str | None = None):
        if project:
            self._project = project
        else:
            info = KeyStore().get_gcp_credentials()
            self._project = info["project_id"]
        self._machine_types_client, _ = get_gce_compute_machine_types_client()

    def get_zones_for_region(self, region: str) -> list[str]:
        try:
            regions_client, _ = get_gce_compute_regions_client()
            region_info = regions_client.get(project=self._project, region=region)
            return [z.rsplit("/", 1)[-1] for z in region_info.zones]
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to get zones from GCE API for region %s", region)
            return []

    def get_zones_for_machine_type(self, region: str, machine_type: str) -> list[str]:
        """Return zones in a region where the given machine type is available."""
        all_zones = self.get_zones_for_region(region)
        available = []
        for zone in all_zones:
            try:
                self._machine_types_client.get(project=self._project, zone=zone, machine_type=machine_type)
                available.append(zone)
            except google.api_core.exceptions.NotFound:
                continue
            except Exception:  # noqa: BLE001
                LOGGER.warning("Error checking machine type %s in zone %s; skipping", machine_type, zone)
                continue
        return available

    def get_common_zones(
        self,
        region: str,
        machine_types: list[str],
        preferred_zones: list[str] | None = None,
    ) -> list[str]:
        """Return zones in the region that support ALL given machine types."""
        if not machine_types:
            return []

        zones_per_type = [set(self.get_zones_for_machine_type(region, mt)) for mt in machine_types]
        common_zones = set.intersection(*zones_per_type) if zones_per_type else set()

        preferred_zones = preferred_zones or []
        missing = [z for z in preferred_zones if z not in common_zones]
        if missing:
            LOGGER.warning("Preferred zones %s do not support all required machine types %s", missing, machine_types)

        ordered = [z for z in preferred_zones if z in common_zones]
        ordered += [z for z in sorted(common_zones) if z not in ordered]

        LOGGER.info("Zones in %s supporting %s: %s", region, machine_types, ordered)
        return ordered

    def get_per_type_zones(self, region: str, machine_types: list[str]) -> dict[str, list[str]]:
        """Return a mapping of machine_type -> available zones in the region."""
        return {mt: self.get_zones_for_machine_type(region, mt) for mt in machine_types}


def gce_public_addresses(instance: compute_v1.Instance) -> list[str]:
    addresses = []

    for interface in instance.network_interfaces:
        for config in interface.access_configs:
            addresses.append(str(config.nat_i_p))

    return addresses


def gce_private_addresses(instance: compute_v1.Instance) -> list[str]:
    addresses = []

    for interface in instance.network_interfaces:
        addresses.append(str(interface.network_i_p))

    return addresses


def gce_mac_address_for_ipv4(ipv4_address: str | None) -> str | None:
    """Derive the MAC address GCE assigns to a NIC from that NIC's internal IPv4.

    GCE builds NIC MACs as `42:01:` followed by the four IPv4 octets in hex. The compute API does
    not report MACs, so this is how a cloud-side NIC is matched to its OS device name - and unlike
    matching by address, it also works while the interface is still waiting for DHCP.
    """
    if not ipv4_address:
        return None

    octets = str(ipv4_address).split(".")
    if len(octets) != 4:
        return None

    try:
        values = [int(octet) for octet in octets]
        if not all(0 <= value <= 255 for value in values):
            return None
    except ValueError:
        return None

    return "42:01:" + ":".join(f"{value:02x}" for value in values)


SECONDARY_NIC_ROUTING_SCRIPT_PATH = "/usr/local/sbin/sct-secondary-nic-routing.sh"
SECONDARY_NIC_ROUTING_SERVICE = "sct-secondary-nic-routing"

# GCE enforces source/NIC consistency for egress packets. Replies sourced from a secondary NIC IP
# are normally sent via the primary NIC default route and get dropped. This script adds source-based
# policy routing so traffic from each secondary NIC IP uses that NIC gateway.
# Values are resolved from metadata at runtime, so one script works for all nodes and can re-run
# after reboot or interface restart.
#
# Source-based policy routing is the method Google officially recommends for reaching secondary
# network interfaces from outside their subnet:
# https://cloud.google.com/vpc/docs/configure-routing-additional-interface
SECONDARY_NIC_ROUTING_SCRIPT = """\
#!/bin/bash
# auto-generated by SCT - policy routing for secondary network interfaces
set -euo pipefail

METADATA_URL="http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces"
# --retry-all-errors also retries connection failures during early boot, but needs curl >= 7.71
RETRY_ALL_ERRORS=$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors || true)
CURL="curl -sf --connect-timeout 5 --retry 3 --retry-delay 1 $RETRY_ALL_ERRORS -H Metadata-Flavor:Google"

# read secondary NIC indexes first; fail the unit on metadata errors
indexes=$($CURL "$METADATA_URL/" | tr -d / | grep -v "^0$")

configured=0
for index in $indexes; do
    ip_address=$($CURL "$METADATA_URL/$index/ip")
    gateway=$($CURL "$METADATA_URL/$index/gateway")
    mac=$($CURL "$METADATA_URL/$index/mac")
    device=$(ip -o link | awk -v mac="$mac" 'tolower($0) ~ tolower(mac) {gsub(":", "", $2); print $2; exit}')

    if [ -z "$device" ]; then
        echo "no OS device with MAC $mac (NIC $index), skipping" >&2
        continue
    fi

    # wait for DHCP to restore the NIC address after link down/up
    for _ in $(seq 60); do
        ip -4 -o addr show dev "$device" | grep -qF "inet $ip_address/" && break
        sleep 1
    done
    if ! ip -4 -o addr show dev "$device" | grep -qF "inet $ip_address/"; then
        echo "address $ip_address did not appear on $device after 60s, installing routes anyway" >&2
    fi

    # table 100+N avoids reserved tables and guest-agent-managed tables
    table=$((100 + index))
    ip route replace "$gateway" dev "$device" scope link table "$table"
    # on GCE, same-subnet traffic is still L3 via gateway, so default route is enough
    ip route replace default via "$gateway" dev "$device" table "$table"

    # script may run multiple times; remove old rule before adding the current one
    ip rule del from "$ip_address" lookup "$table" 2>/dev/null || true
    ip rule add from "$ip_address" lookup "$table" priority "$table"

    configured=$((configured + 1))
done
echo "configured policy routing for $configured secondary NIC(s)"
"""

SECONDARY_NIC_ROUTING_SERVICE_UNIT = f"""\
[Unit]
Description=SCT policy routing for secondary network interfaces
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart={SECONDARY_NIC_ROUTING_SCRIPT_PATH}
RemainAfterExit=yes
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


GCE_IMAGE_URL_REGEX = re.compile(
    r"https://www.googleapis.com/compute/v1/projects/(?P<project>.*)/global/images/(?P<image>.*)"
)


def get_gce_image_tags(link: str) -> dict:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    images_client = compute_v1.ImagesClient(credentials=credentials)

    image_params = GCE_IMAGE_URL_REGEX.search(link).groupdict()

    if image_params.get("image").startswith("family"):
        family = image_params.get("image").split("/")[-1]
        image: Image = images_client.get_from_family(family=family, project=image_params.get("project"))
    else:
        image: Image = images_client.get(**image_params)
    return image.labels


class GcloudContextManager:
    def __init__(self, instance: "GcloudContainerMixin", name: str):
        self._instance = instance
        self._name = name
        self._container = None
        self._span_counter = 0

    def _span_container(self):
        self._span_counter += 1
        if self._container:
            return
        try:
            self._container = self._instance._get_gcloud_container()
        except Exception as exc:  # noqa: BLE001
            try:
                ContainerManager.destroy_container(self._instance, self._name)
            except Exception:  # noqa: BLE001
                pass
            raise exc from None

    def _destroy_container(self):
        self._span_counter -= 1
        if self._span_counter != 0:
            return
        try:
            ContainerManager.destroy_container(self._instance, self._name)
        except Exception:  # noqa: BLE001
            pass
        self._container = None

    def __enter__(self):
        self._span_container()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._destroy_container()

    def run(self, command) -> str:
        one_time = self._container is None
        if one_time:
            self._span_container()
        try:
            command = f"gcloud {command}"
            if kube_config_path := getattr(self._instance, "kube_config_path", ""):
                command = f"KUBECONFIG={kube_config_path} {command}"
            LOGGER.debug("Execute `%s'", command)
            res = self._container.exec_run(["sh", "-c", command])
            if res.exit_code:
                raise DockerException(f"{self._container}: {res.output.decode('utf-8')}")
            return res.output.decode("utf-8")
        finally:
            if one_time:
                self._destroy_container()


class GcloudContainerMixin:
    """Run gcloud command using official Google Cloud SDK Docker image.

    See more details here: https://hub.docker.com/r/google/cloud-sdk
    """

    _gcloud_container_instance = None

    def gcloud_container_run_args(self) -> dict:
        user_home = os.path.expanduser("~")
        volumes = {
            user_home: {"bind": user_home, "mode": "rw"},
        }
        return dict(
            image=GOOGLE_CLOUD_SDK_IMAGE,
            command="cat",
            tty=True,
            name=f"{self.name}-gcloud",
            volumes=volumes,
            user=f"{os.getuid()}:{os.getgid()}",
            tmpfs={"/.config": f"size=50M,uid={os.getuid()}"},
            environment={},
        )

    def _get_gcloud_container(self) -> Container:
        """Create Google Cloud SDK container.

        Cloud SDK requires to enable some authorization method first.  Because of that we start a container which
        runs forever using `cat' command (like Jenkins do), put a service account credentials and activate them.

        All consequent gcloud commands run using container.exec_run() method.
        """
        container = ContainerManager.run_container(self, "gcloud")
        credentials = KeyStore().get_gcp_credentials()
        credentials["client_email"] = f"{credentials['client_email']}"
        shell_command = f"umask 077 && echo '{json.dumps(credentials)}' > /tmp/gcloud_svc_account.json"
        shell_command += " && echo 'kubeletConfig:\n  cpuManagerPolicy: static' > /tmp/system_config.yaml"
        # NOTE: use 'bash' in case of non-alpine sdk image and 'sh' when it is 'alpine' one.
        res = container.exec_run(["bash", "-c", shell_command])
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")
        res = container.exec_run(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                credentials["client_email"],
                "--key-file",
                "/tmp/gcloud_svc_account.json",
                "--project",
                credentials["project_id"],
            ]
        )
        if res.exit_code:
            raise DockerException(f"{container}[]: {res.output.decode('utf-8')}")
        return container

    @property
    def gcloud(self) -> GcloudContextManager:
        return GcloudContextManager(self, "gcloud")


class GkeClusterForCleaner:
    def __init__(self, cluster_info: dict, cleaner: "GkeCleaner"):
        self.cluster_info = cluster_info
        self.cleaner = cleaner

    @cached_property
    def metadata(self) -> dict:
        metadata = self.cluster_info["nodeConfig"]["metadata"].items()
        return {
            "items": [{"key": key, "value": value} for key, value in metadata],
        }

    @cached_property
    def name(self) -> str:
        return self.cluster_info["name"]

    @cached_property
    def zone(self) -> str:
        return self.cluster_info["zone"]

    def destroy(self):
        return self.cleaner.gcloud.run(f"container clusters delete {self.name} --zone {self.zone} --quiet")


class GkeCleaner(GcloudContainerMixin):
    _containers = {}
    tags = {}

    def __init__(self):
        self.name = f"gke-cleaner-{uuid.uuid4()!s:.8}"

    def list_gke_clusters(self) -> list:
        try:
            output = self.gcloud.run("container clusters list --format json")
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("`gcloud container clusters list --format json' failed to run: %s", exc)
        else:
            try:
                return [GkeClusterForCleaner(info, GkeCleaner()) for info in json.loads(output)]
            except json.JSONDecodeError as exc:
                LOGGER.error("Unable to parse output of `gcloud container clusters list --format json': %s", exc)
        return []

    def list_orphaned_gke_disks(self) -> dict:
        disks_per_zone = {}
        try:
            disks = json.loads(
                self.gcloud.run(
                    'compute disks list --format="json(name,zone)" --filter="(name~^gke-.*-pvc-.* OR name~^pvc-.*) '
                    'AND -users:*"'
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("`gcloud compute disks list' failed to run: %s", exc)
        else:
            for disk in disks:
                zone = disk["zone"].split("/")[-1]
                if zone not in disks_per_zone:
                    disks_per_zone[zone] = []
                disks_per_zone[zone].append(disk["name"])
        return disks_per_zone

    def clean_disks(self, disk_names: list[str], zone: str) -> None:
        self.gcloud.run(f"compute disks delete {' '.join(disk_names)} --zone {zone}")

    def __del__(self):
        ContainerManager.destroy_all_containers(self)


class GceLoggingClient:
    def __init__(self, instance_name: str, zone: str):
        credentials = KeyStore().get_gcp_credentials()
        self.credentials = service_account.Credentials.from_service_account_info(credentials)
        self.project_id = credentials["project_id"]
        self.instance_name = instance_name
        self.zone = zone

    def get_system_events(self, from_: float, until: float):
        """Gets system events logs entries from GCE between time (since -> to).

        Returns list of entries in a form of dictionaries. See example output in unit tests."""
        from_ = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(from_))
        until = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(until))
        query = {
            "resourceNames": [f"projects/{self.project_id}"],
            "filter": f'protoPayload.resourceName="projects/{self.project_id}/zones/{self.zone}/instances/{self.instance_name}"'
            f" logName : projects/{self.project_id}/logs/cloudaudit.googleapis.com%2Fsystem_event"
            f' timestamp > "{from_}" timestamp < "{until}"',
        }
        with build("logging", "v2", credentials=self.credentials, cache_discovery=False) as service:
            return self._get_log_entries(service, query)

    def _get_log_entries(self, service, query, page_token=None):
        if page_token:
            query.update({"page_token": page_token})
        ret = service.entries().list(body=query).execute()
        entries = ret.get("entries", [])
        if page_token := ret.get("nextPageToken"):
            entries.extend(self._get_log_entries(service, query, page_token))
        return entries


# Transient GCE API failures: the long-running operation itself is unaffected, only the HTTP call used to
# poll it failed, so the call is safe to repeat. Seen in the wild as
# "503 GET .../zones/<zone>/operations/<id>: Authentication backend unavailable" during node provisioning.
GCE_TRANSIENT_API_ERRORS = (
    google.api_core.exceptions.TooManyRequests,  # 429
    google.api_core.exceptions.InternalServerError,  # 500
    google.api_core.exceptions.BadGateway,  # 502
    google.api_core.exceptions.ServiceUnavailable,  # 503
    google.api_core.exceptions.GatewayTimeout,  # 504
)
GCE_OPERATION_RETRY_INITIAL_BACKOFF = 2.0
GCE_OPERATION_RETRY_MAX_BACKOFF = 30.0
GCE_OPERATION_RETRY_TIMEOUT = 180.0
# How many times a whole set_labels call (issue + poll) is replayed when it fails transiently.
GCE_SET_LABELS_RETRIES = 3
# A stale label fingerprint comes back as 412, which is worth replaying here too: the retry re-reads it.
GCE_SET_LABELS_ERRORS = GCE_TRANSIENT_API_ERRORS + (google.api_core.exceptions.PreconditionFailed,)


def gce_operation_poll_retry(
    timeout: float | None = None, on_error: Callable[[Exception], None] | None = None
) -> api_core_retry.Retry:
    """Build the retry policy for the HTTP calls that poll a GCE long-running operation.

    `ExtendedOperation.result()` gives up as soon as one `GET .../operations/<id>` poll returns a
    transient error, even though the operation itself is still progressing - which aborts a whole test
    when GCE has a short-lived hiccup. Polling is an idempotent GET, so those errors are retried with
    exponential backoff instead.

    Args:
        timeout: the caller's operation timeout; the retry deadline is capped by it so that retrying
            a poll cannot outlive the operation it is polling. `None` means the default deadline.
        on_error: (optional) called with every transient error that is retried.
    """
    retry_timeout = GCE_OPERATION_RETRY_TIMEOUT if timeout is None else min(GCE_OPERATION_RETRY_TIMEOUT, timeout)
    return api_core_retry.Retry(
        predicate=api_core_retry.if_exception_type(*GCE_TRANSIENT_API_ERRORS),
        initial=GCE_OPERATION_RETRY_INITIAL_BACKOFF,
        maximum=GCE_OPERATION_RETRY_MAX_BACKOFF,
        multiplier=2.0,
        timeout=retry_timeout,
        on_error=on_error,
    )


def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
) -> Any:
    """
    Waits for the extended (long-running) operation to complete.

    If the operation is successful, it will return its result.
    If the operation ends with an error, an exception will be raised.
    If there were any warnings during the execution of the operation
    they will be printed to sys.stderr.

    Args:
        operation: a long-running operation you want to wait on.
        verbose_name: (optional) a more verbose name of the operation,
            used only during error and warning reporting.
        timeout: how long (in seconds) to wait for operation to finish.
            If None, wait indefinitely.

    Returns:
        Whatever the operation.result() returns.

    Raises:
        This method will raise the exception received from `operation.exception()`
        or RuntimeError if there is no exception set, but there is an `error_code`
        set for the `operation`.

        In case of an operation taking longer than `timeout` seconds to complete,
        a `concurrent.futures.TimeoutError` will be raised.

        Transient GCE API errors hit while polling the operation (429, 5xx) are retried with
        exponential backoff and do not fail the call, see `gce_operation_poll_retry()`.
    """
    transient_errors: List[Exception] = []

    def _on_transient_error(exc: Exception) -> None:
        transient_errors.append(exc)
        LOGGER.warning(
            "Transient GCE API error while polling %s (occurrence #%s), retrying: %s",
            verbose_name,
            len(transient_errors),
            exc,
        )

    try:
        result = operation.result(timeout=timeout, retry=gce_operation_poll_retry(timeout, _on_transient_error))
    except TimeoutError as exc:
        # An exhausted poll retry surfaces as a plain timeout, which hides the actual GCE error - restate it.
        if transient_errors:
            raise TimeoutError(
                f"{verbose_name} did not complete within {timeout} seconds: polling it hit "
                f"{len(transient_errors)} transient GCE API error(s), the last one was: {transient_errors[-1]}"
            ) from exc
        raise

    if transient_errors:
        LOGGER.info(
            "%s completed after %s transient GCE API error(s) while polling", verbose_name, len(transient_errors)
        )

    if operation.error_code:
        LOGGER.debug("Error during %s: [Code: %s]: %s", verbose_name, operation.error_code, operation.error_message)
        LOGGER.debug("Operation ID: %s", operation.name)
        raise operation.exception() or RuntimeError(operation.error_message)

    if operation.warnings:
        LOGGER.debug("Warnings during %s:", verbose_name)
        for warning in operation.warnings:
            LOGGER.debug(" - %s: %s", warning.code, warning.message)

    return result


def disk_from_image(
    disk_type: str,
    boot: bool,
    disk_size_gb: int = None,
    source_image: str = None,
    auto_delete: bool = True,
    device_name: str = None,
    type_: Literal["PERSISTENT", "SCRATCH"] = None,
    interface: str = None,
) -> compute_v1.AttachedDisk:
    """
    Create an AttachedDisk object to be used in VM instance creation. Uses an image as the
    source for the new disk.

    Args:
         disk_type: the type of disk you want to create. This value uses the following format:
            "zones/{zone}/diskTypes/(pd-standard|pd-ssd|pd-balanced|pd-extreme)".
            For example: "zones/us-west3-b/diskTypes/pd-ssd"
        disk_size_gb: size of the new disk in gigabytes
        boot: boolean flag indicating whether this disk should be used as a boot disk of an instance
        source_image: source image to use when creating this disk. You must have read access to this disk. This can be one
            of the publicly available images or an image from one of your projects.
            This value uses the following format: "projects/{project_name}/global/images/{image_name}"
        auto_delete: boolean flag indicating whether this disk should be deleted with the VM that uses it
        device_name: (optional) name of the device
        type_: configure local storage disk or persistent disk 'PERSISTENT' or 'SCRATCH'
        interface: (optional) interface of the disk i.e. 'NVME'

    Returns:
        AttachedDisk object configured to be created using the specified image.
    """
    boot_disk = compute_v1.AttachedDisk()
    initialize_params = compute_v1.AttachedDiskInitializeParams()
    if source_image:
        initialize_params.source_image = source_image
    if disk_size_gb:
        initialize_params.disk_size_gb = disk_size_gb
    initialize_params.disk_type = disk_type
    boot_disk.initialize_params = initialize_params
    # Remember to set auto_delete to True if you want the disk to be deleted when you delete
    # your VM instance.
    boot_disk.auto_delete = auto_delete
    boot_disk.boot = boot
    if device_name:
        boot_disk.device_name = device_name
    if type_:
        boot_disk.type_ = type_
    if interface:
        boot_disk.interface = interface
    return boot_disk


def create_instance(  # noqa: PLR0913
    project_id: str,
    zone: str,
    instance_name: str,
    disks: List[compute_v1.AttachedDisk],
    machine_type: str = "n2-standard-1",
    network_name: str = None,
    subnetwork_link: str = None,
    internal_ip: str = None,
    external_access: bool = False,
    external_ipv4: str = None,
    accelerators: List[compute_v1.AcceleratorConfig] = None,
    spot: bool = False,
    instance_termination_action: str = "STOP",
    custom_hostname: str = None,
    delete_protection: bool = False,
    network_tags: list = None,
    metadata: dict = None,
    service_accounts: list = None,
) -> compute_v1.Instance:
    """
    Send an instance creation request to the Compute Engine API and wait for it to complete.

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        zone: name of the zone to create the instance in. For example: "us-west3-b"
        instance_name: name of the new virtual machine (VM) instance.
        disks: a list of compute_v1.AttachedDisk objects describing the disks
            you want to attach to your new instance.
        machine_type: machine type of the VM being created. This value uses the
            following format: "zones/{zone}/machineTypes/{type_name}".
            For example: "zones/europe-west3-c/machineTypes/f1-micro"
        network_name: name of the network you want the new instance to use.
            For example: "global/networks/default" represents the network
            named "default", which is created automatically for each project.
        subnetwork_link: name of the subnetwork you want the new instance to use.
            This value uses the following format:
            "regions/{region}/subnetworks/{subnetwork_name}"
        internal_ip: internal IP address you want to assign to the new instance.
            By default, a free address from the pool of available internal IP addresses of
            used subnet will be used.
        external_access: boolean flag indicating if the instance should have an external IPv4
            address assigned.
        external_ipv4: external IPv4 address to be assigned to this instance. If you specify
            an external IP address, it must live in the same region as the zone of the instance.
            This setting requires `external_access` to be set to True to work.
        accelerators: a list of AcceleratorConfig objects describing the accelerators that will
            be attached to the new instance.
        spot: boolean value indicating if the new instance should be a Spot VM or not.
        instance_termination_action: What action should be taken once a Spot VM is terminated.
            Possible values: "STOP", "DELETE"
        custom_hostname: Custom hostname of the new VM instance.
            Custom hostnames must conform to RFC 1035 requirements for valid hostnames.
        delete_protection: boolean value indicating if the new virtual machine should be
            protected against deletion or not.
        network_tags: List of tags to apply to network labels
        metadata: dict of key values to add to metadata
        service_accounts: list of service account to attach to the instance
    Returns:
        Instance object.
    """
    instance_client, _ = get_gce_compute_instances_client()

    # Use the network interface provided in the network_link argument.
    network_interface = compute_v1.NetworkInterface()
    network_interface.network = f"global/networks/{network_name}"
    if subnetwork_link:
        network_interface.subnetwork = subnetwork_link

    if internal_ip:
        network_interface.network_i_p = internal_ip

    if external_access:
        access = compute_v1.AccessConfig()
        access.type_ = compute_v1.AccessConfig.Type.ONE_TO_ONE_NAT.name
        access.name = "External NAT"
        access.network_tier = access.NetworkTier.PREMIUM.name
        if external_ipv4:
            access.nat_i_p = external_ipv4
        network_interface.access_configs = [access]

    # Collect information into the Instance object.
    instance = compute_v1.Instance()
    instance.network_interfaces = [network_interface]
    instance.name = instance_name
    instance.disks = disks
    if re.match(r"^zones/[a-z\d\-]+/machineTypes/[a-z\d\-]+$", machine_type):
        instance.machine_type = machine_type
    else:
        instance.machine_type = f"zones/{zone}/machineTypes/{machine_type}"

    if accelerators:
        instance.guest_accelerators = accelerators

    instance.scheduling = compute_v1.Scheduling()

    # Handle z3-highmem machine type special case
    # z3-highmem instances have built-in local SSDs and require MIGRATE on host maintenance
    # They cannot be spot instances because MIGRATE is incompatible with spot VMs
    if "z3-highmem" in machine_type:
        instance.scheduling.on_host_maintenance = "MIGRATE"
        instance.disks = [disk for disk in disks if "-data-local-ssd-" not in disk.device_name]
    elif spot:
        # Set the Spot VM setting
        # Spot VMs require on_host_maintenance to be TERMINATE as they cannot live migrate
        instance.scheduling.on_host_maintenance = "TERMINATE"
        instance.scheduling.provisioning_model = compute_v1.Scheduling.ProvisioningModel.SPOT.name
        instance.scheduling.instance_termination_action = instance_termination_action
    elif machine_type.split("/")[-1].startswith("e2-"):
        # e2 family supports only on_host_maintenance=MIGRATE for non-spot VMs
        instance.scheduling.on_host_maintenance = "MIGRATE"
    else:
        # avoid live migration and unexpected restarts disrupting tests
        instance.scheduling.on_host_maintenance = "TERMINATE"
        instance.scheduling.automatic_restart = False

    if custom_hostname is not None:
        # Set the custom hostname for the instance
        instance.hostname = custom_hostname

    if delete_protection:
        # Set the delete protection bit
        instance.deletion_protection = True

    if metadata:
        instance.metadata = compute_v1.Metadata()
        for key, value in metadata.items():
            instance.metadata.items += [({"key": key, "value": str(value)})]

    if service_accounts:
        instance.service_accounts += [compute_v1.ServiceAccount(**sa) for sa in service_accounts]
    if network_tags:
        instance.tags = compute_v1.Tags()
        instance.tags.items += network_tags
    # Prepare the request to insert an instance.
    request = compute_v1.InsertInstanceRequest()
    request.zone = zone
    request.project = project_id
    request.instance_resource = instance

    # Wait for the create operation to complete.
    LOGGER.debug("Creating the %s instance in %s...", instance_name, zone)

    operation = instance_client.insert(request=request)

    wait_for_extended_operation(operation, "instance creation")

    LOGGER.debug("Instance %s created.", instance_name)
    return instance_client.get(project=project_id, zone=zone, instance=instance_name)


def gce_set_labels(
    instances_client: compute_v1.InstancesClient,
    instance: compute_v1.Instance,
    new_labels: dict,
    project: str,
    zone: str,
):
    """
    Helper to do the set_labels operation correctly, without
    removing existing labels and applying the fingerprinting correctly

    Args:
        instances_client: client to execute the operation on.
        instance: the instance to label
        new_labels: dict of the labels to apply
        project: the project id to use
        zone: the zone instance is in

    Returns:
        Whatever the operation.result() returns.

    Raises:
        The last transient GCE API error if `GCE_SET_LABELS_RETRIES` attempts all failed, or whatever
        `wait_for_extended_operation()` raises for a non-transient failure.
    """
    last_error = None
    for attempt in range(1, GCE_SET_LABELS_RETRIES + 1):
        if attempt > 1:
            # The label fingerprint is invalidated by any concurrent label change and by a request that
            # reached GCE before the error did, so re-read the instance instead of replaying the old one.
            instance = instances_client.get(project=project, zone=zone, instance=instance.name)

        request = compute_v1.InstancesSetLabelsRequest()
        request.labels = instance.labels
        request.label_fingerprint = instance.label_fingerprint
        request.labels.update(new_labels)

        try:
            operation = instances_client.set_labels(
                project=project, zone=zone, instance=instance.name, instances_set_labels_request_resource=request
            )
            return wait_for_extended_operation(operation, f"setting labels on {instance.name}")
        except GCE_SET_LABELS_ERRORS as exc:
            last_error = exc
            if attempt == GCE_SET_LABELS_RETRIES:
                break
            backoff = min(GCE_OPERATION_RETRY_INITIAL_BACKOFF * 2 ** (attempt - 1), GCE_OPERATION_RETRY_MAX_BACKOFF)
            LOGGER.warning(
                "Failed to set labels on %s (attempt %s/%s), retrying in %ss: %s",
                instance.name,
                attempt,
                GCE_SET_LABELS_RETRIES,
                backoff,
                exc,
            )
            time.sleep(backoff)

    LOGGER.error("Failed to set labels on %s after %s attempts", instance.name, GCE_SET_LABELS_RETRIES)
    raise last_error


def gce_set_tags(
    instances_client: compute_v1.InstancesClient, instance: compute_v1.Instance, new_tags: list, project: str, zone: str
):
    """
    Helper to do the set_tags operation correctly, without
    removing existing tags and applying the fingerprinting correctly

    Args:
        instances_client: client to execute the operation on.
        instance: the instance to label
        new_tags: list of the tags to apply
        project: the project id to use
        zone: the zone instance is in

    Returns:
        Whatever the operation.result() returns.
    """

    request = compute_v1.SetTagsInstanceRequest()

    request.tags_resource = instance.tags
    request.tags_resource.items.extend(new_tags)
    request.tags_resource.items = list(set(request.tags_resource.items))
    request.zone = zone
    request.project = project
    request.instance = instance.name

    operation = instances_client.set_tags(request=request)

    return wait_for_extended_operation(operation, f"setting tags on {instance.name}")


def gce_check_if_machine_type_supported(
    machine_types_client: compute_v1.MachineTypesClient, machine_type: str, project: str, zone: str
):
    """
    Check if the given machine type is supported in the given zone.
    """
    request = compute_v1.GetMachineTypeRequest()
    request.project = project
    request.zone = zone
    request.machine_type = machine_type
    return machine_types_client.get(request=request)
