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
from typing import Any, List, Literal

from google.oauth2 import service_account
from google.cloud import compute_v1
from google.cloud.compute_v1 import Image
from google.cloud import storage
from google.api_core.extended_operation import ExtendedOperation
from googleapiclient.discovery import build

from sdcm.keystore import KeyStore
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container


# NOTE: we cannot use neither 'slim' nor 'alpine' versions because we need the 'beta' component be installed.
GOOGLE_CLOUD_SDK_IMAGE = "google/cloud-sdk:437.0.1"

LOGGER = logging.getLogger(__name__)

# The keys are the region name, the value is the available zones, which will be used for random.choice()
SUPPORTED_REGIONS = {
    # us-east1 zones: b, c, and d. Details: https://cloud.google.com/compute/docs/regions-zones#locations
    # Currently choose only zones c and d as zone b frequently fails allocating resources.
    'us-east1': 'cd',
    'us-east4': 'abc',
    'us-west1': 'abc',
    'us-central1': 'a',
}


SUPPORTED_PROJECTS = {'gcp-sct-project-1',
                      'gcp-local-ssd-latency'} | {os.environ.get('SCT_GCE_PROJECT', 'gcp-sct-project-1')}


def random_zone(region: str) -> str:
    availability_zones = SUPPORTED_REGIONS.get(region, None)
    if not availability_zones:
        raise Exception(f'Unsupported region: {region}')
    return f"{random.choice(availability_zones)}"


def get_gce_compute_instances_client() -> tuple[compute_v1.InstancesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.InstancesClient(credentials=credentials), info


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


def get_gce_compute_disks_client() -> tuple[compute_v1.DisksClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.DisksClient(credentials=credentials), info


def get_gce_compute_machine_types_client() -> tuple[compute_v1.MachineTypesClient, dict]:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    return compute_v1.MachineTypesClient(credentials=credentials), info


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


GCE_IMAGE_URL_REGEX = re.compile(
    r'https://www.googleapis.com/compute/v1/projects/(?P<project>.*)/global/images/(?P<image>.*)')


def get_gce_image_tags(link: str) -> dict:
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    images_client = compute_v1.ImagesClient(credentials=credentials)

    image_params = GCE_IMAGE_URL_REGEX.search(link).groupdict()

    if image_params.get('image').startswith('family'):
        family = image_params.get('image').split('/')[-1]
        image: Image = images_client.get_from_family(family=family, project=image_params.get('project'))
    else:
        image: Image = images_client.get(**image_params)
    return image.labels


class GcloudContextManager:
    def __init__(self, instance: 'GcloudContainerMixin', name: str):
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
        return dict(image=GOOGLE_CLOUD_SDK_IMAGE,
                    command="cat",
                    tty=True,
                    name=f"{self.name}-gcloud",
                    volumes=volumes,
                    user=f"{os.getuid()}:{os.getgid()}",
                    tmpfs={'/.config': f'size=50M,uid={os.getuid()}'},
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
        res = container.exec_run(["gcloud", "auth", "activate-service-account", credentials["client_email"],
                                  "--key-file", "/tmp/gcloud_svc_account.json",
                                  "--project", credentials["project_id"]])
        if res.exit_code:
            raise DockerException(f"{container}[]: {res.output.decode('utf-8')}")
        return container

    @property
    def gcloud(self) -> GcloudContextManager:
        return GcloudContextManager(self, 'gcloud')


class GkeClusterForCleaner:
    def __init__(self, cluster_info: dict, cleaner: "GkeCleaner"):
        self.cluster_info = cluster_info
        self.cleaner = cleaner

    @cached_property
    def metadata(self) -> dict:
        metadata = self.cluster_info["nodeConfig"]["metadata"].items()
        return {"items": [{"key": key, "value": value} for key, value in metadata], }

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
                LOGGER.error(
                    "Unable to parse output of `gcloud container clusters list --format json': %s",
                    exc)
        return []

    def list_orphaned_gke_disks(self) -> dict:
        disks_per_zone = {}
        try:
            disks = json.loads(self.gcloud.run(
                'compute disks list --format="json(name,zone)" --filter="(name~^gke-.*-pvc-.* OR name~^pvc-.*) '
                'AND -users:*"'))
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
        self.project_id = credentials['project_id']
        self.instance_name = instance_name
        self.zone = zone

    def get_system_events(self, from_: float, until: float):
        """Gets system events logs entries from GCE between time (since -> to).

        Returns list of entries in a form of dictionaries. See example output in unit tests."""
        from_ = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(from_))
        until = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(until))
        query = {
            "resourceNames": [
                f"projects/{self.project_id}"
            ],
            "filter": f'protoPayload.resourceName="projects/{self.project_id}/zones/{self.zone}/instances/{self.instance_name}"'
            f' logName : projects/{self.project_id}/logs/cloudaudit.googleapis.com%2Fsystem_event'
            f' timestamp > "{from_}" timestamp < "{until}"'
        }
        with build('logging', 'v2', credentials=self.credentials, cache_discovery=False) as service:
            return self._get_log_entries(service, query)

    def _get_log_entries(self, service, query, page_token=None):
        if page_token:
            query.update({"page_token": page_token})
        ret = service.entries().list(body=query).execute()
        entries = ret.get('entries', [])
        if page_token := ret.get("nextPageToken"):
            entries.extend(self._get_log_entries(service, query, page_token))
        return entries


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
    """
    result = operation.result(timeout=timeout)

    if operation.error_code:
        LOGGER.debug("Error during %s: [Code: %s]: %s", verbose_name,
                     operation.error_code, operation.error_message)
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

    if "z3-highmem" in machine_type:
        instance.scheduling.on_host_maintenance = "MIGRATE"
        instance.disks = [disk for disk in disks if "-data-local-ssd-" not in disk.device_name]

    if spot:
        # Set the Spot VM setting
        instance.scheduling.provisioning_model = (
            compute_v1.Scheduling.ProvisioningModel.SPOT.name
        )
        instance.scheduling.instance_termination_action = instance_termination_action

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


def gce_set_labels(instances_client: compute_v1.InstancesClient,
                   instance: compute_v1.Instance,
                   new_labels: dict,
                   project: str,
                   zone: str):
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
    """

    request = compute_v1.InstancesSetLabelsRequest()
    request.labels = instance.labels
    request.label_fingerprint = instance.label_fingerprint
    request.labels.update(new_labels)

    operation = instances_client.set_labels(project=project,
                                            zone=zone,
                                            instance=instance.name,
                                            instances_set_labels_request_resource=request)

    return wait_for_extended_operation(operation, f"setting labels on {instance.name}")


def gce_set_tags(instances_client: compute_v1.InstancesClient,
                 instance: compute_v1.Instance,
                 new_tags: list,
                 project: str,
                 zone: str):
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
        machine_types_client: compute_v1.MachineTypesClient,
        machine_type: str,
        project: str,
        zone: str):
    """
    Check if the given machine type is supported in the given zone.
    """
    request = compute_v1.GetMachineTypeRequest()
    request.project = project
    request.zone = zone
    request.machine_type = machine_type
    return machine_types_client.get(request=request)
