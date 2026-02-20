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
# Copyright (c) 2026 ScyllaDB

import base64
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Dict, List, Optional

import oci
from oci.core.models import (
    LaunchInstanceDetails,
    CreateVnicDetails,
    Instance,
    PreemptionAction,
    PreemptibleInstanceConfigDetails,
    CreateVolumeDetails,
    LaunchAttachParavirtualizedVolumeDetails,
)
import pytz

from sdcm.provision.oci.constants import (
    OCI_REGION_NAMES_MAPPING,
    TAG_NAMESPACE,
)
from sdcm.provision.provisioner import (
    InstanceDefinition,
    PricingModel,
    ProvisionError,
    VmInstance,
)
from sdcm.provision.user_data import UserDataBuilder
from sdcm.utils.oci_region import OciRegion
from sdcm.utils.oci_utils import OciService
from sdcm.utils.parallel_object import ParallelObject

LAUNCH_TIME_FORMAT = "%B %d, %Y, %H:%M:%S"
LOGGER = logging.getLogger(__name__)


def get_current_datetime_formatted() -> str:
    return datetime.now(tz=pytz.utc).strftime(LAUNCH_TIME_FORMAT)


@dataclass
class VirtualMachineProvider:
    _compartment_id: str
    _region: str
    _az: str
    _oci_service: OciService = OciService()
    _raw_cache: Dict[str, Instance] = field(default_factory=dict)  # Map DisplayName -> Instance Object

    def __post_init__(self):
        oci_service = OciService()
        self._compute_client = oci_service.get_compute_client(self._region)
        self._identity_client = oci_service.get_identity_client(self._region)
        self._network_client = oci_service.get_network_client(self._region)
        self._bs_client = oci_service.get_block_storage_client(self._region)

    def _get_availability_domain(self, definition: Optional[InstanceDefinition] = None) -> str:
        """
        Resolves the full Availability Domain name.
        Uses self._az which can be a single value ("1"), a list ("1,2,3"), or letters ("a,b,c").
        If a list is provided, distributes based on definition's NodeIndex.
        """
        ads = self._identity_client.list_availability_domains(self._compartment_id).data
        ads.sort(key=lambda x: x.name)
        az_list = [x.strip() for x in str(self._az).split(",")]
        az_to_use = az_list[0]
        if definition and len(az_list) > 1:
            try:
                node_index = int(definition.tags.get("NodeIndex", 1))
                az_to_use = az_list[(node_index - 1) % len(az_list)]
            except (ValueError, TypeError):
                LOGGER.warning(
                    "Could not determine valid NodeIndex for instance %s, using first AZ: %s",
                    definition.name,
                    az_to_use,
                )

        # Check if it's already a full AD name
        for ad in ads:
            if ad.name == az_to_use:
                return ad.name

        # Try index mapping
        index = -1
        if az_to_use.isdigit():
            index = int(az_to_use) - 1
        elif len(az_to_use) == 1 and az_to_use.isalpha():
            index = ord(az_to_use.lower()) - ord("a")
        if 0 <= index < len(ads):
            return ads[index].name

        # Try suffix matching
        for ad in ads:
            if ad.name.endswith(str(az_to_use)):
                return ad.name

        raise ProvisionError(
            f"Invalid or not found Availability Zone identifier '{az_to_use}' (derived from configuration '{self._az}') "
            f"in region {self._region}. Available domains: {[ad.name for ad in ads]}"
        )

    def _create_volumes(self, definition: InstanceDefinition) -> list[str]:
        """
        Useful for non-dense shapes (instance types)
        """
        disk_num = getattr(definition, "data_volume_disk_num", 0)
        if disk_num < 1:
            return []
        disk_size = getattr(definition, "data_volume_disk_size", 0)
        disk_type = str(getattr(definition, "data_volume_disk_type", "balanced")).lower()
        vpus_per_gb = 10  # Default Balanced
        if disk_type == "lower_cost":
            vpus_per_gb = 0
        elif disk_type in ("balanced", "gp2", "gp3"):
            vpus_per_gb = 10
        elif disk_type in ("higher_performance", "io1", "io2", "vpuhigh"):
            vpus_per_gb = 20
        elif "ultra" in disk_type:
            vpus_per_gb = 30  # Start of ultra range
        else:
            raise ProvisionError(f"Got unexpected value for the 'data_volume_disk_type' config option: {disk_type}")
        availability_domain = self._get_availability_domain(definition)
        created_volume_ids = []
        for i in range(disk_num):
            vol_name = f"{definition.name}-vol-{i}"
            try:
                LOGGER.info("Creating %sGB volume '%s' in %s...", disk_size, vol_name, availability_domain)
                vol_details = CreateVolumeDetails(
                    compartment_id=self._compartment_id,
                    availability_domain=availability_domain,
                    display_name=vol_name,
                    size_in_gbs=disk_size,
                    vpus_per_gb=vpus_per_gb,
                )
                try:
                    vol = self._bs_client.create_volume(vol_details).data
                    vol = self._wait_for_volume_state(vol.id, "AVAILABLE")
                except oci.exceptions.ServiceError as e:
                    raise ProvisionError(f"Failed to create volume {vol_name}: {e}")

                created_volume_ids.append(vol.id)
            except Exception as e:  # noqa: BLE001
                LOGGER.error("Failed to prepare volume %s for instance %s: %s", vol_name, definition.name, e)
                for vid in created_volume_ids:
                    try:
                        self._bs_client.delete_volume(vid)
                    except Exception:  # noqa: BLE001
                        pass
                raise e
        return created_volume_ids

    def _wait_for_volume_state(self, volume_id: str, state: str, timeout: int = 300):
        return oci.wait_until(
            self._bs_client, self._bs_client.get_volume(volume_id), "lifecycle_state", state, max_wait_seconds=timeout
        ).data

    def list_instances(self, test_id: str) -> List[Instance]:
        """List instances in the compartment, optionally filtered by tags (test_id)."""
        all_instances = oci.pagination.list_call_get_all_results(
            self._compute_client.list_instances, compartment_id=self._compartment_id
        ).data
        filtered = []
        for inst in all_instances:
            if inst.lifecycle_state == Instance.LIFECYCLE_STATE_TERMINATED:
                continue
            tags = (inst.defined_tags or {}).get(TAG_NAMESPACE, {})
            if test_id and tags.get("TestId") != test_id and test_id not in inst.display_name:
                continue
            filtered.append(inst)
            self._raw_cache[inst.display_name] = inst
        return filtered

    def _get_shape_config(self, definition: InstanceDefinition) -> tuple[str, dict]:
        shape_type, shape_config = definition.type, {}
        if "Flex" in shape_type:
            # NOTE: Flex shapes require explict OCPUs/Memory/NVMe
            #       Support syntax "ShapeName:OCPUs:MemoryGB:NVMe"
            #       Example: "VM.DenseIO.E5.Flex:8:96:1"
            if ":" in shape_type:
                parts = shape_type.split(":")
                shape_type, parts_num = parts[0], len(parts)
                if parts_num > 1:
                    try:
                        shape_config["ocpus"] = float(parts[1])
                    except (ValueError, IndexError):
                        LOGGER.warning("Failed to parse out the OCPUs config from the shape %s", definition.type)
                if parts_num > 2:
                    try:
                        shape_config["memory_in_gbs"] = float(parts[2])
                    except (ValueError, IndexError):
                        LOGGER.warning("Failed to parse out the Memory config from the shape %s", definition.type)
                if parts_num > 3:
                    try:
                        shape_config["nvmes"] = int(parts[3])
                    except (ValueError, IndexError):
                        LOGGER.warning("Failed to parse out the NVMe config from the shape %s", definition.type)
            elif "Dense" in shape_type:
                # NOTE: Dense shapes with local NVMe disks
                shape_config = {"ocpus": 8.0}
            else:
                # NOTE: standard shapes with block storage
                shape_config = {"ocpus": 2.0}

        return shape_type, (shape_config or None)

    def _get_tags(self, definition: InstanceDefinition) -> dict:
        tags = definition.tags.copy()
        tags.update(
            {
                "Name": definition.name,
                "CreatedBy": "SCT",
                "ssh_user": definition.user_name,
                "ssh_key": definition.ssh_key.name,
            }
        )
        # NOTE: OCI strictly forbids null values in defined tags
        tags = {k: (str(v) if v is not None else "") for k, v in tags.items()}
        return tags

    def _provision_instance(
        self, oci_region: OciRegion, definition: InstanceDefinition, pricing_model: PricingModel
    ) -> Instance:
        # NOTE: verify instance existence for idempotency
        if definition.name in self._raw_cache:
            LOGGER.info(
                "Instance '%s' already exists (state: %s). Move to the next one.",
                definition.name,
                self._raw_cache[definition.name].lifecycle_state,
            )
            return self._raw_cache[definition.name]

        existing_instances = list(
            oci.pagination.list_call_get_all_results(
                self._compute_client.list_instances,
                compartment_id=self._compartment_id,
                display_name=definition.name,
            ).data
        )
        for current_instance in existing_instances:
            if current_instance.lifecycle_state == Instance.LIFECYCLE_STATE_TERMINATED:
                continue
            LOGGER.info(
                "Instance '%s' already exists (state: %s). Skipping creation.",
                definition.name,
                current_instance.lifecycle_state,
            )
            # NOTE: wait for the instance to be running
            if current_instance.lifecycle_state != Instance.LIFECYCLE_STATE_RUNNING:
                LOGGER.info("Waiting for existing instance '%s' to reach RUNNING state...", definition.name)
                current_instance = self._wait_for_state(  # noqa: PLW2901
                    current_instance.id, Instance.LIFECYCLE_STATE_RUNNING
                )
            return current_instance

        if definition.name in self._raw_cache:
            return self._raw_cache[definition.name]

        # Prepare parameters for instance launch API
        subnet = oci_region.subnet(public=definition.use_public_ip)
        if not subnet:
            prefix = "Public" if definition.use_public_ip else "Private"
            raise Exception(f"{prefix} subnet not found in region '{self._region}'. Check infrastructure configuration")

        LOGGER.info("Creating '%s' VM in compartment %s (%s)...", definition.name, self._compartment_id, self._region)
        metadata = {"ssh_authorized_keys": definition.ssh_key.public_key.decode("utf-8")}
        if definition.user_data:
            builder = UserDataBuilder(user_data_objects=definition.user_data)
            final_user_data = builder.build_mime_multipart_user_data()
            metadata["user_data"] = base64.b64encode(final_user_data.encode("utf-8")).decode("utf-8")
        shape_type, shape_config = self._get_shape_config(definition)
        launch_details = {
            "compartment_id": self._compartment_id,
            "availability_domain": self._get_availability_domain(definition),
            "display_name": definition.name,
            "image_id": definition.image_id,
            "shape": shape_type,
            "shape_config": shape_config,
            "create_vnic_details": CreateVnicDetails(
                subnet_id=subnet.id,
                assign_public_ip=definition.use_public_ip,
                display_name=definition.name,
                nsg_ids=[oci_region.get_or_create_nsg(definition.tags.get("NodeType", definition.name)).id],
            ),
            "metadata": metadata,
            "defined_tags": {
                TAG_NAMESPACE: self._get_tags(definition) | {"launch_time": get_current_datetime_formatted()},
            },
        }
        if created_volume_ids := self._create_volumes(definition):
            launch_details["launch_volume_attachments"] = [
                LaunchAttachParavirtualizedVolumeDetails(
                    type="paravirtualized",
                    volume_id=vol_id,
                    is_shareable=False,
                )
                for vol_id in created_volume_ids
            ]
        if pricing_model == PricingModel.SPOT:
            launch_details["preemptible_instance_config"] = PreemptibleInstanceConfigDetails(
                preemption_action=PreemptionAction(type="TERMINATE"),
            )

        # Launch an instance with the prepared parameters
        instance = None
        LOGGER.info("DEBUG: going to create an instance with the following launch params: %s", launch_details)
        try:
            instance = self._compute_client.launch_instance(LaunchInstanceDetails(**launch_details)).data
            LOGGER.info("Launched instance %s (OCID: %s). Waiting for RUNNING state...", definition.name, instance.id)
            instance = self._wait_for_state(instance.id, Instance.LIFECYCLE_STATE_RUNNING)

            return instance
        except oci.exceptions.ServiceError as e:
            LOGGER.error("Failed to launch instance %s or attach volumes: %s", definition.name, e)
            if instance:
                try:
                    self.terminate(instance.id)
                except Exception as term_err:  # noqa: BLE001
                    LOGGER.warning(
                        "Failed to terminate instance %s after provision error: %s", definition.name, term_err
                    )
            if created_volume_ids:
                LOGGER.info(
                    "Cleaning up %d volumes for failed instance %s...", len(created_volume_ids), definition.name
                )
                for vid in created_volume_ids:
                    try:
                        self._bs_client.delete_volume(vid)
                    except Exception as ve:  # noqa: BLE001
                        LOGGER.warning("Failed to delete volume %s during cleanup: %s", vid, ve)
            raise ProvisionError(f"Failed to launch OCI instance: {e}")

    def create_instances(
        self, oci_region: OciRegion, definitions: List[InstanceDefinition], pricing_model: PricingModel
    ) -> List[Instance]:
        parallel_object = ParallelObject(
            objects=[[oci_region, definition, pricing_model] for definition in definitions],
            timeout=1800,
            num_workers=min(len(definitions), 32),
        )
        results = parallel_object.run(self._provision_instance, unpack_objects=True)
        created_instances = []
        for result in results:
            if result.exc:
                if isinstance(result.exc, ProvisionError):
                    raise result.exc
                raise ProvisionError(f"Failed to provision instance: {result.exc}") from result.exc
            instance = result.result
            if instance:
                self._raw_cache[instance.display_name] = instance
                created_instances.append(instance)
        return created_instances

    def _wait_for_state(self, instance_id: str, state: str, timeout: int = 600) -> Instance:
        waiter = oci.wait_until(
            self._compute_client,
            self._compute_client.get_instance(instance_id),
            "lifecycle_state",
            state,
            max_wait_seconds=timeout,
        )
        return waiter.data

    def terminate(self, name_or_id: str, wait: bool = True):
        instance = self._resolve_instance(name_or_id)
        if not instance:
            LOGGER.warning("Instance %s not found for termination.", name_or_id)
            return

        LOGGER.info("Terminating instance %s (%s)...", instance.display_name, instance.id)
        self._compute_client.terminate_instance(instance.id)

        if wait:
            self._wait_for_state(instance.id, Instance.LIFECYCLE_STATE_TERMINATED)

        if instance.display_name in self._raw_cache:
            del self._raw_cache[instance.display_name]

    def _resolve_instance(self, name_or_id: str) -> Optional[Instance]:
        if name_or_id.startswith("ocid1.instance"):
            try:
                return self._compute_client.get_instance(name_or_id).data
            except oci.exceptions.ServiceError:
                return None
        return self._raw_cache.get(name_or_id)

    def get_ip_address(self, name_or_id: str, public: bool = True) -> str:
        instance = self._resolve_instance(name_or_id)
        if not instance:
            raise ProvisionError(f"Instance {name_or_id} not found to get IP.")

        # List VNIC attachments
        vnic_attachments = oci.pagination.list_call_get_all_results(
            self._compute_client.list_vnic_attachments, compartment_id=self._compartment_id, instance_id=instance.id
        ).data

        if not vnic_attachments:
            raise ProvisionError(f"No VNICs attached to {name_or_id}")

        # Get primary VNIC (usually the first one or check is_primary)
        vnic_id = vnic_attachments[0].vnic_id
        vnic = self._network_client.get_vnic(vnic_id).data

        if public:
            if vnic.public_ip:
                return vnic.public_ip
            return ""
        else:
            return vnic.private_ip

    def convert_to_vm_instance(self, oci_instance: Instance, provisioner_ref) -> VmInstance:
        """Converts OCI SDK Instance model to SCT VmInstance."""

        # NOTE: transform the 'response' object into the instance one.
        if hasattr(oci_instance, "has_next_page") and hasattr(oci_instance, "data"):
            oci_instance = oci_instance.data
        try:
            public_ip = self.get_ip_address(oci_instance.id, public=True)
            private_ip = self.get_ip_address(oci_instance.id, public=False)
        except Exception as e:  # noqa: BLE001
            LOGGER.warning("Failed to detect the IP address(es): %s", e)
            public_ip = ""
            private_ip = ""

        pricing_model = PricingModel.ON_DEMAND
        if oci_instance.preemptible_instance_config:
            pricing_model = PricingModel.SPOT

        # Extract SCT metadata from tags if available
        tags = oci_instance.defined_tags or {}
        sct_tags = tags.get(TAG_NAMESPACE, {})
        user_name = sct_tags.get("ssh_user", "scyllaadm")
        ssh_key_name = sct_tags.get("ssh_key", "unknown")

        return VmInstance(
            name=oci_instance.display_name,
            region=OCI_REGION_NAMES_MAPPING.get(oci_instance.region.lower(), self._region),
            user_name=user_name,
            ssh_key_name=ssh_key_name,
            public_ip_address=public_ip,
            private_ip_address=private_ip,
            tags=sct_tags,
            pricing_model=pricing_model,
            image=oci_instance.image_id,
            creation_time=oci_instance.time_created,
            instance_type=oci_instance.shape,
            _provisioner=provisioner_ref,
        )

    def add_tags(self, name_or_id: str, tags: Dict[str, str]):
        inst = self._resolve_instance(name_or_id)
        if not inst:
            LOGGER.warning("Instance %s not found for tagging.", name_or_id)
            return
        current_tags = (inst.defined_tags or {}).get(TAG_NAMESPACE, {})
        current_tags.update(tags)
        details = oci.core.models.UpdateInstanceDetails(
            defined_tags={TAG_NAMESPACE: current_tags},
        )
        self._compute_client.update_instance(inst.id, details)

    def start(self, name_or_id: str, wait: bool = True):
        inst = self._resolve_instance(name_or_id)
        if inst:
            self._compute_client.instance_action(inst.id, "START")
            if wait:
                self._wait_for_state(inst.id, Instance.LIFECYCLE_STATE_RUNNING)

    def stop(self, name_or_id: str, wait: bool = True):
        inst = self._resolve_instance(name_or_id)
        if inst:
            self._compute_client.instance_action(inst.id, "STOP")
            if wait:
                self._wait_for_state(inst.id, Instance.LIFECYCLE_STATE_STOPPED)

    def reboot(self, name_or_id: str, wait: bool = True, hard: bool = False):
        """
        Reboots the instance using OCI native actions (SOFTRESET or RESET).
        This ensures that data on local NVMe storage (DenseIO shapes) is preserved,
        unlike STOP/START sequence which deallocates the instance and loses local data.
        """
        inst = self._resolve_instance(name_or_id)
        if inst:
            action = "RESET" if hard else "SOFTRESET"
            self._compute_client.instance_action(inst.id, action)
            if wait:
                self._wait_for_state(inst.id, Instance.LIFECYCLE_STATE_RUNNING)
