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
# Copyright (c) 2022 ScyllaDB
import json
import logging
from functools import cached_property
from typing import Dict, List

from sdcm import cluster
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.provisioner import PricingModel, VmInstance
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.sct_provision import region_definition_builder
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.utils.decorators import retrying
from sdcm.utils.net import resolve_ip_to_dns

LOGGER = logging.getLogger(__name__)
SPOT_TERMINATION_CHECK_DELAY = 15


class CreateAzureNodeError(Exception):
    pass


class AzureNode(cluster.BaseNode):
    """
    Wraps Azure instances, so that we can also control the instance through SSH.
    """

    METADATA_BASE_URL = "http://169.254.169.254/metadata/instance/"

    log = LOGGER

    def __init__(
        self,
        azure_instance: VmInstance,
        credentials,
        parent_cluster,
        node_prefix="node",
        node_index=1,
        base_logdir=None,
        dc_idx=0,
        rack=0,
        after_config=None,
    ):
        self.node_index = node_index
        self.dc_idx = dc_idx
        self.parent_cluster = parent_cluster
        self._instance = azure_instance
        self._instance_type = azure_instance.instance_type
        name = f"{node_prefix}-{self.region}-{node_index}".lower()
        self.last_event_document_incarnation = -1
        ssh_login_info = {
            "hostname": None,
            "user": azure_instance.user_name,
            "key_file": credentials.key_file,
            "extra_ssh_options": "-tt",
        }
        super().__init__(
            name=name,
            parent_cluster=parent_cluster,
            ssh_login_info=ssh_login_info,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            dc_idx=dc_idx,
            rack=rack,
            after_config=after_config,
        )

    @cluster.terminate_on_failure
    def init(self) -> None:
        try:
            super().init()
        except Exception:
            self.log.error("Node %s init failed, collecting Azure VM diagnostics...", self.name)
            self._collect_diagnostics_on_failure()
            raise
        self.remoter.sudo("systemctl stop auditd", ignore_status=True)
        self.remoter.sudo("systemctl disable auditd", ignore_status=True)
        self.remoter.sudo("systemctl mask auditd", ignore_status=True)
        self.remoter.sudo("systemctl daemon-reload", ignore_status=True)

    def _collect_diagnostics_on_failure(self):
        """Collect and log Azure diagnostics when node initialization fails (e.g. SSH timeout)."""
        try:
            diagnostics = self.collect_azure_vm_diagnostics()
            if diagnostics:
                self.log.error("Azure VM diagnostics for %s: %s", self.name, json.dumps(diagnostics, indent=2))
            console_output = self.get_console_output()
            if console_output:
                self.log.error(
                    "Azure serial console output for %s (last 2000 chars): ...%s", self.name, console_output[-2000:]
                )
            activity_log = self.collect_azure_activity_log()
            if activity_log:
                self.log.error(
                    "Azure Activity Log errors/warnings for %s: %s", self.name, json.dumps(activity_log, indent=2)
                )
            nsg_rules = self.collect_effective_nsg_rules()
            if nsg_rules:
                self.log.error("Azure effective NSG rules for %s: %s", self.name, json.dumps(nsg_rules, indent=2))
            resource_health = self.check_azure_resource_health()
            if resource_health:
                self.log.error("Azure Resource Health for %s: %s", self.name, json.dumps(resource_health, indent=2))
            self.save_boot_diagnostics_screenshot()
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to collect diagnostics on failure for %s: %s", self.name, exc)

    def wait_for_cloud_init(self):
        pass  # azure for it, on resources creation

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    @property
    def network_interfaces(self):
        pass

    def refresh_network_interfaces_info(self):
        pass

    @retrying(n=6, sleep_time=1)
    def _set_keep_alive(self) -> bool:
        self._instance.add_tags({"keep": "alive"})
        return super()._set_keep_alive()

    @retrying(n=6, sleep_time=1)
    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self._instance.add_tags({"keep": str(duration_in_hours)})

    def _refresh_instance_state(self):
        ip_tuple = ([self._instance.public_ip_address], [self._instance.private_ip_address])
        return ip_tuple

    @property
    def vm_region(self):
        return self._instance.region

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.pricing_model.is_spot()

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        Returns number of seconds to wait before next check.
        """
        try:
            self.wait_ssh_up(verbose=False)
            result = self.remoter.run(
                'curl http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01 -H "Metadata: true"',
                verbose=False,
            )
            status = json.loads(result.stdout.strip())
            if status["DocumentIncarnation"] == self.last_event_document_incarnation:
                # each change in status["Events"] increments "DocumentIncarnation", return if there was no change.
                return SPOT_TERMINATION_CHECK_DELAY
            for event in status["Events"]:
                self.last_event_document_incarnation = status["DocumentIncarnation"]
                if event["EventType"] == "Preempt":
                    message = f"Got spot termination event for node: {event['Resources']}. VM eviction time is {event['NotBefore']}."
                    SpotTerminationEvent(node=self, message=message).publish()
                else:
                    # other EventType's that can be triggered by Azure's maintenance: "Reboot" | "Redeploy" | "Freeze" | "Terminate"
                    self.log.warning(f"Unhandled Azure scheduled event: {event}")
        except Exception as details:  # noqa: BLE001
            self.log.warning("Error during getting Azure scheduled events: %s", details)
            return 0
        return SPOT_TERMINATION_CHECK_DELAY

    def restart(self):
        # When using NVMe disks in Azure, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance.reboot(wait=True, hard=False)

    def hard_reboot(self):
        self._instance.reboot(wait=True, hard=True)

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance.terminate(wait=True)
        super().destroy()

    def _get_ipv6_ip_address(self):
        # todo: fix it
        return ""

    @property
    def image(self):
        return self._instance.image

    def _get_public_ip_address(self) -> str | None:
        return self._instance.public_ip_address

    def _get_private_ip_address(self) -> str | None:
        return self._instance.private_ip_address

    def configure_remote_logging(self) -> None:
        """Remote logging configured upon vm provisioning using UserDataObject"""
        return

    def get_console_output(self):
        """Retrieve boot diagnostics serial console log from Azure VM.

        This is critical for diagnosing provisioning failures where SSH never connects,
        since without this the node's log directory remains completely empty (SCT-434).
        """
        try:
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            vm_name = self._instance.name
            boot_diagnostics = azure_service.compute.virtual_machines.retrieve_boot_diagnostics_data(
                resource_group_name=resource_group, vm_name=vm_name
            )
            serial_log_uri = getattr(boot_diagnostics, "serial_console_log_blob_uri", None)
            if not serial_log_uri:
                self.log.warning("No serial console log URI available for VM %s", vm_name)
                return ""
            import requests  # noqa: PLC0415

            response = requests.get(serial_log_uri, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to retrieve console output for %s: %s", self.name, exc)
            return ""

    def collect_azure_vm_diagnostics(self) -> dict:
        """Collect Azure VM instance view and NIC effective routes for post-mortem analysis.

        Returns a dict with instance_view statuses, power_state, provisioning_state,
        and NIC effective routes. Useful when SSH connection fails during provisioning.
        """
        diagnostics = {}
        try:
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            vm_name = self._instance.name

            # Collect VM instance view (power state, provisioning state, statuses)
            instance_view = azure_service.compute.virtual_machines.instance_view(resource_group, vm_name)
            if instance_view:
                diagnostics["statuses"] = [
                    {"code": s.code, "display_status": s.display_status, "time": str(s.time)}
                    for s in (instance_view.statuses or [])
                ]
                diagnostics["vm_agent"] = {
                    "statuses": [
                        {"code": s.code, "display_status": s.display_status}
                        for s in (instance_view.vm_agent.statuses if instance_view.vm_agent else [])
                    ]
                }
                diagnostics["boot_diagnostics"] = (
                    {"status": getattr(instance_view.boot_diagnostics, "status", None)}
                    if instance_view.boot_diagnostics
                    else None
                )

            # Collect NIC effective routes to diagnose networking issues
            nic_name = f"{vm_name}-nic"
            try:
                poller = azure_service.network.network_interfaces.begin_get_effective_route_table(
                    resource_group_name=resource_group, network_interface_name=nic_name
                )
                # Wait max 60s for effective routes
                effective_routes = poller.result(timeout=60)
                if effective_routes and effective_routes.value:
                    diagnostics["effective_routes"] = [
                        {
                            "address_prefix": r.address_prefix,
                            "next_hop_type": r.next_hop_type,
                            "next_hop_ip": r.next_hop_ip_address,
                            "state": r.state,
                        }
                        for r in effective_routes.value[:20]  # limit to 20 routes
                    ]
            except Exception as route_exc:  # noqa: BLE001
                diagnostics["effective_routes_error"] = str(route_exc)

        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to collect Azure VM diagnostics for %s: %s", self.name, exc)
            diagnostics["error"] = str(exc)
        return diagnostics

    def collect_effective_nsg_rules(self) -> list:
        """Collect effective NSG (Network Security Group) rules for the VM's NIC.

        Returns the effective security rules applied to the network interface,
        which helps diagnose SSH connectivity issues caused by blocked ports.
        """
        rules = []
        try:
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            vm_name = self._instance.name
            nic_name = f"{vm_name}-nic"

            poller = azure_service.network.network_interfaces.begin_list_effective_network_security_groups(
                resource_group_name=resource_group, network_interface_name=nic_name
            )
            result = poller.result(timeout=60)
            if result and result.value:
                for nsg in result.value:
                    for rule in nsg.effective_security_rules or []:
                        rules.append(
                            {
                                "name": rule.name,
                                "direction": rule.direction,
                                "access": rule.access,
                                "protocol": rule.protocol,
                                "source_port_range": rule.source_port_range,
                                "destination_port_range": rule.destination_port_range,
                                "source_address_prefix": rule.source_address_prefix,
                                "destination_address_prefix": rule.destination_address_prefix,
                                "priority": rule.priority,
                            }
                        )
            self.log.info("Collected %d effective NSG rules for %s", len(rules), nic_name)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to collect effective NSG rules for %s: %s", self.name, exc)
        return rules

    def collect_azure_activity_log(self, minutes_back: int = 30) -> list:
        """Collect Azure Activity Log entries for the resource group.

        Queries the Azure Monitor Activity Log for recent operations that may explain
        provisioning failures (e.g. quota exceeded, deployment failures, throttling).
        """
        entries = []
        try:
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance
            from datetime import datetime, timedelta, timezone  # noqa: PLC0415

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            subscription_id = azure_service.subscription_id

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=minutes_back)
            filter_str = (
                f"eventTimestamp ge '{start_time.isoformat()}' and "
                f"eventTimestamp le '{end_time.isoformat()}' and "
                f"resourceGroupName eq '{resource_group}'"
            )

            from azure.mgmt.monitor import MonitorManagementClient  # noqa: PLC0415

            monitor_client = MonitorManagementClient(
                credential=azure_service.credential, subscription_id=subscription_id
            )
            activity_logs = monitor_client.activity_logs.list(filter=filter_str)
            for log_entry in activity_logs:
                if log_entry.level and log_entry.level.value in ("Error", "Warning", "Critical"):
                    entries.append(
                        {
                            "timestamp": str(log_entry.event_timestamp),
                            "operation": log_entry.operation_name.value if log_entry.operation_name else None,
                            "status": log_entry.status.value if log_entry.status else None,
                            "level": log_entry.level.value,
                            "description": getattr(log_entry, "description", None),
                            "resource_id": log_entry.resource_id,
                        }
                    )
            self.log.info("Collected %d activity log entries (errors/warnings) for %s", len(entries), resource_group)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to collect activity log for %s: %s", self.name, exc)
        return entries

    def check_azure_resource_health(self) -> dict | None:
        """Query Azure Resource Health API for the VM to detect platform-level issues.

        Returns availability status (Available, Unavailable, Degraded, Unknown) and
        any reported platform events (host crashes, unplanned maintenance, etc.).
        """
        try:
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            vm_name = self._instance.name
            subscription_id = azure_service.subscription_id

            resource_uri = (
                f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
                f"/providers/Microsoft.Compute/virtualMachines/{vm_name}"
            )

            from azure.mgmt.resourcehealth import ResourceHealthMgmtClient  # noqa: PLC0415

            health_client = ResourceHealthMgmtClient(
                credential=azure_service.credential, subscription_id=subscription_id
            )
            status = health_client.availability_statuses.get_by_resource(resource_uri=resource_uri)
            if status:
                result = {
                    "availability_state": status.properties.availability_state if status.properties else None,
                    "summary": status.properties.summary if status.properties else None,
                    "reason_type": status.properties.reason_type if status.properties else None,
                    "occurred_time": str(status.properties.occurred_time) if status.properties else None,
                }
                self.log.info("Resource Health for %s: %s", vm_name, result)
                return result
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to query Resource Health for %s: %s", self.name, exc)
        return None

    def save_boot_diagnostics_screenshot(self) -> str | None:
        """Save VM boot diagnostics screenshot to the node's log directory.

        Azure provides a screenshot of the VM console as a BMP via boot diagnostics.
        Useful for diagnosing kernel panics or boot hangs that prevent SSH connectivity.
        """
        try:
            import os  # noqa: PLC0415
            from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415 - cyclic import avoidance

            azure_service = AzureService()
            resource_group = self._instance._provisioner.resource_group_name
            vm_name = self._instance.name
            boot_diagnostics = azure_service.compute.virtual_machines.retrieve_boot_diagnostics_data(
                resource_group_name=resource_group, vm_name=vm_name
            )
            screenshot_uri = getattr(boot_diagnostics, "console_screenshot_blob_uri", None)
            if not screenshot_uri:
                self.log.warning("No screenshot URI available for VM %s", vm_name)
                return None
            import requests  # noqa: PLC0415

            response = requests.get(screenshot_uri, timeout=30)
            response.raise_for_status()
            screenshot_path = os.path.join(self.logdir, f"{vm_name}-boot-screenshot.bmp")
            with open(screenshot_path, "wb") as fh:
                fh.write(response.content)
            self.log.info("Saved boot diagnostics screenshot to %s", screenshot_path)
            return screenshot_path
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to save boot screenshot for %s: %s", self.name, exc)
            return None

    def query_azure_metadata(self, path: str, api_version: str = "2024-07-17") -> str:
        url = f"{self.METADATA_BASE_URL}{path}?api-version={api_version}"
        return self.query_metadata(url=url, headers={"Metadata": "true"})

    @cached_property
    def private_dns_name(self) -> str:
        return resolve_ip_to_dns(self.private_ip_address)


class AzureCluster(cluster.BaseCluster):
    def __init__(  # noqa: PLR0913
        self,
        image_id,
        root_disk_size,
        provisioners: List[AzureProvisioner],
        credentials,
        cluster_uuid=None,
        instance_type="Standard_L8s_v3",
        region_names=None,
        user_name="root",
        cluster_prefix="cluster",
        node_prefix="node",
        n_nodes=3,
        params=None,
        node_type=None,
    ):
        self.provisioners: List[AzureProvisioner] = provisioners
        self._image_id = image_id
        self._root_disk_size = root_disk_size
        self._credentials = credentials
        self._instance_type = instance_type
        self._user_name = user_name
        self._azure_region_names = region_names
        self._node_prefix = node_prefix
        self._definition_builder = region_definition_builder.get_builder(params, test_config=self.test_config)
        super().__init__(
            cluster_uuid=cluster_uuid,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=region_names,
            node_type=node_type,
        )
        self.log.debug("AzureCluster constructor")

    @mark_new_nodes_as_running_nemesis
    def add_nodes(
        self,
        count,
        ec2_user_data="",
        dc_idx=0,
        rack=0,
        enable_auto_bootstrap=False,
        instance_type=None,
        after_config=None,
    ):
        self.log.info("Adding nodes to cluster")
        nodes = []

        instance_dc = 0 if self.params.get("simulated_regions") else dc_idx
        instances = self._create_instances(count, instance_dc, instance_type=instance_type)

        self.log.debug("instances: %s", instances)
        for node_index, instance in enumerate(instances, start=self._node_index + 1):
            # in case rack is not specified, spread nodes to different racks
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(instance, node_index, dc_idx, rack=node_rack, after_config=after_config)
            nodes.append(node)
            self.nodes.append(node)
            self.log.info("Added node: %s", node.name)
            node.enable_auto_bootstrap = enable_auto_bootstrap

        self._node_index += count
        self.log.info("added nodes: %s", nodes)
        return nodes

    def _create_node(self, instance, node_index, dc_idx, rack, after_config=None):
        try:
            node = AzureNode(
                azure_instance=instance,
                credentials=self._credentials[0],
                parent_cluster=self,
                node_prefix=self.node_prefix,
                node_index=node_index,
                base_logdir=self.logdir,
                dc_idx=dc_idx,
                rack=rack,
                after_config=after_config,
            )
            node.init()
            return node
        except Exception as ex:  # noqa: BLE001
            raise CreateAzureNodeError("Failed to create node: %s" % ex) from ex

    def _create_instances(self, count, dc_idx=0, instance_type=None) -> List[VmInstance]:
        region = self._definition_builder.regions[dc_idx]
        assert region, "no region provided, please add `azure_region_name` param"
        pricing_model = PricingModel.SPOT if "spot" in self.instance_provision else PricingModel.ON_DEMAND
        definitions = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            definitions.append(
                self._definition_builder.build_instance_definition(
                    region=region, node_type=self.node_type, index=node_index, instance_type=instance_type
                )
            )
        return provision_instances_with_fallback(
            self.provisioners[dc_idx],
            definitions=definitions,
            pricing_model=pricing_model,
            fallback_on_demand=self.params.get("instance_provision_fallback_on_demand"),
        )

    def get_node_ips_param(self, public_ip=True):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("get_node_ips_param should not run")

    def node_setup(self, node, verbose=False, timeout=3600):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("node_setup should not run")

    def node_startup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("'node_startup' should not run")

    def wait_for_init(self):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("wait_for_init should not run")


class ScyllaAzureCluster(cluster.BaseScyllaCluster, AzureCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners: List[AzureProvisioner],
        credentials,
        instance_type="Standard_L8s_v3",
        user_name="ubuntu",
        user_prefix=None,
        n_nodes=3,
        params=None,
        region_names=None,
    ):
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "db-cluster")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "db-node")
        super().__init__(
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=region_names,
            node_type="scylla-db",
        )
        self.version = "2.1"

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()

    def _reuse_cluster_setup(self, node: AzureNode) -> None:
        super()._reuse_cluster_setup(node)
        node.run_startup_script()


class LoaderSetAzure(cluster.BaseLoaderSet, AzureCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="Standard_D2_v4",
        user_name="centos",
        user_prefix=None,
        n_nodes=1,
        params=None,
        region_names=None,
    ):
        node_prefix = cluster.prepend_user_prefix(user_prefix, "loader-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "loader-set")
        cluster.BaseLoaderSet.__init__(self, params=params)
        AzureCluster.__init__(
            self,
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            node_type="loader",
            region_names=region_names,
        )


class MonitorSetAzure(cluster.BaseMonitorSet, AzureCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="Standard_D2_v4",
        user_name="centos",
        user_prefix=None,
        n_nodes=1,
        targets=None,
        params=None,
        region_names=None,
    ):
        node_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-set")

        targets = targets if targets else {}
        cluster.BaseMonitorSet.__init__(self, targets=targets, params=params)
        AzureCluster.__init__(
            self,
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            node_type="monitor",
            region_names=region_names,
        )
