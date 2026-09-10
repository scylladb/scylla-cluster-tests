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
from sdcm.kernel_panic_checker import AzureKernelPanicChecker
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.sct_provision import region_definition_builder
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.provision.network_configuration import (
    NetworkInterface,
    azure_network_interfaces,
    network_interfaces_count,
)
from sdcm.utils.azure_utils import (
    SECONDARY_NICS_SCRIPT,
    SECONDARY_NICS_SCRIPT_PATH,
    SECONDARY_NICS_SERVICE,
    SECONDARY_NICS_SERVICE_UNIT_TMPL,
)
from sdcm.utils.decorators import retrying
from sdcm.utils.net import resolve_ip_to_dns

LOGGER = logging.getLogger(__name__)
SPOT_TERMINATION_CHECK_DELAY = 15


class CreateAzureNodeError(Exception):
    pass


class Ipv6AddressNotFoundError(Exception):
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
        self._cached_network_interfaces: List[NetworkInterface] | None = None
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
        super().init()
        # disable auditd service
        self.remoter.sudo("systemctl stop auditd", ignore_status=True)
        self.remoter.sudo("systemctl disable auditd", ignore_status=True)
        self.remoter.sudo("systemctl mask auditd", ignore_status=True)
        self.remoter.sudo("systemctl daemon-reload", ignore_status=True)
        if network_interfaces_count(self.parent_cluster.params) > 1:
            self._configure_secondary_nics_os()
        # built after the remoter is up: resolving the device name of each interface needs the
        # MAC -> device map read off the node
        self.scylla_network_configuration = self._build_scylla_network_configuration()
        self.refresh_network_interfaces_info()

    def _configure_secondary_nics_os(self):
        """Configure OS-level addresses and routing for the secondary NICs.

        Azure gives a secondary NIC an address over DHCP but no routing policy, so a reply sourced
        from its address would leave through the primary NIC's default route and be dropped.
        Installs a boot script which queries IMDS and configures every secondary NIC, then runs it
        right away. The systemd service makes the configuration survive reboots.
        """
        self.log.info("Configuring OS-level routing for secondary NICs on %s", self.name)
        nic_count = network_interfaces_count(self.parent_cluster.params)

        self.remoter.sudo(f"bash -c 'cat > {SECONDARY_NICS_SCRIPT_PATH}' << 'SCTEOF'\n{SECONDARY_NICS_SCRIPT}\nSCTEOF")
        self.remoter.sudo(f"chmod 755 {SECONDARY_NICS_SCRIPT_PATH}")

        service_unit = SECONDARY_NICS_SERVICE_UNIT_TMPL.format(
            script_path=SECONDARY_NICS_SCRIPT_PATH, nic_count=nic_count
        )
        service_path = f"/etc/systemd/system/{SECONDARY_NICS_SERVICE}.service"
        self.remoter.sudo(f"bash -c 'cat > {service_path}' << 'SCTEOF'\n{service_unit}\nSCTEOF")
        self.remoter.sudo("systemctl daemon-reload")
        self.remoter.sudo(f"systemctl enable {SECONDARY_NICS_SERVICE}.service")

        # NOTE: run the script now to apply immediately. Failures must not be swallowed: a node with
        #       half-configured NICs stays reachable over its primary interface and only breaks much
        #       later, as a confusing connectivity or streaming error.
        self.remoter.sudo(f"{SECONDARY_NICS_SCRIPT_PATH} {nic_count}")

    def start_network_interface(self, interface_name=None):
        super().start_network_interface(interface_name=interface_name)
        # NOTE: taking a secondary NIC down flushes its addresses and the policy routes/rules of its
        #       dedicated routing table. The 'sct-secondary-nics' service is a 'oneshot' which
        #       normally runs only at boot, so re-run it here to re-apply the configuration once the
        #       interface is back up.
        if self.parent_cluster.extra_network_interface:
            self.remoter.sudo(f"systemctl restart {SECONDARY_NICS_SERVICE}.service")

    def _create_kernel_panic_checker(self):
        return AzureKernelPanicChecker(
            node_name=self.name,
            vm_name=self._instance.name,
            region=self.region,
            resource_group=self._instance._provisioner.resource_group_name,
            host=self.external_address,
            logdir=self.logdir,
        )

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
        """Cached NetworkInterface list, rebuilt from the Azure API only after an invalidation."""
        if self._cached_network_interfaces is None:
            self._cached_network_interfaces = self._build_network_interfaces()
        return self._cached_network_interfaces

    def _invalidate_network_interfaces_cache(self):
        self._cached_network_interfaces = None

    def refresh_network_interfaces_info(self):
        self._invalidate_network_interfaces_cache()
        super().refresh_network_interfaces_info()

    def _build_network_interfaces(self) -> List[NetworkInterface]:
        """Build the NetworkInterface list from the Azure NICs of this VM, primary one first."""
        provisioner = self._instance._provisioner
        devices = self.network_configuration if self.remoter else {}

        interfaces = []
        for device_index, nic in enumerate(provisioner.network_interfaces(self._instance.name)):
            ipv4_private_addresses, ipv6_addresses, public_ipv4, public_ipv6 = [], [], None, None
            for config in nic.ip_configurations:
                public_ip = self._public_ip_of(config, device_index)
                if config.private_ip_address_version == "IPv6":
                    ipv6_addresses.append(config.private_ip_address)
                    public_ipv6 = public_ipv6 or public_ip
                else:
                    ipv4_private_addresses.append(config.private_ip_address)
                    public_ipv4 = public_ipv4 or public_ip

            # Azure reports MACs as '00-0D-3A-...', ip-link as '00:0d:3a:...'
            mac_address = nic.mac_address.replace("-", ":").lower() if nic.mac_address else None
            interfaces.append(
                NetworkInterface(
                    ipv4_public_address=public_ipv4,
                    # only a routable (Public IP) IPv6 belongs here, the VNet-local one is private
                    ipv6_public_addresses=[public_ipv6] if public_ipv6 else [],
                    ipv4_private_addresses=ipv4_private_addresses,
                    ipv6_private_address=ipv6_addresses[0] if ipv6_addresses else "",
                    dns_private_name=self._instance.private_dns_name or "",
                    dns_public_name=None,
                    device_index=device_index,
                    device_name=devices.get(mac_address, "") if mac_address and devices else "",
                    mac_address=mac_address,
                    use_dns_names=self.use_dns_names,
                )
            )
        return interfaces

    def _public_ip_of(self, ip_configuration, device_index: int) -> str | None:
        """Address of the Public IP attached to one ipConfiguration, None when it carries none.

        Azure embeds a Public IP in a NIC as a sub-resource *reference*: the payload carries its
        id but not its `ipAddress` unless the NIC is fetched with
        `expand=IPConfigurations/PublicIPAddress`. The provisioner's IP provider holds the full
        resource - it re-reads every Public IP it creates - so the address comes from there, with
        whatever the NIC happens to carry preferred when it is populated.
        """
        if ip_configuration.public_ip_address is None:
            return None
        if address := getattr(ip_configuration.public_ip_address, "ip_address", None):
            return address
        version = "IPV6" if ip_configuration.private_ip_address_version == "IPv6" else "IPV4"
        provisioner = self._instance._provisioner
        return provisioner._ip_provider.get(self._instance.name, version=version, index=device_index).ip_address

    @retrying(n=6, sleep_time=1)
    def _set_keep_alive(self) -> bool:
        self._instance.add_tags({"keep": "alive"})
        return super()._set_keep_alive()

    @retrying(n=6, sleep_time=1)
    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self._instance.add_tags({"keep": str(duration_in_hours)})

    def _refresh_instance_state(self):
        if self.scylla_network_configuration:
            self.refresh_network_interfaces_info()
            public_ipv4_addresses = [
                interface.ipv4_public_address
                for interface in self.scylla_network_configuration.network_interfaces
                if interface.ipv4_public_address
            ]
            private_ipv4_addresses = [
                interface.ipv4_private_addresses[0]
                for interface in self.scylla_network_configuration.network_interfaces
                if interface.ipv4_private_addresses
            ]
            return public_ipv4_addresses, private_ipv4_addresses
        return ([self._instance.public_ip_address], [self._instance.private_ip_address])

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

    def _restart_inner(self):
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

    def _get_ipv6_ip_address(self) -> str:
        """Routable IPv6 address of the node, empty when the run did not ask for IPv6.

        Prefers what the API reports, and asks the OS only as a fallback - which needs SSH, so
        before the node has a remoter a missing address is an error rather than an empty string.
        """
        if self.scylla_network_configuration:
            if address := self.scylla_network_configuration.interface_ipv6_address:
                return address
        if address := self._api_ipv6_address():
            return address
        if not any(interface["ipv6"] for interface in azure_network_interfaces(self.parent_cluster.params)):
            return ""
        if not self.remoter and not self.destroyed:
            # No SSH yet, and the OS fallback below needs it. This is the node-init path:
            # `ip_ssh_connections` resolves to 'ipv6' whenever 'test_communication' does, so this
            # address is what the SSH connection is about to be opened to. Returning "" here would
            # hand SSH an empty hostname and fail much later, as a connection timeout.
            raise Ipv6AddressNotFoundError(
                f"No routable IPv6 address for {self.name}: the Azure API reports none on its "
                f"primary NIC and the OS cannot be asked before SSH is up"
            )
        return next(iter(self._discover_ipv6_from_os().values()), [""])[0]

    def _api_ipv6_address(self) -> str:
        """Routable IPv6 of the primary NIC per the Azure API, empty while it is not published."""
        interfaces = self.network_interfaces
        if interfaces and interfaces[0].ipv6_public_addresses:
            return interfaces[0].ipv6_public_addresses[0]
        return ""

    def _discover_ipv6_from_os(self) -> dict:
        """Global-scope IPv6 addresses seen by the node OS, keyed by interface name."""
        if not self.remoter or self.destroyed:
            return {}
        result = self.remoter.run("ip -6 -j addr show scope global", ignore_status=True)
        if result.exit_status != 0 or not result.stdout.strip():
            return {}
        try:
            ipv6_map = {}
            for interface in json.loads(result.stdout.strip()):
                addresses = [
                    address["local"]
                    for address in interface.get("addr_info", [])
                    if address.get("family") == "inet6" and address.get("local")
                ]
                if addresses:
                    ipv6_map[interface.get("ifname", "")] = addresses
            return ipv6_map
        except json.JSONDecodeError, KeyError:
            return {}

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
            extra_network_interface=network_interfaces_count(params) > 1,
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
        node_type="scylla-db",
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
            node_type=node_type,
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
