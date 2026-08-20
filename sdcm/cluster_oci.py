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

import json
import logging
import time
from datetime import datetime
from functools import cached_property
from typing import Dict, List

import yaml

from sdcm import cluster
from sdcm.kernel_panic_checker import OCIKernelPanicChecker
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.provision.network_configuration import (
    NetworkInterface,
    ScyllaNetworkConfiguration,
    network_interfaces_count,
)
from sdcm.provision.oci.provisioner import OciProvisioner
from sdcm.provision.provisioner import PricingModel, VmInstance
from sdcm.provision.helpers.certificate import CA_CERT_FILE, CA_KEY_FILE, create_certificate
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.sct_provision import region_definition_builder
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.utils.decorators import retrying
from sdcm.utils.net import resolve_ip_to_dns
from sdcm.utils.oci_utils import get_oci_compartment_id

LOGGER = logging.getLogger(__name__)
SPOT_TERMINATION_CHECK_DELAY = 15
SPOT_TERMINATION_CHECK_OVERHEAD = 5


class CreateOciNodeError(Exception):
    pass


class OciNode(cluster.BaseNode):
    """
    Wraps OCI instances, so that we can also control the instance through SSH.
    """

    METADATA_BASE_URL = "http://169.254.169.254/opc/v2/instance/"

    log = LOGGER

    def __init__(
        self,
        oci_instance: VmInstance,
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
        self._instance = oci_instance
        self._instance_type = oci_instance.instance_type
        self._ipv6_map: dict | None = None
        name = f"{node_prefix}-{self.region}-{node_index}".lower()
        ssh_login_info = {
            "hostname": None,
            "user": oci_instance.user_name,
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

    def wait_for_cloud_init(self):
        # NOTE: cloud-init gets checked by sdcm/provision/helpers/cloud_init
        pass

    def init(self) -> None:
        nic_count = network_interfaces_count(self.parent_cluster.params)
        if nic_count > 1:
            provisioner = self.parent_cluster.provisioners[self.dc_idx]
            provisioner.attach_secondary_vnics(
                name=self._instance.name,
                nic_count=nic_count,
                node_type=self.parent_cluster.node_type or "scylla-db",
            )
        scylla_network_config = self.parent_cluster.params.get("scylla_network_config")
        if scylla_network_config:
            self._try_set_network_configuration(scylla_network_config)
        super().init()
        # After SSH is up, refresh/rebuild network configuration with OS-level data
        if scylla_network_config:
            if not self.scylla_network_configuration:
                # IPv6 wasn't available from API pre-init; discover from OS now that SSH is up
                self._discover_and_set_network_configuration(scylla_network_config, nic_count)
            else:
                self.refresh_network_interfaces_info()
                if nic_count > 1:
                    self._configure_secondary_vnics_os()

    def _try_set_network_configuration(self, scylla_network_config):
        """Try to set scylla_network_configuration from OCI API data (before SSH is available).

        If IPv6 addresses aren't available yet from the API, this will log a warning
        and leave scylla_network_configuration as None for post-SSH discovery.
        """
        try:
            network_config = ScyllaNetworkConfiguration(
                network_interfaces=self.network_interfaces,
                scylla_network_config=scylla_network_config,
            )
            # Validate that test_communication resolves (catches empty ipv6_public_addresses)
            network_config.test_communication
            self.scylla_network_configuration = network_config
            self.log.debug("Node %s scylla_network_config: %s", self.name, scylla_network_config)
            self.log.debug(
                "Node %s network_interfaces: %s", self.name, self.scylla_network_configuration.network_interfaces
            )
        except (IndexError, KeyError) as exc:
            self.log.warning(
                "Cannot resolve network configuration from API data on %s (likely missing IPv6): %s. "
                "Will discover from OS after SSH is up.",
                self.name,
                exc,
            )
            self.scylla_network_configuration = None

    def _discover_and_set_network_configuration(self, scylla_network_config, nic_count):
        """Discover IPv6 addresses from the node OS and set scylla_network_configuration.

        Called after SSH is available when pre-init API-based setup failed (e.g., IPv6 not in API).
        """
        self.log.info("Discovering network configuration from OS on %s", self.name)
        self._ipv6_map = self._discover_ipv6_from_os()
        if self._ipv6_map:
            self.log.info("Discovered IPv6 addresses on %s: %s", self.name, self._ipv6_map)
        # Rebuild network_interfaces with discovered IPv6
        interfaces = self._build_network_interfaces()
        try:
            network_config = ScyllaNetworkConfiguration(
                network_interfaces=interfaces,
                scylla_network_config=scylla_network_config,
            )
            # Validate all required addresses resolve
            network_config.test_communication
            self.scylla_network_configuration = network_config
            self.log.debug(
                "Node %s network_interfaces: %s", self.name, self.scylla_network_configuration.network_interfaces
            )
            # Update SSH connection to use the new address
            self.refresh_ip_address()
        except (IndexError, KeyError) as exc:
            self.log.error(
                "Failed to resolve network configuration from OS-discovered data on %s: %s. "
                "Node will continue using fallback IP for SSH.",
                self.name,
                exc,
            )
            return
        if nic_count > 1:
            self._configure_secondary_vnics_os()

    def _discover_ipv6_from_os(self) -> dict:
        """Discover global-scope IPv6 addresses from the node OS via SSH.

        Returns a dict mapping interface name to list of IPv6 addresses.
        """
        result = self.remoter.run(
            "ip -6 -j addr show scope global",
            ignore_status=True,
        )
        if result.exit_status != 0 or not result.stdout.strip():
            return {}
        try:
            ifaces = json.loads(result.stdout.strip())
            ipv6_map = {}
            for iface in ifaces:
                ifname = iface.get("ifname", "")
                addr_info = iface.get("addr_info", [])
                addrs = [a["local"] for a in addr_info if a.get("family") == "inet6" and a.get("local")]
                if addrs:
                    ipv6_map[ifname] = addrs
            return ipv6_map
        except (json.JSONDecodeError, KeyError):
            return {}

    def _build_network_interfaces(self) -> list:
        """Build NetworkInterface list from OCI VNIC attachments.

        Uses self._ipv6_map (populated by _discover_and_set_network_configuration) as
        fallback when the OCI API doesn't return IPv6 addresses for a VNIC.
        """
        provisioner = self.parent_cluster.provisioners[self.dc_idx]
        instance = provisioner._vm_provider._resolve_instance(self._instance.name)  # noqa: SLF001
        if not instance:
            return []

        devices = self._get_network_devices() if self.remoter else {}
        attachments = provisioner._vm_provider.get_vnic_attachments(instance.id)  # noqa: SLF001

        # Collect (vnic, attachment) pairs and sort so the primary VNIC comes first
        vnic_pairs = []
        for attachment in attachments:
            vnic = provisioner.get_vnic_details(attachment.vnic_id)
            vnic_pairs.append((vnic, attachment))
        vnic_pairs.sort(key=lambda pair: (not getattr(pair[0], "is_primary", False), pair[1].nic_index or 0))

        interfaces = []
        for device_index, (vnic, attachment) in enumerate(vnic_pairs):
            private_ip = vnic.private_ip if vnic.private_ip else ""
            public_ip = vnic.public_ip if vnic.public_ip else None
            dns_name = provisioner.get_vnic_private_dns_name(attachment.vnic_id)
            mac_address = vnic.mac_address if hasattr(vnic, "mac_address") else None
            device_name = devices.get(mac_address, "") if mac_address and devices else ""

            ipv6_addresses = provisioner.get_vnic_ipv6_addresses(attachment.vnic_id)
            if not ipv6_addresses and device_name and self._ipv6_map:
                ipv6_addresses = self._ipv6_map.get(device_name, [])

            interfaces.append(
                NetworkInterface(
                    ipv4_public_address=public_ip,
                    ipv6_public_addresses=ipv6_addresses,
                    ipv4_private_addresses=[private_ip] if private_ip else [],
                    ipv6_private_address=ipv6_addresses[0] if ipv6_addresses else "",
                    dns_private_name=dns_name,
                    dns_public_name=None,
                    device_index=device_index,
                    device_name=device_name,
                    mac_address=mac_address,
                    use_dns_names=self.use_dns_names,
                )
            )

        return interfaces

    def _create_kernel_panic_checker(self):
        instance_id = self._get_oci_instance_id()
        if not instance_id:
            return None
        return OCIKernelPanicChecker(
            node_name=self.name,
            instance_id=instance_id,
            compartment_id=get_oci_compartment_id(),
            region=self.region,
            host=self.external_address,
            logdir=self.logdir,
        )

    def _get_oci_instance_id(self) -> str:
        """Resolve the OCI instance OCID from the provisioner cache."""
        try:
            provisioner = self.parent_cluster.provisioners[self.dc_idx]
            vm_provider = provisioner._vm_provider  # noqa: SLF001
            instance = vm_provider._resolve_instance(self._instance.name)  # noqa: SLF001
            return instance.id if instance else ""
        except (AttributeError, IndexError):
            LOGGER.warning("Could not resolve OCI instance ID for %s", self.name)
            return ""

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    @property
    def network_interfaces(self):
        """Build NetworkInterface list from OCI VNIC attachments."""
        return self._build_network_interfaces()

    def _get_network_devices(self) -> dict:
        """Return MAC->device name mapping from the node OS."""
        if not self.remoter:
            return {}
        if network_config_json := self.remoter.run("ip -j link", ignore_status=True).stdout.strip():
            try:
                ifaces = json.loads(network_config_json)
                return {
                    iface["address"]: iface["ifname"]
                    for iface in ifaces
                    if iface.get("ifname") != "lo" and iface.get("address")
                }
            except (json.JSONDecodeError, KeyError):
                pass
        # Fallback
        ip_link_cmd = """ip -o link | awk '$2 != "lo:" {gsub(/:/,"",$2);print $17": " $2}'"""
        network_config = self.remoter.run(ip_link_cmd).stdout.strip()
        return yaml.safe_load(network_config) or {}

    def refresh_network_interfaces_info(self):
        """Refresh the network interfaces info on the ScyllaNetworkConfiguration."""
        if self.scylla_network_configuration:
            self.scylla_network_configuration.network_interfaces = self.network_interfaces

    _SECONDARY_VNICS_SCRIPT = "/usr/local/sbin/sct-configure-secondary-vnics.sh"
    _SECONDARY_VNICS_SERVICE = "sct-secondary-vnics"

    def _configure_secondary_vnics_os(self):
        """Configure OS-level routing for secondary VNICs.

        OCI secondary VNICs need policy-based routing to avoid asymmetric routing.
        Installs a boot script that queries IMDS and configures each secondary VNIC,
        then runs it immediately. The systemd service ensures the configuration
        survives reboots.
        """
        self.log.info("Configuring OS-level routing for secondary VNICs on %s", self.name)
        primary_ip = self._instance.private_ip_address

        # Install a self-contained boot script that discovers and configures secondary VNICs
        script = self._build_secondary_vnics_script(primary_ip)
        self.remoter.sudo(f"bash -c 'cat > {self._SECONDARY_VNICS_SCRIPT}' << 'SCTEOF'\n{script}\nSCTEOF")
        self.remoter.sudo(f"chmod 755 {self._SECONDARY_VNICS_SCRIPT}")

        # Install and enable the systemd service so the script runs on every boot
        service_unit = (
            "[Unit]\n"
            "Description=SCT secondary VNIC configuration\n"
            "After=network-online.target\n"
            "Wants=network-online.target\n"
            "\n"
            "[Service]\n"
            "Type=oneshot\n"
            f"ExecStart={self._SECONDARY_VNICS_SCRIPT}\n"
            "RemainAfterExit=yes\n"
            "\n"
            "[Install]\n"
            "WantedBy=multi-user.target\n"
        )
        service_path = f"/etc/systemd/system/{self._SECONDARY_VNICS_SERVICE}.service"
        self.remoter.sudo(f"bash -c 'cat > {service_path}' << 'SCTEOF'\n{service_unit}\nSCTEOF")
        self.remoter.sudo("systemctl daemon-reload")
        self.remoter.sudo(f"systemctl enable {self._SECONDARY_VNICS_SERVICE}.service")

        # Run the script now to apply immediately
        result = self.remoter.sudo(self._SECONDARY_VNICS_SCRIPT, ignore_status=True)
        if result.exit_status != 0:
            self.log.warning(
                "Secondary VNIC configuration script failed on %s (exit %d)", self.name, result.exit_status
            )

    @staticmethod
    def _build_secondary_vnics_script(primary_ip: str) -> str:
        """Build a shell script that queries IMDS and configures secondary VNICs."""
        return (
            "#!/bin/bash\n"
            "# Auto-generated by SCT — configures secondary VNIC routing.\n"
            "# Runs at boot via systemd and on first attach.\n"
            "set -euo pipefail\n"
            "\n"
            "# --retry-all-errors retries connection resets (curl exit 35/56) but requires curl >= 7.71\n"
            "RETRY_ALL_ERRORS=$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors)\n"
            "\n"
            "METADATA=$(curl -s -f --connect-timeout 10 \\\n"
            "  --retry 5 --retry-max-time 60 $RETRY_ALL_ERRORS \\\n"
            '  -H "Authorization: Bearer Oracle" \\\n'
            "  http://169.254.169.254/opc/v2/vnics/ 2>/dev/null) || exit 0\n"
            "\n"
            'echo "$METADATA" | python3 -c \'\n'
            "import json, subprocess, sys\n"
            "\n"
            f'PRIMARY_IP = "{primary_ip}"\n'
            "vnics = json.load(sys.stdin)\n"
            "\n"
            "for idx, vnic in enumerate(vnics):\n"
            '    pip = vnic.get("privateIp", "")\n'
            "    if pip == PRIMARY_IP:\n"
            "        continue\n"
            '    vr = vnic.get("virtualRouterIp", "")\n'
            '    cidr = vnic.get("subnetCidrBlock", "")\n'
            '    mac = vnic.get("macAddr", "")\n'
            "    if not all([pip, vr, cidr, mac]):\n"
            "        continue\n"
            "\n"
            "    # Resolve interface name by MAC\n"
            "    try:\n"
            "        out = subprocess.check_output(\n"
            '            ["ip", "-o", "link"], text=True\n'
            "        )\n"
            "    except subprocess.CalledProcessError:\n"
            "        continue\n"
            '    iface = ""\n'
            "    for line in out.splitlines():\n"
            "        if mac.lower() in line.lower():\n"
            '            iface = line.split(": ")[1].rstrip()\n'
            "            break\n"
            "    if not iface:\n"
            "        continue\n"
            "\n"
            '    prefix = cidr.split("/")[-1]\n'
            "    table_id = 100 + idx\n"
            "\n"
            "    # IPv4 configuration\n"
            "    for cmd in [\n"
            '        f"ip addr add {pip}/{prefix} dev {iface}",\n'
            '        f"ip link set dev {iface} up",\n'
            '        f"ip rule add from {pip} lookup {table_id} priority {table_id}",\n'
            '        f"ip route add default via {vr} dev {iface} table {table_id}",\n'
            '        f"ip route add {cidr} dev {iface} table {table_id}",\n'
            "    ]:\n"
            "        subprocess.run(cmd.split(), capture_output=True)\n"
            "\n"
            "    # IPv6 configuration\n"
            '    ipv6_addrs = vnic.get("ipv6Addresses", [])\n'
            '    ipv6_vr = vnic.get("ipv6VirtualRouterIp", "")\n'
            '    ipv6_cidrs = vnic.get("ipv6SubnetCidrBlocks", [])\n'
            "    for addr in ipv6_addrs:\n"
            '        subprocess.run(f"ip -6 addr add {addr}/128 dev {iface}".split(), capture_output=True)\n'
            "        subprocess.run(\n"
            '            f"ip -6 rule add from {addr} lookup {table_id} priority {table_id}".split(),\n'
            "            capture_output=True,\n"
            "        )\n"
            "    if ipv6_vr:\n"
            "        subprocess.run(\n"
            '            f"ip -6 route add default via {ipv6_vr} dev {iface} table {table_id}".split(),\n'
            "            capture_output=True,\n"
            "        )\n"
            "    for v6cidr in ipv6_cidrs:\n"
            "        subprocess.run(\n"
            '            f"ip -6 route add {v6cidr} dev {iface} table {table_id}".split(),\n'
            "            capture_output=True,\n"
            "        )\n"
            "'"
        )

    @property
    def external_address(self):
        """The communication address for usage between the test and the nodes."""
        if self.scylla_network_configuration:
            try:
                return self.scylla_network_configuration.test_communication
            except (IndexError, AttributeError):
                pass
        # Fallback: use instance's known-good IP (used during init before network config is ready)
        return self._instance.public_ip_address or self._instance.private_ip_address

    @property
    def ip_address(self):
        if self.scylla_network_configuration:
            try:
                return self.scylla_network_configuration.broadcast_address
            except (IndexError, AttributeError):
                pass
        return self.private_ip_address

    @cached_property
    def cql_address(self):
        if self.scylla_network_configuration:
            try:
                if self.test_config.IP_SSH_CONNECTIONS == "public":
                    return self.scylla_network_configuration.test_communication
                return self.scylla_network_configuration.broadcast_rpc_address
            except (IndexError, AttributeError):
                pass
        return super().cql_address

    @cached_property
    def private_dns_name(self) -> str:
        if self.scylla_network_configuration:
            return self.scylla_network_configuration.dns_private_name
        return self._resolve_private_dns_name()

    def _get_ipv6_ip_address(self) -> str | None:
        if self.scylla_network_configuration:
            return self.scylla_network_configuration.interface_ipv6_address
        return ""

    def _refresh_instance_state(self):
        if self.scylla_network_configuration:
            self.refresh_network_interfaces_info()
            public_ipv4_addresses = [
                iface.ipv4_public_address
                for iface in self.scylla_network_configuration.network_interfaces
                if iface.ipv4_public_address
            ]
            private_ipv4_addresses = [
                iface.ipv4_private_addresses[0]
                for iface in self.scylla_network_configuration.network_interfaces
                if iface.ipv4_private_addresses
            ]
            return public_ipv4_addresses, private_ipv4_addresses
        return ([self._instance.public_ip_address], [self._instance.private_ip_address])

    @retrying(n=6, sleep_time=1)
    def _set_keep_alive(self) -> bool:
        self._instance.add_tags({"keep": "alive"})
        return super()._set_keep_alive()

    @retrying(n=6, sleep_time=1)
    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self._instance.add_tags({"keep": str(duration_in_hours)})

    @property
    def vm_region(self):
        return self._instance.region

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.pricing_model.is_spot()

    def query_oci_metadata(self, path: str) -> str:
        return self.query_metadata(
            url=f"{self.METADATA_BASE_URL}{path}",
            headers={"Authorization": "Bearer Oracle"},
        )

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        Returns number of seconds to wait before next check.
        """
        try:
            self.wait_ssh_up(verbose=False)

            status = self.query_oci_metadata("termination-notification")
            try:
                terminate_action = json.loads(status)
            except ValueError:
                return SPOT_TERMINATION_CHECK_DELAY

            self.log.warning("Got spot termination notification from OCI %s", status)
            terminate_action_timestamp = time.mktime(
                datetime.strptime(terminate_action["timeCreated"], "%Y-%m-%dT%H:%M:%SZ").timetuple()
            )
            # OCI termination happens 120 seconds after notification
            termination_time = terminate_action_timestamp + 120
            next_check_delay = terminate_action["time-left"] = termination_time - time.time()

            SpotTerminationEvent(node=self, message=terminate_action).publish()
            return max(next_check_delay - SPOT_TERMINATION_CHECK_OVERHEAD, 0)
        except Exception as details:  # noqa: BLE001
            self.log.warning("Error during getting OCI spot termination notification: %s", details)

        return SPOT_TERMINATION_CHECK_DELAY

    def _restart_inner(self):
        self._instance.reboot(wait=True, hard=False)

    def hard_reboot(self):
        self._instance.reboot(wait=True, hard=True)

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance.terminate(wait=True)
        super().destroy()

    @property
    def image(self):
        return self._instance.image

    def _get_public_ip_address(self) -> str | None:
        return self._instance.public_ip_address

    def _get_private_ip_address(self) -> str | None:
        return self._instance.private_ip_address

    def _resolve_private_dns_name(self) -> str:
        """Resolve private DNS name without scylla_network_configuration (fallback path)."""
        instance_private_dns = getattr(getattr(self, "_instance", None), "private_dns_name", None)
        if instance_private_dns:
            return instance_private_dns

        metadata_hostname = None
        try:
            metadata_hostname = self.query_oci_metadata("hostname")
            if "." in metadata_hostname:
                return metadata_hostname
            self.log.warning(
                "OCI metadata hostname for node %s is short label '%s'. Trying reverse DNS for FQDN.",
                self.name,
                metadata_hostname,
            )
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "Failed to query OCI metadata hostname for node %s (%s). Falling back.",
                self.name,
                exc,
            )

        private_ip_address = self.private_ip_address
        if not private_ip_address:
            self.log.warning(
                "Node %s has no private IP while resolving private DNS name. Falling back to node name.",
                self.name,
            )
            return self.name

        try:
            return resolve_ip_to_dns(private_ip_address)
        except ValueError as exc:
            if metadata_hostname:
                self.log.warning(
                    "Unable to resolve private IP %s for node %s (%s). Falling back to metadata hostname '%s'.",
                    private_ip_address,
                    self.name,
                    exc,
                    metadata_hostname,
                )
                return metadata_hostname
            self.log.warning(
                "Unable to resolve private IP %s for node %s (%s). Falling back to private IP.",
                private_ip_address,
                self.name,
                exc,
            )
            return private_ip_address

    def create_node_certificate(
        self, cert_file, cert_key, csr_file=None, extra_ip_addresses=None, extra_dns_names=None
    ):
        """Create OCI node certificate with both short and FQDN hostname SANs when available."""
        dns_names = {
            self.public_dns_name,
            self.private_dns_name,
            getattr(getattr(self, "_instance", None), "private_dns_name", None),
        }

        try:
            metadata_hostname = self.query_oci_metadata("hostname")
        except Exception:  # noqa: BLE001
            metadata_hostname = None

        if metadata_hostname:
            dns_names.add(metadata_hostname)

        private_ip_address = self.private_ip_address
        if private_ip_address:
            try:
                reverse_dns = resolve_ip_to_dns(private_ip_address)
                if reverse_dns:
                    dns_names.add(reverse_dns)
            except ValueError:
                pass

        if extra_dns_names:
            dns_names.update(extra_dns_names)

        ip_addresses = [self.ip_address, self.public_ip_address]
        if extra_ip_addresses:
            ip_addresses.extend(extra_ip_addresses)

        create_certificate(
            cert_file,
            cert_key,
            self.name,
            ca_cert_file=CA_CERT_FILE,
            ca_key_file=CA_KEY_FILE,
            server_csr_file=csr_file,
            ip_addresses=ip_addresses,
            dns_names=sorted(dns for dns in dns_names if dns),
        )


class OciCluster(cluster.BaseCluster):
    def __init__(  # noqa: PLR0913
        self,
        image_id,
        root_disk_size,
        provisioners: List[OciProvisioner],
        credentials,
        cluster_uuid=None,
        instance_type="VM.DenseIO.E4.Flex",
        region_names=None,
        user_name="root",
        cluster_prefix="cluster",
        node_prefix="node",
        n_nodes=3,
        params=None,
        node_type=None,
    ):
        self.provisioners: List[OciProvisioner] = provisioners
        self._image_id = image_id
        self._root_disk_size = root_disk_size
        self._credentials = credentials
        self._instance_type = instance_type
        self._user_name = user_name
        self._oci_region_names = region_names
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
        self.log.debug("OciCluster constructor")

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
        instances = self._create_instances(count, instance_dc, instance_type=instance_type, rack=rack)

        self.log.debug("instances: %s", instances)
        for node_index, instance in enumerate(instances, start=self._node_index + 1):
            # NOTE: In case rack is not specified, spread nodes to different racks using
            #       the 0-based formula to stay consistent with AZ selection in '_get_availability_domain'.
            node_rack = (node_index - 1) % self.racks_count if rack is None else rack
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
            node = OciNode(
                oci_instance=instance,
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
            raise CreateOciNodeError("Failed to create node: %s" % ex) from ex

    def _create_instances(self, count, dc_idx=0, instance_type=None, rack=None) -> List[VmInstance]:
        region = self._definition_builder.regions[dc_idx]
        assert region, "no region provided, please add `oci_region_name` param"
        pricing_model = PricingModel.SPOT if "spot" in self.instance_provision else PricingModel.ON_DEMAND
        definitions = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            definition = self._definition_builder.build_instance_definition(
                region=region,
                node_type=self.node_type,
                index=node_index,
                dc_idx=dc_idx,
                instance_type=instance_type,
            )
            if rack is not None:
                definition.rack_index = rack
            definitions.append(definition)
        return provision_instances_with_fallback(
            self.provisioners[dc_idx],
            definitions=definitions,
            pricing_model=pricing_model,
            fallback_on_demand=self.params.get("instance_provision_fallback_on_demand"),
        )


class ScyllaOciCluster(cluster.BaseScyllaCluster, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners: List[OciProvisioner],
        credentials,
        instance_type="VM.DenseIO.E4.Flex",
        user_name="scyllaadm",
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

    def _reuse_cluster_setup(self, node: OciNode) -> None:
        super()._reuse_cluster_setup(node)
        node.run_startup_script()


class LoaderSetOci(cluster.BaseLoaderSet, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="VM.Standard3.Flex",
        user_name="ubuntu",
        user_prefix=None,
        n_nodes=1,
        params=None,
        region_names=None,
    ):
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "loader-set")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "loader-node")
        cluster.BaseLoaderSet.__init__(self, params=params)
        OciCluster.__init__(
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


class MonitorSetOci(cluster.BaseMonitorSet, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="VM.Standard3.Flex",
        user_name="ubuntu",
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
        OciCluster.__init__(
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
