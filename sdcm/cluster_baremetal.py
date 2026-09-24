"""Baremetal backend for SCT (experimental).

Provides cluster management for pre-provisioned physical machines.
This backend is experimental and not regularly tested in CI.
"""

import logging
from typing import Optional, TypedDict

from invoke import UnexpectedExit

from sdcm import cluster
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis

LOGGER = logging.getLogger(__name__)

BASE_NAME = "db-node"
LOADER_NAME = "loader-node"
MONITOR_NAME = "monitor-node"


class NodeInfo(TypedDict):
    public_ip: str
    private_ip: str


class NodeCredentialInformation(TypedDict):
    username: str
    node_list: list[NodeInfo]


class BareMetalCredentials(TypedDict):
    db_nodes: NodeCredentialInformation
    loader_nodes: NodeCredentialInformation
    monitor_nodes: NodeCredentialInformation


class NodeIpsNotConfiguredError(Exception):
    pass


class PhysicalMachineNode(cluster.BaseNode):
    log = LOGGER

    def __init__(
        self,
        name,
        parent_cluster: "PhysicalMachineCluster",
        public_ip,
        private_ip,
        credentials,
        base_logdir=None,
        node_prefix=None,
        after_config=None,
        dc_idx=0,
        rack=0,
        node_index=0,
    ):
        self.node_index = node_index
        ssh_login_info = {
            "hostname": None,
            "user": getattr(parent_cluster, "ssh_username", credentials.name),
            "key_file": credentials.key_file,
        }
        self._public_ip = public_ip
        self._private_ip = private_ip
        super().__init__(
            name=name,
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            ssh_login_info=ssh_login_info,
            node_prefix=node_prefix,
            after_config=after_config,
            dc_idx=dc_idx,
            rack=rack,
        )

    def init(self):
        super().init()
        self.set_hostname()

    def wait_for_cloud_init(self):
        pass

    def _get_public_ip_address(self) -> Optional[str]:
        return self._public_ip

    @property
    def vm_region(self):
        return "baremetal"

    @property
    def region(self):
        return "baremetal"

    def scylla_setup(self, disks, devname: str):
        try:
            super().scylla_setup(disks, devname)
        except UnexpectedExit as exc:
            # Covering for case when scylla-setup script has already been run. If the command is scylla_setup
            # and there's "already" in the stdout of that command, we can skip this method safely as nics and disks
            # were already configured on this node.
            if "scylla_setup" in exc.result.command and "already" in exc.streams_for_display()[0].lower():
                return
            raise exc

    def _get_private_ip_address(self) -> Optional[str]:
        return self._private_ip

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self.log.warning(
            "_set_keep_duration is not implemented for PhysicalMachineNode, since there's no tagging for baremetal nodes."
        )

    def set_hostname(self):
        # disabling since for baremetal we aren't going to fuss with their names, they are preconfigured
        pass

    def reboot(self, hard=True, verify_ssh=True):
        raise NotImplementedError("reboot not implemented")

    def restart(self):
        self.remoter.run("sudo reboot -h now", ignore_status=True)

    def destroy(self):
        self.stop_task_threads()  # For future implementation of destroy
        self.wait_till_tasks_threads_are_stopped()
        super().destroy()


class PhysicalMachineCluster(cluster.BaseCluster):
    def __init__(self, **kwargs):
        self.nodes = []
        self.credentials = kwargs.pop("credentials")
        n_nodes = kwargs.get("n_nodes")
        self._node_public_ips = kwargs.pop("public_ips", None) or []
        self._node_private_ips = kwargs.pop("private_ips", None) or []
        node_cnt = n_nodes[0] if isinstance(n_nodes, list) else n_nodes
        if len(self._node_public_ips) < node_cnt or len(self._node_private_ips) < node_cnt:
            raise NodeIpsNotConfiguredError("Physical hosts IPs are not configured!")
        super().__init__(**kwargs)

    @property
    def ssh_username(self) -> str:
        return self._ssh_username

    def _create_node(self, name, public_ip, private_ip, dc_idx, rack=0, node_index=0, after_config=None):
        node = PhysicalMachineNode(
            name,
            parent_cluster=self,
            public_ip=public_ip,
            private_ip=private_ip,
            credentials=self.credentials[0],
            base_logdir=self.logdir,
            node_prefix=self.node_prefix,
            dc_idx=dc_idx,
            rack=rack,
            node_index=node_index,
            after_config=after_config,
        )
        node.init()
        return node

    def _reuse_cluster_setup(self, node):
        node.run_startup_script()  # Reconfigure syslog-ng.

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
        assert instance_type is None, "baremetal can't provision different types"
        # Validate the whole request first: creating some of the nodes and then raising would
        # leave them on self.nodes, and _node_index advanced, while the caller gets an exception
        # instead of the list it needs to initialize them with.
        #
        # A host needs both of its addresses, and the constructor only checks the two lists
        # against the *initial* node count -- so count the pairs, not the public IPs, or a later
        # grow slips past this guard and dies on _node_private_ips[node_index].
        configured_hosts = min(len(self._node_public_ips), len(self._node_private_ips))
        if self._node_index + count > configured_hosts:
            raise NodeIpsNotConfiguredError(
                f"{self.node_prefix}: {count} node(s) requested from index {self._node_index}, but only "
                f"{configured_hosts} physical host(s) are configured with both a public and a private address"
            )

        added_nodes = []
        for _ in range(count):
            # BaseCluster._node_index persists across calls, as it does on AWS.  Counting from
            # zero per call instead would hand a second call the same name and the same physical
            # host as the first -- and restart the rack round-robin with it.
            node_index = self._node_index
            node_name = "%s-%s" % (self.node_prefix, node_index)
            # `rack is None` is how BaseCluster says "spread the nodes yourself", which it does
            # whenever simulated_racks is set.  A physical host has no availability zone to derive
            # a rack from -- without this every node lands in RACK0, and SCT enables
            # rf_rack_valid_keyspaces for every test in defaults/test_default.yaml, so any keyspace
            # with RF > 1 is then rejected as not RF-rack-valid.  Round-robin over racks_count, as
            # the cloud backends do; the rack reaches cassandra-rackdc.properties through
            # SnitchConfig, which BaseScyllaCluster.node_setup applies once simulated_racks > 1.
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(
                node_name,
                self._node_public_ips[node_index],
                self._node_private_ips[node_index],
                dc_idx=dc_idx,
                rack=node_rack,
                node_index=node_index,
                after_config=after_config,
            )
            self.nodes.append(node)
            added_nodes.append(node)
            self._node_index += 1
        # BaseCluster.add_nodes is documented to return the list of nodes, and both cloud backends
        # do; this one returned None, so any caller using the result (the nemesis add-node paths)
        # would fail on it.
        return added_nodes


class ScyllaPhysicalCluster(cluster.BaseScyllaCluster, PhysicalMachineCluster):
    def _reuse_cluster_setup(self, node):
        PhysicalMachineCluster._reuse_cluster_setup(self, node)

    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "db-cluster"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "db-node"),
                node_type="scylla-db",
            )
        )
        super().__init__(**kwargs)


class LoaderSetPhysical(cluster.BaseLoaderSet, PhysicalMachineCluster):
    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "loader-set"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "loader-node"),
                node_type="loader",
            )
        )
        cluster.BaseLoaderSet.__init__(self, kwargs["params"])
        PhysicalMachineCluster.__init__(self, **kwargs)

    @classmethod
    def _get_node_ips_param(cls, ip_type="public"):
        return cluster.BaseLoaderSet.get_node_ips_param(ip_type)


class MonitorSetPhysical(cluster.BaseMonitorSet, PhysicalMachineCluster):
    def __init__(self, **kwargs):
        user_prefix = kwargs.pop("user_prefix")
        if username := kwargs.pop("ssh_username", None):
            self._ssh_username = username
        kwargs.update(
            dict(
                cluster_prefix=cluster.prepend_user_prefix(user_prefix, "monitor-set"),
                node_prefix=cluster.prepend_user_prefix(user_prefix, "monitor-node"),
                node_type="monitor",
            )
        )
        cluster.BaseMonitorSet.__init__(self, targets=kwargs["targets"], params=kwargs["params"])
        kwargs.pop("targets")
        PhysicalMachineCluster.__init__(self, **kwargs)
