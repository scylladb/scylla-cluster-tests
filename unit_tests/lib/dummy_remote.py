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


import logging
import shutil
import tempfile
from pathlib import Path

from sdcm.cluster import BaseCluster, BaseNode, BaseScyllaCluster
from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import get_data_dir_path

LOGGER = logging.getLogger(__name__)


class DummyOutput:
    """Command result stand-in whose stdout and stderr both hold the given text."""

    def __init__(self, stdout):
        self.stdout = stdout
        self.stderr = stdout


class DummyRemote:
    """Remoter stand-in that echoes commands back and copies files locally."""

    @staticmethod
    def run(*args, **kwargs):
        """Log the command and return a DummyOutput echoing it as stdout."""
        LOGGER.info(args, kwargs)
        return DummyOutput(args[0])

    @staticmethod
    def is_up():
        """Report the remote host as reachable."""
        return True

    @staticmethod
    def receive_files(src, dst, *_, **__):
        """Copy src to dst on the local filesystem and report success."""
        shutil.copy(src, dst)
        return True


class LocalNode(BaseNode):
    """BaseNode on localhost that runs commands through LocalCmdRunner and stubs out cloud/SSH setup."""

    def __init__(
        self,
        name,
        parent_cluster,
        ssh_login_info=None,
        base_logdir=None,
        node_prefix=None,
        dc_idx=0,
        node_index=1,
        logdir=None,
    ):
        super().__init__(name, parent_cluster)
        self.node_index = node_index
        self.remoter = LocalCmdRunner()
        self.logdir = logdir or tempfile.mkdtemp(prefix="sct-localnode-")

    @property
    def ip_address(self):
        """Return the loopback address."""
        return "127.0.0.1"

    def wait_for_cloud_init(self):
        """No-op: a local node has no cloud-init to wait for."""

    @property
    def vm_region(self):
        """Return a fixed region name, "eu-north-1"."""
        return "eu-north-1"

    @property
    def network_interfaces(self):
        """Return None: a local node has no cloud network interfaces."""

    def refresh_network_interfaces_info(self):
        """No-op: a local node has no network interface info to refresh."""

    def _refresh_instance_state(self):
        return "127.0.0.1", "127.0.0.1"

    def _get_ipv6_ip_address(self):
        pass

    def check_spot_termination(self):
        """No-op: a local node has no spot termination to check."""

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    def restart(self):
        """No-op: restarting a local node does nothing."""

    @property
    def ssl_conf_dir(self):
        """Return the repo data_dir ssl_conf directory."""
        return Path(get_data_dir_path("ssl_conf"))

    def _init_remoter(self, ssh_login_info):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        """No-op: the local node is always reachable."""

    def set_hostname(self):
        """No-op: the local hostname stays as is."""

    def configure_remote_logging(self):
        """No-op: local nodes keep logs on the host."""

    def do_default_installations(self):
        """No-op: local nodes skip package installations."""

    def start_task_threads(self):
        """No-op: local nodes run no background task threads."""

    def _init_port_mapping(self):
        pass

    def set_keep_alive(self):
        """No-op: local nodes have no keep-alive tag."""


class LocalLoaderSetDummy(BaseCluster):
    """Loader set holding one LocalNode loader, for running stress threads against docker Scylla."""

    def __init__(self, nodes=None, params=None, logdir=None):
        self.name = "LocalLoaderSetDummy"
        self.params = params or {}
        self.added_password_suffix = False
        self.nodes = nodes if nodes is not None else [LocalNode("loader_node", parent_cluster=self)]
        self.logdir = logdir or tempfile.mkdtemp(prefix="sct-loader-")

    def add_nodes(self, *args, **kwargs):
        """Raise NotImplementedError: the loader set has a fixed node list."""
        raise NotImplementedError

    @staticmethod
    def is_kubernetes():
        """Report the loader set as non-Kubernetes."""
        return False

    def get_loader(self):
        """Return the first loader node."""
        return self.nodes[0]

    def log_message(self, *args, **kwargs):
        """No-op: loader messages are discarded."""


class LocalScyllaClusterDummy(BaseScyllaCluster, BaseCluster):
    """Scylla cluster stand-in with no nodes provisioned, configured only from params."""

    def __init__(self, params=None):
        self.name = "LocalScyllaClusterDummy"
        self.params = params or {}
        self.added_password_suffix = False
        self.log = logging.getLogger(self.name)
        self._node_cycle = None

    def add_nodes(self, *args, **kwargs):
        """Raise NotImplementedError: nodes are attached to this cluster directly."""
        raise NotImplementedError

    def get_ip_to_node_map(self, nodes=None):
        """Returns {ip: node} map for all nodes in cluster to get node by ip"""
        source = nodes if nodes is not None else self.nodes
        return {node.ip_address: node for node in source}
