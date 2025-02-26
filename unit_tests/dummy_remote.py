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

# pylint: disable=too-few-public-methods

import os
import shutil
import logging

from sdcm.cluster import BaseNode, BaseCluster, BaseScyllaCluster
from sdcm.remote import LocalCmdRunner
from sdcm.sct_config import SCTConfiguration


class DummyOutput:
    def __init__(self, stdout):
        self.stdout = stdout
        self.stderr = stdout


class DummyRemote:
    @staticmethod
    def run(*args, **kwargs):
        logging.info(args, kwargs)
        return DummyOutput(args[0])

    @staticmethod
    def is_up():
        return True

    @staticmethod
    def receive_files(src, dst):
        shutil.copy(src, dst)
        return True


class LocalNode(BaseNode):
    # pylint: disable=too-many-arguments
    def __init__(self, name, parent_cluster, ssh_login_info=None, base_logdir=None, node_prefix=None, dc_idx=0, node_index=1):
        super().__init__(name, parent_cluster)
        self.node_index = node_index
        self.remoter = LocalCmdRunner()
        self.logdir = os.path.dirname(__file__)

    @property
    def ip_address(self):
        return "127.0.0.1"

    @property
    def region(self):
        return "eu-north-1"

    def _refresh_instance_state(self):
        return "127.0.0.1", "127.0.0.1"

    def _get_ipv6_ip_address(self):
        pass

    def check_spot_termination(self):
        pass

    def _set_keep_duration(self, duration_in_minutes: int) -> None:
        pass

    def restart(self):
        pass


class LocalLoaderSetDummy(BaseCluster):
    # pylint: disable=super-init-not-called,abstract-method
    def __init__(self, nodes=None):
        self.name = "LocalLoaderSetDummy"
        self.params = SCTConfiguration()
        self.nodes = nodes if nodes is not None else [LocalNode("loader_node", parent_cluster=self)]

    @staticmethod
    def get_db_auth():
        return None

    @staticmethod
    def is_kubernetes():
        return False

    def log_message(self, *args, **kwargs):
        pass


class LocalScyllaClusterDummy(BaseScyllaCluster):
    # pylint: disable=super-init-not-called
    def __init__(self):
        self.name = "LocalScyllaClusterDummy"
        self.params = SCTConfiguration()

    @staticmethod
    def get_db_auth():
        return None

    def get_ip_to_node_map(self):
        """returns {ip: node} map for all nodes in cluster to get node by ip"""
        return {node.ip_address: node for node in self.nodes}
