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

"""The cluster object behind `hydra update-scylla-packages`, kept apart so the CLI imports sdcm.cluster only when it runs."""

import logging
from functools import cached_property

from sdcm.cluster import BaseCluster, BaseNode, BaseScyllaCluster
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.common import SSH_KEY_AWS_DEFAULT, SSH_KEY_GCE_DEFAULT


class ScyllaDBCluster(BaseScyllaCluster, BaseCluster):
    """
    Simple composition of cluster classes to get access to update_db_packages
    API of cluster objects
    """

    nodes: list[BaseNode]

    def __init__(self, test_id, *args, **kwargs):
        self.uuid = test_id
        self.nodes = []
        self.params = kwargs.get("params") or SCTConfiguration()
        self.backend = kwargs.get("params", "aws")
        self.log = logging.getLogger(self.__class__.__name__)
        logging.getLogger("paramiko").setLevel(logging.WARNING)

    @cached_property
    def key_name(self):
        key = SSH_KEY_AWS_DEFAULT if self.backend == "aws" else SSH_KEY_GCE_DEFAULT
        return f"~/.ssh/{key}"

    def init_nodes(self, nodes, proxy):
        """`nodes`: (name, hostname, user) per DB node; `proxy`: (host, user, key) of the bastion."""
        proxy_host, proxy_user, proxy_key = proxy
        for name, hostname, user in nodes:
            login_info = {"hostname": hostname, "user": user, "key_file": self.key_name}
            node = BaseNode(name=name, parent_cluster=self, ssh_login_info=login_info)
            node.set_seed_flag(True)
            RemoteCmdRunnerBase.set_default_ssh_transport("fabric")
            node.remoter = RemoteCmdRunnerBase.create_remoter(
                **node.ssh_login_info, proxy_host=proxy_host, proxy_user=proxy_user, proxy_key=proxy_key
            )
            self.nodes.append(node)
