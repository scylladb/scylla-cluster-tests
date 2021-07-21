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

import atexit
import logging
from itertools import chain
from typing import Literal, List, Optional

from sdcm import cluster
from sdcm.remote import LOCALRUNNER, shell_script_cmd


IPTABLES_BIN = "iptables"
IPTABLES_LEGACY_BIN = "iptables-legacy"

LOGGER = logging.getLogger(__name__)


IptablesChainCommand = Literal["A", "C", "D"]


# pylint: disable=too-few-public-methods
class IptablesPodPortsRedirectMixin:
    def iptables_node_redirect_rules(self,
                                     dest_ip: str,
                                     iptables_bin: str = IPTABLES_BIN,
                                     command: IptablesChainCommand = "A") -> List[str]:
        to_ip = self._cluster_ip_service.spec.cluster_ip
        return [iptables_port_redirect_rule(iptables_bin=iptables_bin,
                                            command=command,
                                            to_ip=to_ip,
                                            to_port=p.target_port,
                                            dest_ip=dest_ip,
                                            dest_port=p.node_port) for p in self._loadbalancer_service.spec.ports]


# pylint: disable=too-few-public-methods
class IptablesPodIpRedirectMixin:
    def iptables_node_redirect_rules(self,
                                     dest_ip: str,
                                     iptables_bin: str = IPTABLES_BIN,
                                     command: IptablesChainCommand = "A") -> List[str]:
        to_ip = self._cluster_ip_service.spec.cluster_ip
        return [iptables_ip_redirect_rule(iptables_bin=iptables_bin, command=command, to_ip=to_ip, dest_ip=dest_ip), ]


class IptablesClusterOpsMixin:
    def hydra_iptables_redirect_rules(self,
                                      command: IptablesChainCommand = "A",
                                      nodes: Optional[list] = None) -> List[str]:
        if nodes is None:
            nodes = self.nodes
        return list(chain.from_iterable(node.iptables_node_redirect_rules(dest_ip=node.hydra_dest_ip,
                                                                          iptables_bin=IPTABLES_LEGACY_BIN,
                                                                          command=command) for node in nodes))

    def nodes_iptables_redirect_rules(self,
                                      command: IptablesChainCommand = "A",
                                      nodes: Optional[list] = None) -> List[str]:
        if nodes is None:
            nodes = self.nodes
        return list(chain.from_iterable(node.iptables_node_redirect_rules(dest_ip=node.nodes_dest_ip,
                                                                          command=command) for node in nodes))

    def add_hydra_iptables_rules(self, nodes: Optional[list] = None) -> None:
        add_rules_commands = self.hydra_iptables_redirect_rules(nodes=nodes)
        del_rules_commands = self.hydra_iptables_redirect_rules(command="D", nodes=nodes)

        LOCALRUNNER.sudo(shell_script_cmd("\n".join(add_rules_commands)))
        atexit.register(LOCALRUNNER.sudo, shell_script_cmd("\n".join(del_rules_commands)))

    def update_nodes_iptables_redirect_rules(self,
                                             command: IptablesChainCommand = "A",
                                             nodes: Optional[list] = None,
                                             loaders: bool = True,
                                             monitors: bool = True) -> None:
        nodes_to_update = []
        if tester := cluster.TestConfig.tester_obj():
            if loaders and tester.loaders:
                nodes_to_update.extend(tester.loaders.nodes)
            if monitors and tester.monitors:
                nodes_to_update.extend(tester.monitors.nodes)

        if nodes_to_update:
            LOGGER.debug("Found following nodes to apply new iptables rules: %s", nodes_to_update)
            iptables_rules = "\n".join(self.nodes_iptables_redirect_rules(command=command, nodes=nodes))
            for node in nodes_to_update:
                node.remoter.sudo(shell_script_cmd(iptables_rules))


# pylint: disable=too-many-arguments
def iptables_port_redirect_rule(iptables_bin: str,
                                command: IptablesChainCommand,
                                to_ip: str,
                                to_port: int,
                                dest_ip: str,
                                dest_port: int) -> str:
    return f"{iptables_bin} -t nat -{command} OUTPUT -d {to_ip} -p tcp --dport {to_port} " \
           f"-j DNAT --to-destination {dest_ip}:{dest_port}"


def iptables_ip_redirect_rule(iptables_bin: str, command: IptablesChainCommand, to_ip: str, dest_ip: str) -> str:
    return f"{iptables_bin} -t nat -{command} OUTPUT -d {to_ip} -j DNAT --to-destination {dest_ip}"
