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

import logging
from types import SimpleNamespace
from unittest import mock

import pytest

from sdcm.cluster import BaseNode, BaseScyllaCluster
from sdcm.utils.ldap import LDAP_SSH_TUNNEL_LOCAL_PORT

SYSLOGNG_PORT = 5000
VECTOR_PORT = 5003


@pytest.mark.parametrize(
    "mode,has_semanage,semanage_fails,relabels,installs",
    [
        pytest.param("Enforcing", True, False, True, False, id="enforcing"),
        pytest.param("Enforcing", False, False, True, True, id="enforcing-no-semanage"),
        pytest.param("Enforcing", True, True, True, False, id="enforcing-semanage-fails"),
        pytest.param("Permissive", True, False, False, False, id="permissive"),
        pytest.param("", False, False, False, False, id="ubuntu-no-getenforce"),
    ],
)
def test_tunnel_ports_relabelled_only_on_enforcing_node(mode, has_semanage, semanage_fails, relabels, installs):
    """An enforcing node gets its tunnel ports labelled `ssh_port_t`; a failure only logs a warning."""
    node = BaseNode.__new__(BaseNode)
    node.log = mock.Mock(spec=logging.Logger)
    node.install_package = mock.Mock()
    node.remoter = mock.Mock()
    node.remoter.run.side_effect = lambda command, **_: {
        "getenforce": mock.Mock(stdout=f"{mode}\n"),
        "command -v semanage": mock.Mock(ok=has_semanage),
    }[command]
    if semanage_fails:
        node.remoter.sudo.side_effect = RuntimeError("semanage failed")

    node._allow_reverse_tunnel_ports_in_selinux([VECTOR_PORT, LDAP_SSH_TUNNEL_LOCAL_PORT])

    expected_sudo = [
        mock.call(f"semanage port -a -t ssh_port_t -p tcp {port}")
        for port in ([VECTOR_PORT] if semanage_fails else [VECTOR_PORT, LDAP_SSH_TUNNEL_LOCAL_PORT])
    ]
    assert node.remoter.sudo.call_args_list == (expected_sudo if relabels else [])
    assert node.install_package.called == installs
    assert node.log.warning.called == semanage_fails


@pytest.mark.parametrize(
    "node_type,ldap,expected",
    [
        pytest.param("scylla-db", True, [VECTOR_PORT, LDAP_SSH_TUNNEL_LOCAL_PORT], id="db-with-ldap"),
        pytest.param("scylla-db", False, [VECTOR_PORT], id="db-without-ldap"),
        pytest.param("loader", True, [VECTOR_PORT], id="loader-gets-no-ldap-tunnel"),
    ],
)
def test_port_mapping_allows_the_ports_of_the_tunnels_it_creates(node_type, ldap, expected):
    """Every tunnel but syslog-ng's gets its node port relabelled, and each tunnel container still starts."""
    node = BaseNode.__new__(BaseNode)
    node.parent_cluster = SimpleNamespace(node_type=node_type)
    node._allow_reverse_tunnel_ports_in_selinux = mock.Mock()
    test_config = SimpleNamespace(
        IP_SSH_CONNECTIONS="public",
        SYSLOGNG_ADDRESS=("10.0.0.1", 32000),
        SYSLOGNG_SSH_TUNNEL_LOCAL_PORT=SYSLOGNG_PORT,
        VECTOR_ADDRESS=("10.0.0.1", 33000),
        VECTOR_SSH_TUNNEL_LOCAL_PORT=VECTOR_PORT,
        LDAP_ADDRESS=("10.0.0.1", 389) if ldap else None,
    )

    with (
        mock.patch.object(BaseNode, "test_config", test_config, create=True),
        mock.patch("sdcm.cluster.ContainerManager") as container_manager,
    ):
        node._init_port_mapping()

    node._allow_reverse_tunnel_ports_in_selinux.assert_called_once_with(expected)
    expected_containers = [
        mock.call(node, "auto_ssh:syslog_ng", local_port=32000, remote_port=SYSLOGNG_PORT),
        mock.call(node, "auto_ssh:vector", local_port=33000, remote_port=VECTOR_PORT),
    ]
    if LDAP_SSH_TUNNEL_LOCAL_PORT in expected:
        expected_containers.append(
            mock.call(node, "auto_ssh:ldap", local_port=389, remote_port=LDAP_SSH_TUNNEL_LOCAL_PORT)
        )
    assert container_manager.run_container.call_args_list == expected_containers


@pytest.mark.parametrize("logs_transport", ["vector", "ssh"])
def test_logging_port_keeps_its_tunnel_label_without_syslog_ng(logs_transport):
    """Only syslog-ng, which runs confined, needs the log port relabelled to `syslogd_port_t`."""
    cluster = BaseScyllaCluster.__new__(BaseScyllaCluster)
    cluster.params = {"logs_transport": logs_transport}
    node = mock.Mock()

    cluster._allow_logging_port_in_selinux(node)

    node.install_package.assert_not_called()
    node.remoter.sudo.assert_not_called()
