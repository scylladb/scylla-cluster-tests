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

from sdcm.provision.common.configuration_script import SYSLOGNG_SSH_TUNNEL_LOCAL_PORT as BUILDER_SYSLOGNG_PORT
from sdcm.test_config import TestConfig
from sdcm.utils.ldap import LDAP_SSH_TUNNEL_LOCAL_PORT, LDAP_SSH_TUNNEL_SSL_PORT


def test_ssh_tunnel_ports_are_distinct():
    """With `ip_ssh_connections: public` every tunnel binds its port on the node's loopback: one port each."""
    ports = {
        "syslog-ng": TestConfig.SYSLOGNG_SSH_TUNNEL_LOCAL_PORT,
        "vector": TestConfig.VECTOR_SSH_TUNNEL_LOCAL_PORT,
        "ldap": LDAP_SSH_TUNNEL_LOCAL_PORT,
        "ldap-ssl": LDAP_SSH_TUNNEL_SSL_PORT,
    }

    assert len(set(ports.values())) == len(ports), ports
    assert BUILDER_SYSLOGNG_PORT == TestConfig.SYSLOGNG_SSH_TUNNEL_LOCAL_PORT
