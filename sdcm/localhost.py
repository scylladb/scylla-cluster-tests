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

import os
import logging
from typing import Optional

from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.k8s import HelmContainerMixin
from sdcm.utils.rsyslog import RSyslogContainerMixin
from sdcm.utils.gce_utils import GcloudContainerMixin
from sdcm.utils.ldap import LDAP_PORT, LDAP_SSL_PORT, LdapContainerMixin
from sdcm.utils.syslogng import SyslogNGContainerMixin

LOGGER = logging.getLogger(__name__)


class LocalHost(
    SyslogNGContainerMixin, RSyslogContainerMixin, GcloudContainerMixin, HelmContainerMixin, LdapContainerMixin
):
    def __init__(self, user_prefix: Optional[str] = None, test_id: Optional[str] = None) -> None:
        self._containers = {}
        self.tags = {}
        self.name = (f"{user_prefix}-" if user_prefix else "") + "localhost" + (f"-{test_id}" if test_id else "")

    @property
    def ldap_ports(self) -> Optional[dict]:
        return {
            "ldap_port": ContainerManager.get_container_port(self, "ldap", LDAP_PORT),
            "ldap_ssl_port": ContainerManager.get_container_port(self, "ldap", LDAP_SSL_PORT),
        }

    def destroy(self) -> None:
        ContainerManager.destroy_all_containers(self)
        try:
            os.remove(self.rsyslog_confpath)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Unable to delete `%s': %s", self.rsyslog_confpath, exc)
