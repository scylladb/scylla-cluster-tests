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
# Copyright (c) 2021 ScyllaDB

import os
import logging
import getpass
from functools import cached_property
from pwd import getpwnam
from tempfile import mkstemp
from typing import Optional

from sdcm.utils.docker_utils import ContainerManager

SYSLOG_NG_IMAGE = "linuxserver/syslog-ng:3.30.1"
SYSLOGNG_PORT = 49153  # Non-root


LOGGER = logging.getLogger(__name__)


class SyslogNGContainerMixin:
    @cached_property
    def syslogng_confpath(self) -> str:
        return generate_syslogng_conf_file()

    @property
    def syslogng_port(self) -> Optional[int]:
        return ContainerManager.get_container_port(self, "syslogng", SYSLOGNG_PORT)

    @property
    def syslogng_log_dir(self) -> Optional[str]:
        # This should work after process that had run container was terminated and another one starts
        # that is why it is not using variable to store log directory instead
        return ContainerManager.get_host_volume_path(
            instance=self,
            container_name="syslogng",
            path_in_container="/var/log",
        )

    def syslogng_container_run_args(self, logdir: str) -> dict:
        basedir, logdir = os.path.split(logdir)
        logdir = os.path.abspath(os.path.join(os.environ.get("_SCT_LOGDIR", basedir), logdir))
        os.makedirs(logdir, exist_ok=True)
        LOGGER.debug("syslog-ng will store logs at %s", logdir)
        volumes = {
            self.syslogng_confpath: {"bind": "/config/syslog-ng.conf", "mode": "ro,z"},
            logdir: {"bind": "/var/log", "mode": "z"},
        }
        username = getpass.getuser()
        return {
            'image': SYSLOG_NG_IMAGE,
            'name': f"{self.name}-syslogng",
            'auto_remove': True,
            'ports': {f"{SYSLOGNG_PORT}/tcp": None},
            'volumes': volumes,
            'environment': {
                'PUID': getpwnam(username).pw_uid,
                'PGID': getpwnam(username).pw_gid,
            }
        }


def generate_syslogng_conf_file():
    conf_fd, conf_path = mkstemp(prefix="syslog-ng", suffix=".conf")
    with os.fdopen(conf_fd, 'w') as file_obj:
        file_obj.write(SYSLOG_CONF.format(port=SYSLOGNG_PORT))
    LOGGER.debug("syslog-ng conf file created in '%s'", conf_path)
    return conf_path


SYSLOG_CONF = """
@version: 3.29
@include "scl.conf"

options {{
  flush-timeout(500);
  keep_hostname(yes);
  create_dirs(yes);
  perm(0640);
  dir_perm(0750);
  frac-digits(3);
}};

source s_network_tcp {{
  syslog(
    transport("tcp")
    port({port})
    max-connections(1000)
  );
}};

template default_template {{
  template("${{R_ISODATE}} ${{HOST}} $(padding $(uppercase !${{LEVEL}}) 9) | ${{MSGHDR}}${{MESSAGE}}\n");
}};

destination d_local {{
  file("/var/log/hosts/${{HOST:-localhost}}/messages.log" template(default_template) );
}};

log {{
  source(s_network_tcp);
  destination(d_local);
}};
"""
