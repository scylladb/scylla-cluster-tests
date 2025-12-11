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

VECTOR_DEV_IMAGE = "timberio/vector:0.46.1-alpine"
VECTOR_DEV_PORT = 49153  # Non-root
VECTOR_EXTERNAL_PORT = 15000  # Static external port for xcloud communication


LOGGER = logging.getLogger(__name__)


class VectorDevContainerMixin:
    @cached_property
    def vector_confpath(self) -> str:
        return generate_vector_conf_file()

    @property
    def vector_port(self) -> Optional[int]:
        return VECTOR_EXTERNAL_PORT

    @property
    def vector_log_dir(self) -> Optional[str]:
        # This should work after process that had run container was terminated and another one starts
        # that is why it is not using variable to store log directory instead
        return ContainerManager.get_host_volume_path(
            instance=self,
            container_name="vector",
            path_in_container="/var/log",
        )

    def vector_container_run_args(self, logdir: str) -> dict:
        basedir, logdir = os.path.split(logdir)
        logdir = os.path.abspath(os.path.join(os.environ.get("_SCT_LOGDIR", basedir), logdir))
        os.makedirs(logdir, exist_ok=True)
        LOGGER.debug("vector will store logs at %s", logdir)
        volumes = {
            self.vector_confpath: {"bind": "/etc/vector/vector.yaml", "mode": "ro,z"},
            logdir: {"bind": "/var/log", "mode": "z"},
        }
        username = getpass.getuser()
        return {
            'image': VECTOR_DEV_IMAGE,
            'name': f"{self.name}-vector",
            'command': " -c /etc/vector/vector.yaml",
            'auto_remove': True,
            'ports': {f"{VECTOR_DEV_PORT}/tcp": VECTOR_EXTERNAL_PORT},
            'volumes': volumes,
            'environment': {
                'PUID': getpwnam(username).pw_uid,
                'PGID': getpwnam(username).pw_gid,
            }
        }


def generate_vector_conf_file():
    conf_fd, conf_path = mkstemp(prefix="vector", suffix=".yaml")
    with os.fdopen(conf_fd, 'w') as file_obj:
        file_obj.write(VECTOR_CONF.format(port=VECTOR_DEV_PORT))
    LOGGER.debug("vector conf file created in '%s'", conf_path)
    return conf_path


VECTOR_CONF = """
# Vector configuration file
# This file is used to configure the Vector agent.

sources:
  sct_nodes:
    type: vector
    address: 0.0.0.0:{port}

transforms:
  format_logs:
    type: remap
    inputs:
      - sct_nodes
    source: |
       .timestamp = format_timestamp!(.timestamp, "%Y-%m-%dT%H:%M:%S%.3f")

       .level = upcase(to_syslog_level!(to_int!(.PRIORITY)))
       pad_count = if 7 - length(to_string(.level)) > 0 {{ 7 - length(to_string(.level)) }} else {{ 0 }}

       # Generate padding using a static array of spaces
       padding = join!(slice!([" "], 0, pad_count), "")

       .level_padded = padding + "!" + .level
        .comp = join!([.SYSLOG_IDENTIFIER, "[", ._PID, "]"], "")
       .message = join!([.timestamp, .host, .level_padded, "|" , .comp,  .message], " ")

sinks:
  file_by_host:
    type: file
    inputs:
      - format_logs
    path: /var/log/hosts/{{{{ .host }}}}/messages.log
    encoding:
      codec: text
"""
