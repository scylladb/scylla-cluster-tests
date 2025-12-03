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
from functools import cached_property


AUTO_SSH_IMAGE = "jnovack/autossh:1.2.2"
AUTO_SSH_LOGFILE = "autossh.log"


class AutoSshContainerMixin:
    """Add auto_ssh container hooks to a node.

    Requires `ssh_login_info', `name' and `logdir' properties.

    See sdcm.utils.docker_utils.ContainerManager for details.
    """

    def auto_ssh_container_run_args(self, local_port, remote_port, ssh_mode="-R"):
        hostname = self.ssh_login_info["hostname"]
        port = self.ssh_login_info.get("port", "22")
        user = self.ssh_login_info["user"]
        volumes = {os.path.expanduser(self.ssh_login_info["key_file"]): {"bind": "/id_rsa", "mode": "ro,z"}}

        return dict(
            image=AUTO_SSH_IMAGE,
            name=f"{self.name}-{hostname.replace(':', '-')}-autossh",
            environment=dict(
                SSH_HOSTNAME=hostname,
                SSH_HOSTPORT=port,
                SSH_HOSTUSER=user,
                SSH_TUNNEL_HOST="127.0.0.1",
                SSH_MODE=ssh_mode,
                SSH_TUNNEL_LOCAL=local_port,
                SSH_TUNNEL_REMOTE=remote_port,
                AUTOSSH_GATETIME=0,
            ),
            network_mode="host",
            restart_policy={"Name": "always"},
            volumes=volumes,
        )

    @cached_property
    def auto_ssh_container_logfile(self):
        return os.path.join(self.logdir, AUTO_SSH_LOGFILE)
