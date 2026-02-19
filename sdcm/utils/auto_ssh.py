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

from sdcm.utils.reverse_tunnel import get_tunnel_manager, AutosshTunnelManager


AUTO_SSH_IMAGE = "jnovack/autossh:1.2.2"
AUTO_SSH_LOGFILE = "autossh.log"


class AutoSshContainerMixin:
    """Add auto_ssh container hooks to a node.

    Requires `ssh_login_info', `name' and `logdir' properties.

    See sdcm.utils.docker_utils.ContainerManager for details.
    
    This mixin now delegates to the reverse_tunnel module for actual implementation,
    maintaining backward compatibility while supporting alternative tunnel types.
    """

    def auto_ssh_container_run_args(self, local_port, remote_port, ssh_mode="-R"):
        """Get container run arguments for autossh tunnel.
        
        This method maintains backward compatibility with existing code.
        It creates an AutosshTunnelManager and delegates to it.
        
        Args:
            local_port: Local port on the test runner
            remote_port: Remote port on the node
            ssh_mode: SSH tunnel mode (default: "-R" for reverse)
            
        Returns:
            Dictionary of container run arguments
        """
        # Get tunnel mode from test config if available
        tunnel_mode = getattr(self, 'test_config', None)
        if tunnel_mode:
            tunnel_mode = getattr(tunnel_mode, 'params', {}).get('reverse_tunnel_mode', 'autossh')
        else:
            tunnel_mode = 'autossh'
        
        # For backward compatibility, if this method is called directly,
        # we always use autossh (maintaining existing behavior)
        tunnel_manager = get_tunnel_manager(self, 'autossh', 'autossh')
        return tunnel_manager.get_container_run_args(local_port, remote_port, ssh_mode)

    @cached_property
    def auto_ssh_container_logfile(self):
        return os.path.join(self.logdir, AUTO_SSH_LOGFILE)
