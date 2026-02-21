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
# Copyright (c) 2025 ScyllaDB

import os
import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Optional

try:
    from pyngrok import ngrok, conf
    PYNGROK_AVAILABLE = True
except ImportError:
    PYNGROK_AVAILABLE = False
    ngrok = None
    conf = None

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class ReverseTunnelManager(ABC):
    """Base class for reverse tunnel managers.
    
    Manages reverse tunneling from remote nodes to the local test runner
    for services like syslog-ng, vector, and ldap.
    """

    def __init__(self, node: "BaseNode", service_name: str):
        """Initialize reverse tunnel manager.
        
        Args:
            node: The BaseNode instance
            service_name: Name of the service (e.g., 'syslog_ng', 'vector', 'ldap')
        """
        self.node = node
        self.service_name = service_name
        self.logdir = getattr(node, 'logdir', '/tmp')

    @abstractmethod
    def get_container_run_args(self, local_port: int, remote_port: int, ssh_mode: str = "-R") -> dict:
        """Get arguments for running the tunnel container.
        
        Args:
            local_port: Local port on the test runner (where service is running)
            remote_port: Remote port on the node (where service should be accessible)
            ssh_mode: SSH tunnel mode (default: "-R" for reverse)
            
        Returns:
            Dictionary of container run arguments
        """
        pass

    @cached_property
    def logfile(self) -> str:
        """Get the path to the tunnel log file."""
        return os.path.join(self.logdir, f"{self.service_name}_tunnel.log")


class AutosshTunnelManager(ReverseTunnelManager):
    """Reverse tunnel manager using autossh (Docker-based SSH tunneling).
    
    Creates a reverse SSH tunnel that allows the remote node to access
    services running on the test runner at localhost:remote_port by
    forwarding them from the test runner's local_port.
    """

    AUTOSSH_IMAGE = "jnovack/autossh:1.2.2"

    def get_container_run_args(self, local_port: int, remote_port: int, ssh_mode: str = "-R") -> dict:
        """Get arguments for running the autossh container.
        
        Args:
            local_port: Local port on the test runner where service is running
            remote_port: Remote port on the node where service should be accessible
            ssh_mode: SSH tunnel mode (default: "-R" for reverse)
            
        Returns:
            Dictionary of container run arguments for autossh
        """
        hostname = self.node.ssh_login_info["hostname"]
        port = self.node.ssh_login_info.get("port", "22")
        user = self.node.ssh_login_info["user"]
        volumes = {os.path.expanduser(self.node.ssh_login_info["key_file"]): {"bind": "/id_rsa", "mode": "ro,z"}}

        return dict(
            image=self.AUTOSSH_IMAGE,
            name=f"{self.node.name}-{hostname.replace(':', '-')}-autossh",
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


class NgrokTunnelManager(ReverseTunnelManager):
    """Reverse tunnel manager using pyngrok (Python-based ngrok client).
    
    Instead of SSH reverse tunnels, this creates a public ngrok tunnel
    that exposes the local service. Remote nodes can then connect to
    the public ngrok URL to access the service.
    """

    # Class-level storage for active tunnels to prevent duplicates
    _active_tunnels: dict[int, object] = {}

    def __init__(self, node: "BaseNode", service_name: str):
        """Initialize ngrok tunnel manager.
        
        Args:
            node: The BaseNode instance
            service_name: Name of the service (e.g., 'syslog_ng', 'vector', 'ldap')
            
        Raises:
            ImportError: If pyngrok is not available
        """
        if not PYNGROK_AVAILABLE:
            raise ImportError(
                "pyngrok is not installed. Install it with: pip install pyngrok\n"
                "Or use 'autossh' tunnel mode instead by setting: reverse_tunnel_mode: autossh"
            )
        super().__init__(node, service_name)
        self._configure_ngrok()

    def _configure_ngrok(self):
        """Configure ngrok with custom settings."""
        # Set ngrok log path
        ngrok_log = os.path.join(self.logdir, f"{self.service_name}_ngrok.log")
        
        # Configure ngrok logging
        pyngrok_config = conf.get_default()
        pyngrok_config.log_event_callback = lambda log: self._log_ngrok_event(log, ngrok_log)

    def _log_ngrok_event(self, log_message: str, log_file: str):
        """Log ngrok events to a file.
        
        Args:
            log_message: The log message from ngrok
            log_file: Path to the log file
        """
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"{log_message}\n")
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Failed to write ngrok log: %s", exc)

    def get_container_run_args(self, local_port: int, remote_port: int, ssh_mode: str = "-R") -> dict:
        """Get arguments for ngrok tunnel (returns dummy container args).
        
        For ngrok, we don't use a Docker container. Instead, we create a public
        tunnel to expose the local service. This method creates the tunnel and
        returns metadata that can be stored.
        
        The workflow is:
        1. Start ngrok tunnel exposing localhost:local_port to the internet
        2. Remote node connects to the public ngrok URL (instead of localhost:remote_port)
        
        Args:
            local_port: Local port on the test runner where service is running
            remote_port: Remote port on the node (not used for ngrok, kept for API compatibility)
            ssh_mode: SSH tunnel mode (ignored for ngrok)
            
        Returns:
            Dictionary with tunnel metadata (not actual container args)
        """
        # Check if tunnel already exists for this port
        if local_port in self._active_tunnels:
            tunnel = self._active_tunnels[local_port]
            public_url = tunnel.public_url
            LOGGER.info("Reusing existing ngrok tunnel for %s: %s -> localhost:%s", 
                       self.service_name, public_url, local_port)
        else:
            try:
                # Start ngrok tunnel exposing the local service
                tunnel = ngrok.connect(local_port, bind_tls=True)
                self._active_tunnels[local_port] = tunnel
                public_url = tunnel.public_url
                
                LOGGER.info("Ngrok tunnel for %s created: %s -> localhost:%s", 
                           self.service_name, public_url, local_port)
            except Exception as exc:
                LOGGER.error("Failed to create ngrok tunnel for %s: %s", self.service_name, exc)
                raise
        
        # Return metadata about the tunnel (not actual container args)
        # The ContainerManager will skip container creation for ngrok
        return dict(
            image="ngrok-tunnel",  # Marker to indicate this is an ngrok tunnel, not a container
            name=f"{self.node.name}-ngrok-{self.service_name}",
            environment=dict(
                TUNNEL_TYPE="ngrok",
                NGROK_URL=public_url,
                SERVICE_NAME=self.service_name,
                LOCAL_PORT=str(local_port),
                REMOTE_PORT=str(remote_port),
            ),
        )

    @classmethod
    def disconnect_all(cls):
        """Disconnect all active ngrok tunnels."""
        for port, tunnel in list(cls._active_tunnels.items()):
            try:
                ngrok.disconnect(tunnel.public_url)
                LOGGER.info("Disconnected ngrok tunnel for port %s", port)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning("Failed to disconnect ngrok tunnel for port %s: %s", port, exc)
        cls._active_tunnels.clear()

    @classmethod
    def get_tunnel_url(cls, local_port: int) -> Optional[str]:
        """Get the public URL for an active tunnel.
        
        Args:
            local_port: The local port the tunnel is connected to
            
        Returns:
            The public URL or None if no tunnel exists for that port
        """
        tunnel = cls._active_tunnels.get(local_port)
        return tunnel.public_url if tunnel else None


def get_tunnel_manager(node: "BaseNode", service_name: str, tunnel_mode: str = "autossh") -> ReverseTunnelManager:
    """Factory function to create the appropriate tunnel manager.
    
    Args:
        node: The BaseNode instance
        service_name: Name of the service
        tunnel_mode: Type of tunnel ('autossh' or 'ngrok')
        
    Returns:
        An instance of the appropriate ReverseTunnelManager subclass
    """
    tunnel_mode = tunnel_mode.lower()
    
    if tunnel_mode == "ngrok":
        return NgrokTunnelManager(node, service_name)
    elif tunnel_mode == "autossh":
        return AutosshTunnelManager(node, service_name)
    else:
        raise ValueError(f"Unknown tunnel mode: {tunnel_mode}. Must be 'autossh' or 'ngrok'")
