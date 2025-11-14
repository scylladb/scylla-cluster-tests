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

import logging
from textwrap import dedent


LOGGER = logging.getLogger(__name__)

DEFAULT_AGENT_PORT = 15000
DEFAULT_AGENT_BINARY_PATH = "/usr/local/bin/sct-agent"
DEFAULT_AGENT_CONFIG_PATH = "/etc/sct-agent/config.yaml"
DEFAULT_AGENT_SERVICE_PATH = "/etc/systemd/system/sct-agent.service"


def get_agent_config_yaml(api_keys: list[str], port: int = DEFAULT_AGENT_PORT, max_concurrent_jobs: int = 10) -> str:
    """
    Generate agent configuration YAML.

    :param api_keys: List of API keys for authentication
    :param port: Agent HTTP server port
    :param max_concurrent_jobs: Maximum number of concurrent jobs

    :return: YAML configuration as string
    """
    api_keys = "\n    ".join(f'- "{key}"' for key in api_keys)
    return dedent(f"""
        server:
          host: "0.0.0.0"
          port: {port}

        security:
          api_keys:
            {api_keys}

        executor:
          max_concurrent_jobs: {max_concurrent_jobs}
          default_timeout_seconds: 300
    """)


def get_agent_systemd_service(binary_path: str = DEFAULT_AGENT_BINARY_PATH,
                              config_path: str = DEFAULT_AGENT_CONFIG_PATH) -> str:
    """
    Generate systemd service unit file.

    :param binary_path: path to agent binary on a node
    :param config_path: path to agent configuration file on a node

    :return: Systemd service unit file
    """
    return dedent(f"""
        [Unit]
        Description=SCT Agent - Command execution agent for Scylla Cluster Tests
        After=network-online.target
        Wants=network-online.target

        [Service]
        ExecStart={binary_path} --config {config_path}
        Restart=always
        RestartSec=10
        User=root
        StandardOutput=journal
        StandardError=journal
        SyslogIdentifier=sct-agent
        LimitNOFILE=65536

        [Install]
        WantedBy=multi-user.target
    """)


def install_agent_script(agent_binary_url: str,
                         api_keys: list[str],
                         port: int = DEFAULT_AGENT_PORT,
                         binary_path: str = DEFAULT_AGENT_BINARY_PATH,
                         config_path: str = DEFAULT_AGENT_CONFIG_PATH,
                         service_path: str = DEFAULT_AGENT_SERVICE_PATH,
                         max_concurrent_jobs: int = 10) -> str:
    """
    Generate bash script to install and configure SCT agent.

    :param agent_binary_url: URL to download agent binary from
    :param api_keys: list of API keys for authentication
    :param port: agent HTTP server port
    :param binary_path: installation path for agent binary
    :param config_path: path for agent configuration file
    :param service_path: path for systemd service unit file
    :param max_concurrent_jobs: Maximum concurrent jobs

    :return: bash installation script
    """
    config_yaml = get_agent_config_yaml(api_keys, port, max_concurrent_jobs).replace('$', '\\$')
    service_content = get_agent_systemd_service(binary_path, config_path).replace('$', '\\$')

    return f"""echo "Installing SCT Agent..."

echo "Downloading agent binary from {agent_binary_url}..."
curl -fsSL -o {binary_path} {agent_binary_url}
chmod +x {binary_path}

if ! {binary_path} --version >/dev/null 2>&1; then
    echo "Warning: Agent binary may not be valid, continuing anyway..."
fi

mkdir -p $(dirname {config_path})

cat > {config_path} <<'EOF'
{config_yaml}
EOF

cat > {service_path} <<'EOF'
{service_content}
EOF

systemctl daemon-reload
systemctl enable sct-agent.service

systemctl start sct-agent.service

echo "Waiting for agent to be ready..."
for i in {{1..30}}; do
    if curl -f http://localhost:{port}/health >/dev/null 2>&1; then
        echo "SCT Agent is ready!"
        break
    fi
    echo "Waiting for agent... ($i/30)"
    sleep 2
done

if ! curl -f http://localhost:{port}/health >/dev/null 2>&1; then
    echo "Warning: Agent may not have started successfully"
    systemctl status sct-agent.service || true
fi
"""


def wait_for_agent_script(port: int = DEFAULT_AGENT_PORT, timeout: int = 60) -> str:
    """Generate script to wait for SCT agent to be ready"""
    return dedent(f"""
        #!/bin/bash
        echo "Waiting for SCT Agent to be ready..."
        for i in $(seq 1 {timeout}); do
            if curl -fs http://localhost:{port}/health >/dev/null 2>&1; then
                echo "SCT Agent is ready!"
                exit 0
            fi
            sleep 1
        done
        echo "ERROR: SCT Agent did not become ready within {timeout} seconds"
        exit 1
    """).strip()


def check_agent_status_script(port: int = DEFAULT_AGENT_PORT) -> str:
    """Generate script to check SCT agent status"""
    return dedent(f"""
        #!/bin/bash
        echo "=== SCT Agent Status ==="
        echo "Port: {port}"

        if pgrep -f sct-agent >/dev/null; then
            echo "Process: RUNNING"
            ps aux | grep "[s]ct-agent"
        else
            echo "Process: NOT RUNNING"
        fi

        if command -v systemctl >/dev/null 2>&1; then
            echo "=== Systemd Service Status ==="
            systemctl status sct-agent.service --no-pager || true
        fi

        echo "=== Health Check ==="
        if curl -f http://localhost:{port}/health 2>/dev/null; then
            echo ""
            echo "Health: OK"
        else
            echo "Health: FAILED"
        fi

        echo "=== Recent Logs ==="
        if [ -f /tmp/sct-agent.log ]; then
            tail -20 /tmp/sct-agent.log
        elif command -v journalctl >/dev/null 2>&1; then
            journalctl -u sct-agent.service -n 20 --no-pager || true
        else
            echo "No logs available"
        fi
    """).strip()
