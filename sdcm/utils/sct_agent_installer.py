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
import os
import secrets
from textwrap import dedent


LOGGER = logging.getLogger(__name__)

DEFAULT_AGENT_PORT = 15000
DEFAULT_AGENT_BINARY_PATH = "/usr/local/bin/sct-agent"
DEFAULT_AGENT_CONFIG_PATH = "/etc/sct-agent/config.yaml"
DEFAULT_AGENT_SERVICE_PATH = "/etc/systemd/system/sct-agent.service"
DEFAULT_AGENT_LOG_PATH = "/var/log/sct-agent.log"
DEFAULT_AGENT_LOGROTATE_PATH = "/etc/logrotate.d/sct-agent"
AGENT_API_KEY_FILENAME = "agent_api_key.secret"


def get_agent_config_yaml(
        api_keys: list[str], port: int = DEFAULT_AGENT_PORT, max_concurrent_jobs: int = 10, log_level: str = "info") -> str:
    """
    Generate agent configuration YAML.

    :param api_keys: list of API keys for authentication
    :param port: agent HTTP server port
    :param max_concurrent_jobs: maximum number of concurrent jobs
    :param log_level: logging level.

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

        logging:
          level: "{log_level}"
    """)


def get_agent_systemd_service(binary_path: str = DEFAULT_AGENT_BINARY_PATH,
                              config_path: str = DEFAULT_AGENT_CONFIG_PATH,
                              log_file_path: str = DEFAULT_AGENT_LOG_PATH) -> str:
    """
    Generate systemd service unit file for SCT agent.

    :param binary_path: path to agent binary on a node
    :param config_path: path to agent configuration file on a node
    :param log_file_path: path to log file (logs to file instead of journald to avoid memory pressure)

    :return: systemd unit file content as a string with the following sections:
        - [Unit]: service description and dependencies
        - [Service]: execution configuration
        - [Install]: installation target
    """
    return dedent(f"""
        [Unit]
        Description=SCT agent - Command execution agent for Scylla Cluster Tests
        After=network-online.target
        Wants=network-online.target

        [Service]
        ExecStart={binary_path} --config {config_path} --log-file {log_file_path}
        Restart=always
        RestartSec=10
        User=root
        StandardOutput=null
        StandardError=null
        LimitNOFILE=65536

        [Install]
        WantedBy=multi-user.target
    """)


def get_agent_logrotate_config(log_file_path: str = DEFAULT_AGENT_LOG_PATH) -> str:
    """Generate logrotate configuration for SCT agent log file"""
    return dedent(f"""
        {log_file_path} {{
            daily
            rotate 7
            compress
            delaycompress
            missingok
            notifempty
            create 0644 root root
        }}
    """)


def install_agent_script(agent_binary_url: str,
                         api_keys: list[str],
                         port: int = DEFAULT_AGENT_PORT,
                         binary_path: str = DEFAULT_AGENT_BINARY_PATH,
                         config_path: str = DEFAULT_AGENT_CONFIG_PATH,
                         service_path: str = DEFAULT_AGENT_SERVICE_PATH,
                         log_file_path: str = DEFAULT_AGENT_LOG_PATH,
                         logrotate_path: str = DEFAULT_AGENT_LOGROTATE_PATH,
                         max_concurrent_jobs: int = 10,
                         log_level: str = "info") -> str:
    """
    Generate bash script to install and configure SCT agent.

    :param agent_binary_url: URL to download agent binary from
    :param api_keys: list of API keys for authentication
    :param port: agent HTTP server port
    :param binary_path: installation path for agent binary
    :param config_path: path for agent configuration file
    :param service_path: path for systemd service unit file
    :param log_file_path: path for agent log file
    :param logrotate_path: path for logrotate configuration file
    :param max_concurrent_jobs: maximum concurrent jobs
    :param log_level: logging level.

    :return: bash installation script
    """
    config_yaml = get_agent_config_yaml(api_keys, port, max_concurrent_jobs, log_level).replace('$', '\\$')
    service_content = get_agent_systemd_service(binary_path, config_path, log_file_path).replace('$', '\\$')
    logrotate_content = get_agent_logrotate_config(log_file_path).replace('$', '\\$')

    return f"""echo "Installing SCT agent..."

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

chmod 600 {config_path}

cat > {service_path} <<'EOF'
{service_content}
EOF

cat > {logrotate_path} <<'EOF'
{logrotate_content}
EOF

systemctl daemon-reload
systemctl enable sct-agent.service

systemctl start sct-agent.service

echo "Waiting for agent to be ready..."
for i in {{1..30}}; do
    if curl -f http://localhost:{port}/health >/dev/null 2>&1; then
        echo "SCT agent is ready!"
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
        echo "Waiting for SCT agent to be ready..."
        for i in $(seq 1 {timeout}); do
            if curl -fs http://localhost:{port}/health >/dev/null 2>&1; then
                echo "SCT agent is ready!"
                exit 0
            fi
            sleep 1
        done
        echo "ERROR: SCT agent did not become ready within {timeout} seconds"
        exit 1
    """).strip()


def check_agent_status_script(port: int = DEFAULT_AGENT_PORT) -> str:
    """Generate script to check SCT agent status"""
    return dedent(f"""
        #!/bin/bash
        echo "=== SCT agent Status ==="
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


def generate_agent_api_key() -> str:
    """Generate an API key for SCT agent authentication"""
    return secrets.token_urlsafe(32)


def save_agent_api_key(logdir: str, api_key: str) -> None:
    """Save the agent API key to a local file with restricted permissions"""
    key_path = os.path.join(logdir, AGENT_API_KEY_FILENAME)
    os.makedirs(logdir, exist_ok=True)
    with open(key_path, 'w', encoding='utf-8') as f:
        f.write(api_key)

    os.chmod(key_path, 0o600)


def load_agent_api_key(logdir: str) -> str | None:
    """Load the agent API key from a local file"""
    key_path = os.path.join(logdir, AGENT_API_KEY_FILENAME)
    if not os.path.exists(key_path):
        return None

    with open(key_path, 'r', encoding='utf-8') as f:
        api_key = f.read().strip()

    return api_key


def reconfigure_agent_script(
        api_keys: list[str],
        port: int = DEFAULT_AGENT_PORT,
        config_path: str = DEFAULT_AGENT_CONFIG_PATH,
        max_concurrent_jobs: int = 10,
        log_level: str = "info") -> str:
    """
    Generate bash script to reconfigure running SCT agent with a new API key(s).

    This script updates the agent configuration file and restarts the service,
    allowing the test to use a new API key with already-provisioned instances.
    """
    config_yaml = get_agent_config_yaml(api_keys, port, max_concurrent_jobs, log_level).replace('$', '\\$')

    return f"""echo "Reconfiguring SCT agent with new API key..."

[ -f {config_path} ] && cp {config_path} {config_path}.backup

cat > {config_path} <<'EOF'
{config_yaml}
EOF

chmod 600 {config_path}
systemctl restart sct-agent.service

for i in {{1..30}}; do
    if curl -f http://localhost:{port}/health >/dev/null 2>&1; then
        echo "SCT agent is ready with new API key!"
        break
    fi
    echo "Waiting for agent... ($i/30)"
    sleep 2
done

if ! curl -f http://localhost:{port}/health >/dev/null 2>&1; then
    echo "Warning: Agent may not have restarted successfully"
    systemctl status sct-agent.service || true
fi
"""
