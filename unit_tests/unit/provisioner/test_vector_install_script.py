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

import subprocess

from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.provision.common.utils import (
    configure_backoff_timeout,
    install_vector_service,
    update_repo_cache,
)


def test_is_syntactically_valid_bash():
    """The script runs under `bash -cxe`, so a syntax error aborts node creation."""
    full_script = configure_backoff_timeout() + install_vector_service()
    result = subprocess.run(["bash", "-n"], input=full_script, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr


def test_both_log_transports_refresh_os_package_lists_for_later_node_setup():
    """Node setup installs rsync after this script, so package lists must be refreshed first.

    The refresh happens after vector is installed, so logging is not delayed by slow mirrors.
    """
    config = ConfigurationScriptBuilder(
        syslog_host_port=("10.0.0.1", 1234),
        hostname="test-node",
        configure_sshd=False,
    )

    refresh = update_repo_cache()

    config.logs_transport = "vector"
    vector_script = config.to_string()
    assert refresh in vector_script
    assert vector_script.index(refresh) > vector_script.index("systemctl enable vector")

    config.logs_transport = "syslog-ng"
    assert refresh in config.to_string()
