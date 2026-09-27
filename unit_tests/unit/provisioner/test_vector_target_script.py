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

import shlex
import subprocess

from sdcm.provision.common.utils import configure_vector_target_script
from sdcm.remote.base import shell_script_cmd


def test_vector_stop_timeout_outlasts_graceful_shutdown():
    """Fedora's 45s stop timeout would SIGABRT vector during its 60s graceful shutdown."""
    script = configure_vector_target_script(host="10.0.0.1", port=6000)

    drop_in = "/etc/systemd/system/vector.service.d/sct-stop-timeout.conf"
    assert f"cat > {drop_in} <<'EOF'\n[Service]\nTimeoutStopSec=90s\nEOF" in script
    assert script.index(drop_in) < script.index("systemctl daemon-reload") < script.index("systemctl restart vector")
    assert "address: 10.0.0.1:6000" in script


def test_vector_target_script_survives_single_quote_wrapping():
    """Nodes get the script as `bash -cxe '<script>'`; a stray apostrophe would cut it short."""
    script = configure_vector_target_script(host="10.0.0.1", port=6000)

    tokens = shlex.split(shell_script_cmd(script, quote="'"))

    assert tokens[:2] == ["bash", "-cxe"]
    assert len(tokens) == 3
    assert "TimeoutStopSec=90s" in tokens[2]
    assert "systemctl restart vector" in tokens[2]
    subprocess.run(["bash", "-n", "-c", tokens[2]], check=True)
