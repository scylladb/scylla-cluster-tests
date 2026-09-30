"""Regression test for the quoting of the node configuration script.

`BaseNode.configure_remote_logging()` runs the generated script as
``bash -cxe '<script>'`` (see `shell_script_cmd(script, quote="'")`), and the command
is passed to the remote login shell as a single line. A single quote wrapping
whitespace anywhere inside the script therefore terminates the outer quoting and the
remote shell fails to parse the command:

    bash: -c: line 100: syntax error near unexpected token `do'

Parsing the wrapped command with `bash -n` reproduces exactly that failure.
"""

import shutil
import subprocess

import pytest

from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.remote.base import shell_script_cmd


pytestmark = pytest.mark.skipif(not shutil.which("bash"), reason="bash is not available")


@pytest.mark.parametrize("logs_transport", ["vector", "syslog-ng"])
@pytest.mark.parametrize("install_docker", [False, True])
def test_configuration_script_survives_shell_quoting(logs_transport, install_docker):
    script = ConfigurationScriptBuilder(
        syslog_host_port=("10.0.0.1", 5000),
        logs_transport=logs_transport,
        hostname="node-1",
        install_docker=install_docker,
    ).to_string()

    # the very same wrapping as in BaseNode.configure_remote_logging()
    cmd = shell_script_cmd(script, quote="'")

    result = subprocess.run(["bash", "-n", "-c", cmd], capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"generated script is not shell-quoting safe:\n{result.stderr}"
