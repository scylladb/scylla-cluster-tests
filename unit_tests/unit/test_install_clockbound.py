import subprocess
from unittest.mock import MagicMock

from sdcm.cluster import BaseScyllaCluster


def test_install_clockbound_script_parses():
    node = MagicMock()
    node.distro.is_rhel_like = False
    node.remoter.run.return_value.stdout = "aarch64\n"

    BaseScyllaCluster._install_clockbound(node, "3.0.0-alpha.1")

    node.install_package.assert_called_once_with("rpm2cpio cpio")
    cmd = node.remoter.sudo.call_args.args[0]
    assert "releases/download/3.0.0-alpha.1/clockbound-3.0.0-alpha.1-1.aarch64.rpm" in cmd
    # The outer shell unquotes the script as on the node; the inner bash only parses it.
    result = subprocess.run(
        ["sh", "-c", cmd.replace("bash -cxe", "bash -n -c", 1)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
