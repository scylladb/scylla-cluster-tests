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

import pytest

from sdcm.remote.remote_base import RemoteCmdRunnerBase


@pytest.mark.parametrize("ssh_transport", ["libssh2", "fabric"])
@pytest.mark.integration
def test_ssh_none_auth(ssh_server_container, ssh_transport):
    """Test 'none' authentication for SSH transports.

    Verifies that both libssh2 and fabric can authenticate using the 'none' method.
    """
    ssh_login_info = ssh_server_container
    RemoteCmdRunnerBase.set_default_ssh_transport(ssh_transport)
    remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)

    res = remoter.run("ls -l /", retry=0)
    assert res.ok, f"SSH command failed: {res}"
    output = getattr(res, "stdout", str(res))
    for dirname in ["bin", "etc", "usr"]:
        assert dirname in output, f"Expected directory '{dirname}' not found in output: {output}"
