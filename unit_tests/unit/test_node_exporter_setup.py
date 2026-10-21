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

from unittest.mock import MagicMock

import pytest

from sdcm.node_exporter_setup import NODE_EXPORTER_VERSION, NodeExporterSetup


def _remoter(machine, probe="free 9100\n"):
    """A remoter whose port probe prints `probe` and whose `uname -m` prints `machine`."""
    remoter = MagicMock()
    remoter.run.side_effect = lambda cmd, **_: MagicMock(stdout=f"{machine}\n" if cmd == "uname -m" else probe)
    return remoter


@pytest.mark.parametrize(
    "machine, expected",
    [("x86_64", "amd64"), ("amd64", "amd64"), ("aarch64", "arm64"), ("arm64", "arm64")],
)
def test_install_downloads_the_release_for_the_loader_machine(machine, expected):
    remoter = _remoter(machine)

    NodeExporterSetup.install(remoter=remoter)

    remoter.run.assert_any_call("uname -m", verbose=False)
    script = remoter.sudo.call_args.args[0]
    release = f"node_exporter-{NODE_EXPORTER_VERSION}.linux-{expected}"
    assert f"{release}.tar.gz" in script
    assert f"mv {release}/node_exporter /usr/local/bin" in script
    other = "arm64" if expected == "amd64" else "amd64"
    assert f"linux-{other}" not in script


def test_install_rejects_a_machine_with_no_node_exporter_release():
    remoter = _remoter("riscv64")

    with pytest.raises(ValueError, match="riscv64"):
        NodeExporterSetup.install(remoter=remoter)

    remoter.sudo.assert_not_called()


def test_install_requires_a_node_or_a_remoter():
    with pytest.raises(AssertionError):
        NodeExporterSetup.install()


def test_running_node_exporter_is_reused():
    """A distro node_exporter, or the one set up for another node on the same host, is scraped as is."""
    remoter = _remoter("x86_64", probe="running 9100\n")

    assert NodeExporterSetup.install(remoter=remoter) == 9100
    remoter.sudo.assert_not_called()


def test_install_listens_on_the_free_port_the_probe_found():
    remoter = _remoter("x86_64", probe="free 9101\n")

    assert NodeExporterSetup.install(remoter=remoter) == 9101
    assert "--web.listen-address=:9101 " in remoter.sudo.call_args.args[0]


def test_install_fails_when_every_port_is_taken():
    remoter = _remoter("x86_64", probe="free \n")

    with pytest.raises(RuntimeError, match="No free port"):
        NodeExporterSetup.install(remoter=remoter)
    remoter.sudo.assert_not_called()
