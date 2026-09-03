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


def _remoter(machine):
    remoter = MagicMock()
    remoter.run.return_value = MagicMock(stdout=f"{machine}\n")
    return remoter


@pytest.mark.parametrize(
    "machine, expected",
    [("x86_64", "amd64"), ("amd64", "amd64"), ("aarch64", "arm64"), ("arm64", "arm64")],
)
def test_install_downloads_the_release_for_the_loader_machine(machine, expected):
    remoter = _remoter(machine)

    NodeExporterSetup.install(remoter=remoter)

    remoter.run.assert_called_once_with("uname -m", verbose=False)
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
