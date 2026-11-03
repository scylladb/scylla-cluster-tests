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

"""Tests that ssh2-python does not share libcrypto with the ssl module."""

import ssl  # noqa: F401  # loads the libcrypto of the ssl module
from pathlib import Path

import pytest
import ssh2.session

PROC_MAPS = Path("/proc/self/maps")

pytestmark = pytest.mark.skipif(not PROC_MAPS.exists(), reason="/proc/self/maps not found")


def test_ssh2_loads_private_libcrypto():
    """libssh2 leaves OpenSSL errors on the per-thread error queue of the libcrypto it is linked to.

    When that is the libcrypto of the ssl module, ssl reports those errors as its own failures,
    e.g. "[OSSL_DECODER] unsupported" on HTTPS calls to cloud APIs. The hydra Dockerfile repairs
    a source-built ssh2-python with auditwheel, which bundles a private copy into ssh2_python.libs.
    """
    private_libs_dir = Path(ssh2.session.__file__).parents[1] / "ssh2_python.libs"
    mapped_libcrypto = {
        Path(line.split()[-1]) for line in PROC_MAPS.read_text().splitlines() if "/libcrypto" in line.split()[-1]
    }

    assert any(path.parent == private_libs_dir for path in mapped_libcrypto), (
        f"ssh2-python does not load a libcrypto from {private_libs_dir}, loaded: {sorted(mapped_libcrypto)}"
    )
