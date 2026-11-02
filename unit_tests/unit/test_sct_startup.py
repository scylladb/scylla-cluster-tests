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

import json
import subprocess
import sys

from sdcm import sct_abs_path

# Each one drags in seconds of imports; sct.py must only load them when a command needs them.
# On 2026.1, sct.py's click decorators read SCTConfiguration from the single-file sdcm/sct_config.py.
HEAVY_MODULES = (
    "sdcm.cluster",
    "sdcm.nemesis",
)


def test_importing_sct_loads_no_heavy_module():
    """Guards `hydra --help` and tab completion: a click option built from a heavy name at import time undoes the lazy imports."""
    code = f"import json, sys, sct; print(json.dumps([m for m in {HEAVY_MODULES!r} if m in sys.modules]))"
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=sct_abs_path(""), capture_output=True, text=True, check=True
    )

    assert json.loads(result.stdout.strip().splitlines()[-1]) == []
