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

"""Tests for the escaping of values in the Groovy script sent to Jenkins' Script Console."""

from pathlib import Path

import pytest

from sdcm.utils.trigger_matrix import (
    _build_trigger_groovy,
    _escape_groovy_string,
    build_job_parameters,
    load_matrix_config,
)

TRIGGERS_DIR = Path(__file__).parent.parent.parent / "configurations" / "triggers"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("plain", "plain"),
        ("it's", r"it\'s"),
        (r"back\slash", r"back\\slash"),
        # a backslash already followed by a quote must not end up escaping the escape
        (r"mix\'ed", r"mix\\\'ed"),
        ("", ""),
        # order matters: escaping quotes first would double-escape the backslashes it introduces
        ("\\", "\\\\"),
        ("'", "\\'"),
        # a single-quoted Groovy literal cannot span lines
        ("A=1\nB=2", r"A=1\nB=2"),
        ("A=1\r\nB=2", r"A=1\r\nB=2"),
        # a backslash followed by an n is not a line break, and stays a backslash
        (r"A=1\nB=2", r"A=1\\nB=2"),
    ],
)
def test_escape_groovy_string(raw, expected):
    assert _escape_groovy_string(raw) == expected


@pytest.mark.parametrize("matrix_file", sorted(TRIGGERS_DIR.glob("*.yaml")), ids=lambda path: path.name)
def test_every_parameter_literal_stays_on_one_line(matrix_file):
    """No parameter of a real trigger matrix may break the script across lines.

    A raw line break inside a single-quoted literal fails the whole Script Console call with
    "expecting ''', found '\\n'", and the job is not triggered.
    """
    config = load_matrix_config(matrix_file)
    for job in config.jobs:
        params = build_job_parameters(job, config.defaults, "master", {"unified_package": "https://x/pkg.tar.gz"})
        script = _build_trigger_groovy(job.job_name, params)
        [params_line] = [line for line in script.splitlines() if "def params = [" in line]
        assert params_line.rstrip().endswith("]"), f"{job.job_name}: a parameter breaks the script across lines"
