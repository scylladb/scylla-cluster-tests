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

from click.testing import CliRunner

from sct_sizing import sizing_preview

_MINIMAL_CONFIG = "unit_tests/test_configs/minimal_test_case.yaml"


def test_sizing_preview_merges_dot_and_double_underscore_env_overrides(monkeypatch):
    """SCT_SIZING_DB.vcpu (dot) and SCT_SIZING_DB__memory (__) both land in the same nested override."""
    monkeypatch.setenv("SCT_SIZING_DB.vcpu", "8")
    monkeypatch.setenv("SCT_SIZING_DB__memory", "32")

    runner = CliRunner()
    result = runner.invoke(sizing_preview, [_MINIMAL_CONFIG])

    assert result.exit_code == 0, result.output
    assert "sizing_db (env-var)" in result.output
