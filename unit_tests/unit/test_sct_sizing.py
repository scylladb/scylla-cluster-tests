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


def test_sizing_preview_sums_multi_dc_node_counts(tmp_path):
    """A multi-DC n_db_nodes list is summed across DCs."""
    config = tmp_path / "multi_dc.yaml"
    config.write_text(f"{open(_MINIMAL_CONFIG).read()}\nn_db_nodes: [3, 2]\n")

    runner = CliRunner()
    result = runner.invoke(sizing_preview, [str(config)])

    assert result.exit_code == 0, result.output
    assert "Role: db (× 5 nodes)" in result.output
