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

"""The nested `nemesis` option loaded through `SCTConfiguration`: files, overlays and env vars."""

import subprocess
import sys

import pytest

from sdcm import sct_config

RUNNERS_YAML = '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/nemesis_runners.yaml"]'


def test_nested_nemesis_loads_from_config_file(monkeypatch):
    monkeypatch.setenv("SCT_CONFIG_FILES", RUNNERS_YAML)

    conf = sct_config.SCTConfiguration()

    assert conf.get("nemesis.interval") == 7
    assert [runner.class_name for runner in conf.nemesis.runners] == [
        "SisyphusMonkey",
        "SisyphusMonkey",
        "CategoricalMonkey",
        "ManagerRcloneBackup",
    ]


def test_later_config_file_deep_merges_into_nested_nemesis(monkeypatch):
    monkeypatch.setenv(
        "SCT_CONFIG_FILES", RUNNERS_YAML[:-1] + ', "unit_tests/test_configs/nemesis_runners_overlay.yaml"]'
    )

    conf = sct_config.SCTConfiguration()

    assert conf.get("nemesis.interval") == 30
    assert conf.get("nemesis.seed") == 42
    assert len(conf.nemesis.runners) == 4


def test_env_vars_override_nested_nemesis(monkeypatch):
    monkeypatch.setenv("SCT_CONFIG_FILES", RUNNERS_YAML)
    monkeypatch.setenv("SCT_NEMESIS__INTERVAL", "11")
    monkeypatch.setenv("SCT_NEMESIS__RUNNERS", '[{"NoOpMonkey": {"interval": 1}}]')

    conf = sct_config.SCTConfiguration()

    assert conf.get("nemesis.interval") == 11
    assert conf.get("nemesis.seed") == 42
    assert [(runner.class_name, runner.interval) for runner in conf.nemesis.runners] == [("NoOpMonkey", 1)]


@pytest.mark.parametrize("module", ["sdcm.nemesis", "sdcm.sct_config"])
def test_config_and_nemesis_import_without_cycle(module):
    """The config builds its runner models from the nemesis classes, so nemesis must not need the config at import."""
    result = subprocess.run(
        [sys.executable, "-W", "ignore", "-c", f"import {module}"], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr
