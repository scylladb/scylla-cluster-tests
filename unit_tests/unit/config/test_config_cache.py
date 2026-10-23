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

"""The configuration cache shared between the hydra invocations of one build.

A full ``SCTConfiguration()`` (the docker backend from the autouse environment) is written to a file,
and a second ``SCTConfiguration()`` with ``SCT_CONFIG_CACHE_FILE`` pointing at it must come back
identical: every field, the fields excluded from ``model_dump``, the private loader attributes and
the types of the nested models.  The constructor's side effects are redone on a hit, the placement
written by provisioning still lands on top, and the variable is accepted by the unsupported-variables
check.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from sdcm import sct_abs_path
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_config.cache import (
    CACHE_FILE_ENV,
    PRIVATE_ATTRS,
    cold_load,
    config_cache_path,
    write_config_cache,
)

EXCLUDED_FIELDS = [name for name, field in SCTConfiguration.model_fields.items() if field.exclude]


@pytest.fixture(name="cold_config", scope="module")
def fixture_cold_config():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("SCT_CLUSTER_BACKEND", "docker")
        mp.setenv("SCT_USE_MGMT", "false")
        mp.setenv("SCT_SCYLLA_VERSION", "2026.1")
        mp.setenv("SCT_TEST_ID", "11111111-2222-3333-4444-555555555555")
        mp.setenv("SCT_REGION_NAME", "eu-west-1")
        mp.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
        mp.delenv(CACHE_FILE_ENV, raising=False)
        yield SCTConfiguration()


@pytest.fixture(name="cache_file")
def fixture_cache_file(cold_config, tmp_path):
    return write_config_cache(cold_config, tmp_path / "sct_config_cache.json")


@pytest.fixture(name="restored_config")
def fixture_restored_config(cache_file, monkeypatch):
    monkeypatch.setenv(CACHE_FILE_ENV, str(cache_file))
    with patch("sdcm.sct_config.config.SCTConfiguration._load_environment_variables") as load_env:
        config = SCTConfiguration()
    load_env.assert_not_called()
    return config


def test_restored_config_equals_the_one_written(cold_config, restored_config):
    assert restored_config.model_dump() == cold_config.model_dump()
    assert restored_config.model_fields_set == cold_config.model_fields_set
    for name in EXCLUDED_FIELDS:
        assert getattr(restored_config, name) == getattr(cold_config, name), name
    for name in PRIVATE_ATTRS:
        assert getattr(restored_config, name) == getattr(cold_config, name), name


def test_restored_nested_models_keep_their_types(cold_config, restored_config):
    nested = [name for name in SCTConfiguration.model_fields if isinstance(getattr(cold_config, name), BaseModel)]
    assert nested, "the model has nested models to check"
    for name in nested:
        assert type(getattr(restored_config, name)) is type(getattr(cold_config, name)), name


def test_restored_config_passes_verification(restored_config):
    restored_config.verify_configuration()


def test_hit_replays_the_load_side_effects(cache_file, monkeypatch):
    monkeypatch.setenv(CACHE_FILE_ENV, str(cache_file))
    with (
        patch("sdcm.sct_config.config.SCTConfiguration._propagate_keystore_env") as keystore_env,
        patch("sdcm.sct_config.config.add_severity_limit_rules") as severity_rules,
        patch("sdcm.sct_config.config.SCTCapacityReservation.get_cr_from_aws") as capacity_reservation,
        patch("sdcm.sct_config.config.SCTDedicatedHosts.reserve") as dedicated_hosts,
    ):
        config = SCTConfiguration()
    keystore_env.assert_called_once_with()
    severity_rules.assert_called_once_with(config.get("max_events_severities"))
    capacity_reservation.assert_called_once_with(config)
    dedicated_hosts.assert_called_once_with(config)


@pytest.fixture(name="placement_env")
def fixture_placement_env(cache_file, monkeypatch):
    """Point at the cache and pin the region variables the placement code exports, so they are restored after the test."""
    monkeypatch.setenv(CACHE_FILE_ENV, str(cache_file))
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.delenv("SCT_AVAILABILITY_ZONE", raising=False)


def test_hit_applies_the_resolved_placement_on_top(placement_env):
    placement = {"region_name": "eu-north-1", "availability_zone": "b"}
    with patch("sdcm.sct_config.config.TestConfig.read_resolved_placement", return_value=placement):
        config = SCTConfiguration()
    assert config.get("region_name") == ["eu-north-1"]
    assert config.get("availability_zone") == "b"


def test_hit_re_resolves_amis_for_a_relocated_region(placement_env):
    """A placement that moved the region without carrying AMIs needs them re-resolved, as a cold load does."""
    placement = {"region_name": "eu-north-1"}
    with (
        patch("sdcm.sct_config.config.TestConfig.read_resolved_placement", return_value=placement),
        patch("sdcm.sct_config.config.SCTConfiguration.resolve_amis") as resolve_amis,
    ):
        config = SCTConfiguration()
    assert config._resolved_placement_source_region == "eu-west-1"
    resolve_amis.assert_called_once_with(["eu-north-1"], source_region="eu-west-1")


def test_missing_cache_file_falls_back_to_a_full_load(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(CACHE_FILE_ENV, str(tmp_path / "absent.json"))
    with patch(
        "sdcm.sct_config.config.SCTConfiguration._load_environment_variables",
        wraps=SCTConfiguration._load_environment_variables,
        autospec=True,
    ) as load_env:
        config = SCTConfiguration()
    assert load_env.called
    assert config.get("cluster_backend") == "docker"
    assert "the file is absent" in caplog.text


def test_unset_variable_means_no_cache(monkeypatch):
    monkeypatch.delenv(CACHE_FILE_ENV, raising=False)
    assert config_cache_path() is None


def test_relative_cache_path_resolves_against_the_sct_root(monkeypatch):
    monkeypatch.setenv(CACHE_FILE_ENV, "sct_config_cache.json")
    assert config_cache_path() == Path(sct_abs_path("sct_config_cache.json"))


def test_cold_load_hides_the_cache_and_restores_the_variable(cache_file, monkeypatch):
    monkeypatch.setenv(CACHE_FILE_ENV, str(cache_file))
    with cold_load():
        assert config_cache_path() is None
    assert config_cache_path() == cache_file


def test_verification_accepts_the_cache_variable(cold_config, monkeypatch):
    monkeypatch.setenv(CACHE_FILE_ENV, "sct_config_cache.json")
    cold_config.verify_configuration()
