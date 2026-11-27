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

"""`aws_fleet_db_use_alternatives`: EC2 Fleet alternatives to instance_type_db, derived from `sizing_db`."""

import logging
import pathlib

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.utils.cloud_catalog import interruption

# every AWS type in the catalog matching the scale test's sizing_db (vcpu 2, memory >=16, x86_64), with local disk
SCALE_SIZING_MATCHES = {"i7i.large", "i7ie.large", "i4i.large", "i3en.large"}


@pytest.fixture
def aws_env(monkeypatch, tmp_path):
    # the package conftest's minimal test case pins instance_type_db, which would bypass sizing
    minimal = pathlib.Path("unit_tests/test_configs/minimal_test_case.yaml").read_text()
    config = tmp_path / "unpinned_test_case.yaml"
    config.write_text("\n".join(line for line in minimal.splitlines() if not line.startswith("instance_type_db:")))
    monkeypatch.setenv("SCT_CONFIG_FILES", str(config))
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_INSTANCE_PROVISION", "on_demand")
    monkeypatch.setenv("SCT_SIZING_DB__vcpu", "2")
    monkeypatch.setenv("SCT_SIZING_DB__memory", ">=16")
    monkeypatch.setenv("SCT_SIZING_DB__arch", "x86_64")
    monkeypatch.setenv("SCT_AWS_FLEET_DB_USE_ALTERNATIVES", "true")
    return monkeypatch


def test_alternatives_are_every_same_size_sizing_match_except_the_pick(aws_env):
    conf = SCTConfiguration()

    primary = conf.get("instance_type_db")
    alternatives = conf.get("aws_fleet_db_alternative_types")
    assert primary == "i7i.large"  # sizing's pick: the first preferred family
    assert set(alternatives) == SCALE_SIZING_MATCHES - {primary}
    # i3en is outside the sizing preference list: still offered, but ranked after the preferred families
    assert alternatives[-1] == "i3en.large"


def test_alternatives_keep_the_size_of_the_pick(aws_env):
    """`vcpu: '>=2'` matches bigger types too, but only same-size ones can stand in for instance_type_db."""
    aws_env.setenv("SCT_SIZING_DB__vcpu", ">=2")
    conf = SCTConfiguration()

    assert set(conf.get("aws_fleet_db_alternative_types")) == SCALE_SIZING_MATCHES - {conf.get("instance_type_db")}


def test_spot_alternatives_follow_the_interruption_ranking(aws_env):
    aws_env.setenv("SCT_INSTANCE_PROVISION", "spot")
    aws_env.setattr(
        interruption,
        "aws_spot_buckets",
        lambda: {"eu-west-1": {"i7i.large": 4, "i7ie.large": 0, "i4i.large": 1, "i3en.large": 0}},
    )
    conf = SCTConfiguration()

    # the same ranking picks instance_type_db and orders what is left behind it; i3en is outside the preferred
    # families, so it stays last although it is as rarely interrupted as the pick
    assert conf.get("instance_type_db") == "i7ie.large"
    assert conf.get("aws_fleet_db_alternative_types") == ["i4i.large", "i7i.large", "i3en.large"]


def test_no_alternatives_unless_requested(aws_env):
    aws_env.setenv("SCT_AWS_FLEET_DB_USE_ALTERNATIVES", "false")

    assert SCTConfiguration().get("aws_fleet_db_alternative_types") == []


def test_explicit_instance_type_db_has_no_alternatives(aws_env, caplog):
    """Nothing was sized, so there is nothing to derive from: warn and let EC2 Fleet use that one type."""
    aws_env.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    with caplog.at_level(logging.WARNING):
        conf = SCTConfiguration()

    assert conf.get("aws_fleet_db_alternative_types") == []
    assert "instance_type_db (i4i.large) was set explicitly" in caplog.text


def test_scale_cluster_config_enables_alternatives(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "test-cases/scale/scale-cluster.yaml")

    conf = SCTConfiguration()

    assert conf.get("instance_type_db") in SCALE_SIZING_MATCHES
    assert set(conf.get("aws_fleet_db_alternative_types")) == SCALE_SIZING_MATCHES - {conf.get("instance_type_db")}
