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

"""Validation of `aws_instance_type_db_alternatives`, the EC2 Fleet-only DB instance type alternatives."""

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from sdcm.sct_config import SCTConfiguration


def validate(primary, alternatives):
    """Run the validation against a minimal stand-in for the few config accessors it uses."""
    params = {"instance_type_db": primary, "aws_instance_type_db_alternatives": alternatives}
    config = SimpleNamespace(get=params.get, region_names=["eu-west-1"], log=logging.getLogger(__name__))
    SCTConfiguration._validate_aws_instance_type_db_alternatives(config)


@pytest.mark.parametrize(
    "alternatives",
    [
        pytest.param(["i7ie.large", "i4i.large", "i3en.large"], id="same_size_different_disk_and_generation"),
        pytest.param([], id="no_alternatives"),
        pytest.param(None, id="unset"),
    ],
)
def test_interchangeable_alternatives_are_accepted(alternatives):
    validate("i7i.large", alternatives)


def test_alternative_of_another_architecture_is_rejected():
    """i8g.large matches i7i.large on vCPU, memory and disk, but can't boot the x86_64 AMI."""
    with pytest.raises(AssertionError, match=r"'i8g.large' is arm64, but instance_type_db 'i7i.large' is x86_64"):
        validate("i7i.large", ["i4i.large", "i8g.large"])


def test_alternative_of_another_size_is_rejected():
    with pytest.raises(AssertionError, match=r"'i4i.xlarge' has 4 vCPUs and 32.0GB memory"):
        validate("i4i.large", ["i4i.xlarge"])


def test_uncatalogued_alternative_is_still_checked_for_architecture():
    with (
        patch("sdcm.sct_config.config.get_arch_from_instance_type", return_value="arm64") as get_arch,
        pytest.raises(AssertionError, match=r"'x9z.large' is arm64"),
    ):
        validate("i7i.large", ["x9z.large"])

    get_arch.assert_called_once_with("x9z.large", region_name="eu-west-1")


def test_uncatalogued_alternative_size_is_reported_as_unverified(caplog):
    with (
        patch("sdcm.sct_config.config.get_arch_from_instance_type", return_value="x86_64"),
        caplog.at_level(logging.WARNING),
    ):
        validate("i7i.large", ["x9z.large"])

    assert "Can't verify that aws_instance_type_db_alternatives entry 'x9z.large'" in caplog.text


def test_scale_cluster_config_parses_alternatives_as_a_list_and_passes_validation(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "test-cases/scale/scale-cluster.yaml")

    conf = SCTConfiguration()

    assert conf.get("instance_type_db") == "i7i.large"  # resolved from the test's sizing_db
    assert conf.get("aws_instance_type_db_alternatives") == ["i7ie.large", "i4i.large", "i3en.large"]
    conf._instance_type_validation()
