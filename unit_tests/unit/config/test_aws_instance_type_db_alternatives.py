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


# every AWS instance type matching the scale test's sizing_db
SCALE_SIZING_MATCHES = ["i7i.large", "i7ie.large", "i4i.large", "i3en.large"]


@pytest.mark.parametrize(
    "provision, region",
    [
        pytest.param(None, None, id="defaults"),
        # what the scale-cluster Jenkins jobs run with: sizing then ranks the matches by spot interruption rate
        pytest.param("spot_fleet", "eu-west-1", id="scale_jobs"),
    ],
)
def test_scale_cluster_offers_every_sizing_match_to_the_fleet(monkeypatch, provision, region):
    """Whichever type sizing picks as instance_type_db, EC2 Fleet must still be offered all the others."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "test-cases/scale/scale-cluster.yaml")
    if provision:
        monkeypatch.setenv("SCT_INSTANCE_PROVISION", provision)
        monkeypatch.setenv("SCT_REGION_NAME", region)

    conf = SCTConfiguration()

    assert conf.get("aws_instance_type_db_alternatives") == SCALE_SIZING_MATCHES  # parsed as an actual list
    assert conf.get("instance_type_db") in SCALE_SIZING_MATCHES  # resolved from the test's sizing_db
    conf._instance_type_validation()


def validate_region_support(params):
    """Run `_instance_type_validation` with an AWS check that, like AWS itself, rejects comma-separated names."""
    config = SimpleNamespace(
        get=({"cluster_backend": "aws"} | params).get,
        region_names=["eu-west-1"],
        _validate_aws_instance_type_db_alternatives=lambda: None,
    )
    with patch(
        "sdcm.sct_config.config.aws_check_instance_type_supported",
        side_effect=lambda instance_type, region: "," not in instance_type,
    ) as check:
        SCTConfiguration._instance_type_validation(config)
    return [instance_type for instance_type, _ in (call.args for call in check.call_args_list)]


def test_alternatives_are_checked_one_by_one():
    checked = validate_region_support(
        {"instance_type_db": "i7i.large", "aws_instance_type_db_alternatives": ["i7ie.large", "i4i.large"]}
    )
    assert checked == ["i7i.large", "i7ie.large", "i4i.large"]


@pytest.mark.parametrize("param_name", ["instance_type_db", "instance_type_loader", "instance_type_monitor"])
def test_single_type_options_are_not_split(param_name):
    """Only the alternatives option is a list; a comma-separated single-type option must fail validation."""
    with pytest.raises(AssertionError, match=rf"'i4i.large,i3en.large' \(param: {param_name}\)"):
        validate_region_support({param_name: "i4i.large,i3en.large"})
