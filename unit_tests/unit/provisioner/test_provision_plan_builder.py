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

"""Which provisioning steps the AWS upfront path tries, and in what order."""

import pytest

from sdcm.provision.aws.provisioner import AWSInstanceProvisioner
from sdcm.provision.common.provision_plan_builder import ProvisionPlanBuilder


def steps(provision_type: str, fallback_on_demand: bool) -> list[tuple[str, bool, bool]]:
    builder = ProvisionPlanBuilder(
        initial_provision_type=provision_type,
        fallback_provision_on_demand=fallback_on_demand,
        region_name="eu-west-1",
        availability_zone="a",
        provisioner=AWSInstanceProvisioner(),
    )
    return [(step.name, step.spot, step.fleet) for step in builder.provision_plan.provision_steps]


@pytest.mark.parametrize("fallback_on_demand", [True, False])
def test_spot_fleet_never_falls_back_to_on_demand(fallback_on_demand):
    """Choosing spot_fleet means spot or nothing, even when on-demand fallback is configured."""
    assert steps("spot_fleet", fallback_on_demand) == [("Spot Fleet", True, True)]


@pytest.mark.parametrize(
    "fallback_on_demand, expected",
    [
        pytest.param(True, [("Spot", True, False), ("On Demand", False, False)], id="with_fallback"),
        pytest.param(False, [("Spot", True, False)], id="without_fallback"),
    ],
)
def test_spot_falls_back_to_on_demand_only_when_configured(fallback_on_demand, expected):
    assert steps("spot", fallback_on_demand) == expected


def test_on_demand_is_a_single_step():
    assert steps("on_demand", True) == [("On Demand", False, False)]
