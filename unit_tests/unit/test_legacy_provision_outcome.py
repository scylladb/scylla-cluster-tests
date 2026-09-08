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

"""Spot-vs-on-demand observability on the LEGACY AWS provisioning path (SCT-850).

`ProvisionPlan` covers upfront `provision-resources`. This covers everything that never goes through it:
mid-test `add_nodes` (nemesis grow/shrink), and whole families such as artifact tests that provision via
`tester.get_cluster_aws()`. A spot->on-demand downgrade there used to be entirely silent.
"""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from sdcm.cluster_aws import AWSCluster
from sdcm.ec2_client import CreateSpotInstancesError
from sdcm.sct_provision.common.utils import INSTANCE_PROVISION_ON_DEMAND, INSTANCE_PROVISION_SPOT


def _cluster(instance_provision=INSTANCE_PROVISION_SPOT, fallback_on_demand=True):
    """A bare AWSCluster with only what fallback_provision_type touches."""
    cluster = AWSCluster.__new__(AWSCluster)
    cluster.instance_provision = instance_provision
    cluster.region_names = ["eu-west-1"]
    cluster._ec2_instance_type = "i4i.large"
    cluster.log = MagicMock()
    cluster.params = {"availability_zone": "a,b", "instance_provision_fallback_on_demand": fallback_on_demand}
    return cluster


def _run(cluster, on_demand=None, spot=None):
    """Drive fallback_provision_type with stubbed creators; return the published event kwargs."""
    with (
        patch.object(AWSCluster, "_create_on_demand_instances", **(on_demand or {})),
        patch.object(AWSCluster, "_create_spot_instances", **(spot or {})),
        patch("sdcm.cluster_aws.SpotProvisionOutcomeEvent") as mock_event,
    ):
        try:
            instances = cluster.fallback_provision_type(count=3, interfaces=[], ec2_user_data="", dc_idx=0, az_idx=1)
        except Exception as exc:  # noqa: BLE001
            instances = exc
    return instances, mock_event


def test_spot_success_records_no_downgrade():
    cluster = _cluster()
    instances, event = _run(cluster, spot={"return_value": ["i-1"]})

    assert instances == ["i-1"]
    kwargs = event.call_args.kwargs
    assert kwargs["requested"] == INSTANCE_PROVISION_SPOT
    assert kwargs["realized"] == INSTANCE_PROVISION_SPOT
    assert kwargs["count"] == 3


def test_silent_downgrade_to_on_demand_is_recorded():
    """The whole point: mid-test spot exhaustion quietly falls back, and used to leave no trace."""
    cluster = _cluster()
    instances, event = _run(
        cluster,
        spot={"side_effect": CreateSpotInstancesError("capacity-not-available")},
        on_demand={"return_value": ["i-2"]},
    )

    assert instances == ["i-2"]
    kwargs = event.call_args.kwargs
    assert kwargs["requested"] == INSTANCE_PROVISION_SPOT
    assert kwargs["realized"] == INSTANCE_PROVISION_ON_DEMAND


def test_total_failure_records_before_reraising():
    """This path raises rather than returning [] (unlike ProvisionPlan), so the event must precede the raise."""
    cluster = _cluster()
    err = CreateSpotInstancesError("capacity-not-available")
    instances, event = _run(
        cluster,
        spot={"side_effect": err},
        on_demand={"side_effect": botocore.exceptions.ClientError({"Error": {"Code": "X"}}, "RunInstances")},
    )

    assert isinstance(instances, botocore.exceptions.ClientError), "the original error must still propagate"
    assert event.call_args.kwargs["realized"] is None


def test_any_client_error_downgrades_and_is_recorded():
    """Documents existing behaviour: effectively every ClientError triggers the on-demand fallback.

    `check_spot_error()` substring-matches `SPOT_STATUS_UNEXPECTED_ERROR = "error"` against the whole
    exception text, and every botocore ClientError renders as "An error occurred (...) ...". So the
    "only retry spot-specific errors" guard never actually rejects a ClientError. Pre-existing, not changed
    here - but it means the downgrade is recorded for these too, which is the behaviour we want to pin.
    """
    cluster = _cluster()
    instances, event = _run(
        cluster,
        spot={"side_effect": botocore.exceptions.ClientError({"Error": {"Code": "Boom"}}, "RunInstances")},
        on_demand={"return_value": ["i-3"]},
    )

    assert instances == ["i-3"]
    assert event.call_args.kwargs["realized"] == INSTANCE_PROVISION_ON_DEMAND


def test_no_fallback_configured_records_failure():
    cluster = _cluster(fallback_on_demand=False)
    instances, event = _run(cluster, spot={"side_effect": CreateSpotInstancesError("capacity-not-available")})

    assert isinstance(instances, CreateSpotInstancesError)
    assert event.call_args.kwargs["realized"] is None


def test_event_carries_region_and_az():
    """az_idx is threaded through so the event can name the AZ, matching the upfront path's fields."""
    cluster = _cluster()
    _, event = _run(cluster, spot={"return_value": ["i-1"]})

    kwargs = event.call_args.kwargs
    assert kwargs["region"] == "eu-west-1"
    assert kwargs["availability_zone"] == "b", "az_idx=1 should map to the second configured AZ letter"
    assert kwargs["instance_type"] == "i4i.large"


def test_publishing_failure_never_breaks_provisioning():
    """This sits on the hot path for every add_nodes; a reporting bug must not fail node creation."""
    cluster = _cluster()
    with (
        patch.object(AWSCluster, "_create_spot_instances", return_value=["i-1"]),
        patch("sdcm.cluster_aws.SpotProvisionOutcomeEvent", side_effect=RuntimeError("boom")),
    ):
        assert cluster.fallback_provision_type(count=1, interfaces=[], ec2_user_data="", dc_idx=0, az_idx=0) == ["i-1"]


@pytest.mark.parametrize("az_idx", [0, 1, 99])
def test_out_of_range_az_index_is_tolerated(az_idx):
    """A bad index must degrade to an empty AZ, not raise out of the provisioning path."""
    cluster = _cluster()
    with (
        patch.object(AWSCluster, "_create_spot_instances", return_value=["i-1"]),
        patch("sdcm.cluster_aws.SpotProvisionOutcomeEvent") as event,
    ):
        cluster.fallback_provision_type(count=1, interfaces=[], ec2_user_data="", dc_idx=0, az_idx=az_idx)

    assert event.call_args.kwargs["availability_zone"] in {"a", "b", ""}
