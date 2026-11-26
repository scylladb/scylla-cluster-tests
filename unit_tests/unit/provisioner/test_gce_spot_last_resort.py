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

"""GCE spot fallback breadth - SCT-896 item 4.

Two pre-existing narrownesses, both of which bite harder the more GCE runs are on spot:

- zone fallback refused any batch bigger than one node, so a multi-node cluster got no zone retry at all;
- the on-demand downgrade fired only on `OperationPreemptedError` (a *running* instance reclaimed), never
  on `ZoneResourcesExhaustedError` (a request that never got capacity) - so a GCE run failed outright
  where the same run on AWS would have paid for on-demand and carried on.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_gce import GCECluster
from sdcm.provision.provisioner import (
    InstanceConfigurationError,
    PricingModel,
    ProvisionError,
    ProvisionUnrecoverableError,
    ZoneResourcesExhaustedError,
)
from sdcm.sct_provision.instances_provider import _provision_with_on_demand_last_resort

EXHAUSTED = "ZONE_RESOURCE_POOL_EXHAUSTED"


def named_mock(name, **attrs):
    """`MagicMock(name=...)` names the mock for its repr; it does NOT give it a `.name` attribute."""
    mock = MagicMock(**attrs)
    mock.name = name
    return mock


def make_provisioner(zone="us-east1-b", zones=None, existing=()):
    """A GceProvisioner double whose list_instances reports the `existing` instance names."""
    provisioner = MagicMock()
    provisioner.availability_zone = zone
    provisioner.availability_zones = zones or [zone]
    provisioner.list_instances.return_value = [named_mock(name) for name in existing]
    return provisioner


def make_cluster(provisioner, *, fallback_on_demand=True, az_fallback=True, provision="spot"):
    """A bare GCECluster carrying only what the fallback path touches."""
    cluster = GCECluster.__new__(GCECluster)
    cluster.log = MagicMock()
    cluster.params = {
        "instance_provision_fallback_on_demand": fallback_on_demand,
        "fallback_to_next_availability_zone": az_fallback,
    }
    cluster.provisioners = [provisioner]
    cluster._gce_zone_names = ["us-east1-b"]
    cluster._gce_network = "qa-vpc"
    cluster._gce_instance_type = "n2-highmem-16"
    cluster.instance_provision = provision
    cluster.node_type = "scylla-db"
    cluster.test_config = MagicMock()
    cluster.test_config.test_id.return_value = "test-id"

    builder = MagicMock()
    builder.regions = ["us-east1"]
    builder.build_instance_definition.side_effect = lambda index, **_: named_mock(f"node-{index}", type="n2-highmem-16")
    cluster._definition_builder = builder
    return cluster


@pytest.fixture
def gce_env():
    """Patch the three module-level names the fallback path reaches out through."""
    with (
        patch("sdcm.cluster_gce.provision_instances_with_fallback") as provision,
        patch("sdcm.cluster_gce.get_alternative_zones", return_value=["c", "d"]) as alternatives,
        patch("sdcm.cluster_gce.GceProvisioner", side_effect=lambda **kw: make_provisioner(kw["availability_zone"])),
    ):
        yield provision, alternatives


# modern path (`hydra provision-resources`)


def modern_params(provision="spot", fallback_on_demand=True):
    """SCT params for a GCE run in us-east1 with the given pricing model and fallback switch."""
    return {
        "instance_provision": provision,
        "instance_provision_fallback_on_demand": fallback_on_demand,
        "gce_datacenter": "us-east1",
    }


def run_last_resort(params, provision):
    """Run `provision` through the on-demand last resort with cleanup_region patched; return the patch."""
    with patch("sdcm.sct_provision.instances_provider.gce_region_fallback.cleanup_region") as cleanup:
        _provision_with_on_demand_last_resort(
            params=params, test_id="test-id", network_name="qa-vpc", provision=provision
        )
    return cleanup


def test_multi_node_batch_now_falls_back_to_another_zone(gce_env):
    """The whole point of item 4a: a 3-node batch used to be refused zone fallback outright."""
    provision, _ = gce_env
    provision.side_effect = [ZoneResourcesExhaustedError(EXHAUSTED), ["vm1", "vm2", "vm3"]]
    cluster = make_cluster(make_provisioner())

    assert cluster._create_instances([1, 2, 3]) == ["vm1", "vm2", "vm3"]
    assert provision.call_count == 2, "the second call is the retry in the next zone"
    assert cluster._gce_zone_names[0] == "c", "the cluster must follow the batch to the zone that worked"


def test_partial_batch_is_swept_before_retrying_another_zone():
    """Instance names are reused verbatim in the next zone, so leftovers both leak and collide."""
    provisioner = make_provisioner(existing=["node-1", "node-99"])
    cluster = make_cluster(provisioner)
    definitions = [named_mock("node-1"), named_mock("node-2")]

    cluster._destroy_partial_batch(provisioner, definitions)

    provisioner.terminate_instance.assert_called_once_with("node-1", wait=True)
    provisioner.cleanup.assert_not_called(), "cleanup() would take the rest of the running cluster with it"


def test_sweep_failure_does_not_abort_the_retry():
    """_destroy_partial_batch swallows a failing terminate so the zone retry still runs."""
    provisioner = make_provisioner(existing=["node-1"])
    provisioner.terminate_instance.side_effect = RuntimeError("boom")
    cluster = make_cluster(provisioner)

    cluster._destroy_partial_batch(provisioner, [named_mock("node-1")])  # must not raise


def test_on_demand_is_the_last_resort_after_every_zone_refused_spot(gce_env):
    """On-demand is requested only after spot was refused in the configured zone and every alternative zone."""
    provision, _ = gce_env
    provision.side_effect = [
        ZoneResourcesExhaustedError(EXHAUSTED),  # configured zone
        ZoneResourcesExhaustedError(EXHAUSTED),  # zone c
        ZoneResourcesExhaustedError(EXHAUSTED),  # zone d
        ["vm1"],  # on-demand, back in the configured zone
    ]
    cluster = make_cluster(make_provisioner())

    assert cluster._create_instances([1]) == ["vm1"]
    assert provision.call_args.kwargs["pricing_model"] is PricingModel.ON_DEMAND
    assert provision.call_args.kwargs["fallback_on_demand"] is False, "no second downgrade to attempt"


def test_spot_is_tried_in_every_zone_before_paying_on_demand(gce_env):
    """Ordering is the whole value: downgrading early buys on-demand capacity another zone had on spot."""
    provision, _ = gce_env
    provision.side_effect = [ZoneResourcesExhaustedError(EXHAUSTED), ["vm1"]]
    cluster = make_cluster(make_provisioner())

    cluster._create_instances([1])

    assert [call.kwargs["pricing_model"] for call in provision.call_args_list] == [
        PricingModel.SPOT,
        PricingModel.SPOT,
    ]


def test_no_on_demand_retry_when_the_run_opted_out(gce_env):
    """With the fallback disabled, a spot capacity error is raised and never downgraded to on-demand."""
    provision, _ = gce_env
    provision.side_effect = ZoneResourcesExhaustedError(EXHAUSTED)
    cluster = make_cluster(make_provisioner(), fallback_on_demand=False)

    with pytest.raises(ZoneResourcesExhaustedError):
        cluster._create_instances([1])
    assert all(call.kwargs["pricing_model"] is PricingModel.SPOT for call in provision.call_args_list)


def test_no_on_demand_retry_for_a_non_capacity_error(gce_env):
    """Auth/quota/misconfiguration fails the same way on-demand; retrying only burns time."""
    provision, _ = gce_env
    provision.side_effect = ProvisionError("PERMISSION_DENIED")
    cluster = make_cluster(make_provisioner())

    with pytest.raises(ProvisionError, match="PERMISSION_DENIED"):
        cluster._create_instances([1])
    assert provision.call_count == 1


def test_on_demand_run_is_left_alone(gce_env):
    """A run already on on-demand gets the capacity error raised without any downgrade."""
    provision, _ = gce_env
    provision.side_effect = ZoneResourcesExhaustedError(EXHAUSTED)
    cluster = make_cluster(make_provisioner(), provision="on_demand")

    with pytest.raises(ZoneResourcesExhaustedError):
        cluster._create_instances([1])


def test_multi_az_provisioner_still_refuses_zone_fallback(gce_env):
    """Swapping a multi-AZ provisioner for a single-zone one would collapse the cluster into one zone."""
    provision, _ = gce_env
    provision.side_effect = [ZoneResourcesExhaustedError(EXHAUSTED), ["vm1"]]
    cluster = make_cluster(make_provisioner(zones=["us-east1-b", "us-east1-c"]))

    # It still gets the on-demand last resort - that keeps the placement, so it is always safe.
    assert cluster._create_instances([1]) == ["vm1"]
    assert provision.call_args.kwargs["pricing_model"] is PricingModel.ON_DEMAND


def test_modern_path_retries_on_demand_once_every_region_is_exhausted():
    """run_last_resort retries once on-demand after every region refused spot, sweeping leftovers first."""
    calls = []

    def provision():
        calls.append(None)
        if len(calls) == 1:
            raise ProvisionUnrecoverableError("Failed creating clusters in region 'us-east1' and all fallbacks")

    params = modern_params()
    cleanup = run_last_resort(params, provision)

    assert len(calls) == 2
    assert params["instance_provision"] == "on_demand"
    # the failed attempt's leftovers reuse the same names as the retry
    cleanup.assert_called_once_with("test-id", "us-east1", network_name="qa-vpc")


def test_modern_path_leaves_a_configuration_error_alone():
    """`InstanceConfigurationError` shares a base class with the exhaustion error but no capacity fixes it."""

    def provision():
        raise InstanceConfigurationError("[pd-ssd, n4-standard-16] features are not compatible")

    params = modern_params()
    with pytest.raises(InstanceConfigurationError):
        run_last_resort(params, provision)
    assert params["instance_provision"] == "spot", "the run must not be silently switched"


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(modern_params(provision="on_demand"), id="already-on-demand"),
        pytest.param(modern_params(fallback_on_demand=False), id="fallback-disabled"),
    ],
)
def test_modern_path_does_not_retry_when_it_should_not(params):
    """Capacity errors propagate unchanged when the run is already on-demand or the fallback is disabled."""

    def provision():
        raise ZoneResourcesExhaustedError(EXHAUSTED)

    with pytest.raises(ZoneResourcesExhaustedError):
        run_last_resort(params, provision)


def test_modern_path_success_never_touches_the_pricing_model():
    """A successful first provision leaves the pricing model as spot."""
    params = modern_params()
    run_last_resort(params, lambda: None)
    assert params["instance_provision"] == "spot"
