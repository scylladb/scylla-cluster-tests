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

"""GCE create on a quota whose limit is 0 in the region must fail at once, not be retried.

The retry re-issues the insert 15 minutes apart. A zero limit is never freed by waiting, so the
retries only held artifacts runs until their 45 minute stage timeout aborted them (SCT-1550).
Another region can have the quota, so region fallback moves the cluster there instead.
"""

import os
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from sdcm.keystore import SSHKey
from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.provisioner import (
    InstanceConfigurationError,
    InstanceDefinition,
    PricingModel,
    ProvisionUnrecoverableError,
    ZeroQuotaError,
    ZoneResourcesExhaustedError,
)
from unit_tests.lib.dot_dict import DotDict

FAKE_SSH_KEY = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA fake\n", private_key=b"fake-private\n")
WAIT = "sdcm.provision.gce.instance_provider.wait_for_extended_operation"

# Verbatim from scylla-2026.4 artifacts-gce-image-test #17
ZERO_CPU_QUOTA = "QUOTA_EXCEEDED: Quota 'CPUS_PER_VM_FAMILY' exceeded.  Limit: 0.0 in region us-central1."
ZERO_SSD_QUOTA = "QUOTA_EXCEEDED: Quota 'LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY' exceeded.  Limit: 0.0 in region us-central1."
USED_UP_CPU_QUOTA = "QUOTA_EXCEEDED: Quota 'CPUS_PER_VM_FAMILY' exceeded.  Limit: 24.0 in region us-central1."


def _definition(name: str) -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="projects/scylla-images/global/images/test-image",
        type="z4d-highmem-8-highlssd",
        user_name="scylla-test",
        ssh_key=FAKE_SSH_KEY,
        root_disk_size=50,
        root_disk_type="pd-ssd",
    )


@pytest.fixture
def vm_provider():
    """A VirtualMachineProvider with its GCE clients stubbed out."""
    with patch("sdcm.provision.gce.instance_provider.get_gce_compute_instances_client") as client:
        instances_client = MagicMock()
        instances_client.get.return_value.status = "RUNNING"
        client.return_value = (instances_client, {"project_id": "test-project"})
        yield VirtualMachineProvider(
            project_id="test-project",
            zone="us-central1-a",
            test_id="test-123",
            disk_provider=MagicMock(),
            network_provider=MagicMock(),
        )


@pytest.fixture(autouse=True)
def _no_retry_sleep():
    with patch("sdcm.utils.decorators.time.sleep"):
        yield


def _create(vm_provider):
    return vm_provider.get_or_create(definitions=[_definition("node-1")], pricing_model=PricingModel.ON_DEMAND)


@pytest.mark.parametrize("message", [ZERO_CPU_QUOTA, ZERO_SSD_QUOTA])
def test_zero_quota_on_the_insert_operation_fails_without_retry(vm_provider, message):
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()) as insert,
        patch(WAIT, side_effect=google.api_core.exceptions.Forbidden(message)),
        pytest.raises(ZeroQuotaError, match="quota limit is 0"),
    ):
        _create(vm_provider)

    assert insert.call_count == 1


def test_zero_quota_on_the_insert_request_fails_without_retry(vm_provider):
    with (
        patch.object(
            vm_provider, "_build_and_insert_instance", side_effect=google.api_core.exceptions.Forbidden(ZERO_CPU_QUOTA)
        ) as insert,
        pytest.raises(ZeroQuotaError),
    ):
        _create(vm_provider)

    assert insert.call_count == 1


def test_zero_quota_during_the_retry_stops_the_retry(vm_provider):
    """A generic failure starts the retry; the zero quota it then hits must end it at once."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()) as insert,
        patch(
            WAIT,
            side_effect=[
                google.api_core.exceptions.InternalServerError("backend error"),
                google.api_core.exceptions.Forbidden(ZERO_CPU_QUOTA),
            ],
        ),
        pytest.raises(ZeroQuotaError),
    ):
        _create(vm_provider)

    assert insert.call_count == 2


def test_used_up_quota_is_still_retried(vm_provider):
    """A non-zero limit can free up when other runs end, so it keeps the retry."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()) as insert,
        patch(WAIT, side_effect=[google.api_core.exceptions.Forbidden(USED_UP_CPU_QUOTA), None]),
    ):
        instances = _create(vm_provider)

    assert insert.call_count == 2
    assert len(instances) == 1


CONFIG_ERROR = "Error 400: [pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance."


@pytest.mark.parametrize(
    "message,expected_error",
    [(ZERO_CPU_QUOTA, ZeroQuotaError), (CONFIG_ERROR, InstanceConfigurationError)],
)
def test_insert_request_error_drains_the_inserts_already_sent(vm_provider, message, expected_error):
    """Node 1's insert is in flight when node 2's request fails: it must be waited out, so a VM it
    brings up is cached or deleted instead of left running.
    """
    node_1_operation = MagicMock()
    with (
        patch.object(
            vm_provider,
            "_build_and_insert_instance",
            side_effect=[node_1_operation, google.api_core.exceptions.Forbidden(message)],
        ),
        patch.object(vm_provider, "_drain_pending_creations") as drain,
        pytest.raises(expected_error),
    ):
        vm_provider.get_or_create(
            definitions=[_definition("node-1"), _definition("node-2")], pricing_model=PricingModel.ON_DEMAND
        )

    drain.assert_called_once()
    pending = drain.call_args.args[0]
    assert [(entry[1], entry[2]) for entry in pending] == [(node_1_operation, "node-1")]


def _zero_quota_in(region: str) -> ZeroQuotaError:
    return ZeroQuotaError(f"QUOTA_EXCEEDED: Quota 'CPUS_PER_VM_FAMILY' exceeded.  Limit: 0.0 in region {region}.")


class _GceParams(DotDict):
    """Params stand-in whose ``gce_datacenters`` follows ``gce_datacenter``, as the real property does."""

    @property
    def gce_datacenters(self) -> list[str]:
        return self.get("gce_datacenter", "").split()


def _params(**overrides) -> _GceParams:
    return _GceParams(
        {
            "cluster_backend": "gce",
            "gce_datacenter": "us-central1",
            "availability_zone": "a",
            "gce_network": "qa-vpc",
            "fallback_to_next_region": True,
            "fallback_to_next_availability_zone": True,
            **overrides,
        }
    )


@pytest.fixture
def region_fallback(monkeypatch):
    """`sdcm.provision.gce.region_fallback`, with its cleanup sweep stubbed out."""
    # Imported here: at module level, right after instance_provider, region_fallback hits a circular
    # import through sdcm.provision.common.fallback.
    from sdcm.provision.gce import region_fallback as rf  # noqa: PLC0415

    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    with patch.object(rf, "cleanup_region"):
        yield rf


def test_zero_quota_error_relocates_the_region_but_never_the_zone(region_fallback):
    """A zero limit is regional: another zone of the region cannot fix it, another region can."""
    error = ZeroQuotaError(ZERO_CPU_QUOTA)

    assert not region_fallback.is_capacity_error(error)  # zone fallback and the nemesis capacity skip
    assert region_fallback.is_region_relocation_error(error)  # region and multi-DC fallback


def test_zero_quota_alone_never_reaches_the_on_demand_last_resort():
    """On-demand needs the same quota, so a zero limit must not be retried on-demand."""
    # Imported here for the same circular import as in the region_fallback fixture.
    from sdcm.sct_provision.instances_provider import _is_capacity_exhaustion  # noqa: PLC0415

    assert not _is_capacity_exhaustion(ZeroQuotaError(ZERO_CPU_QUOTA))


def test_zone_fallback_skips_a_zero_quota(region_fallback):
    params = _params()
    provision_once = MagicMock(side_effect=_zero_quota_in("us-central1"))
    with (
        patch.object(region_fallback, "GceAZResolver") as resolver,
        pytest.raises(ZeroQuotaError),
    ):
        region_fallback.provision_with_az_fallback(
            params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
        )()

    provision_once.assert_called_once()
    resolver.return_value.get_az_fallback_candidates.assert_not_called()
    assert params["availability_zone"] == "a"


def test_region_fallback_moves_a_zero_quota_to_the_region_that_has_it(region_fallback):
    """SCT-1550: z4d has no quota in us-central1 or us-east1, only in us-east4."""
    params = _params()
    provision_once = MagicMock(side_effect=[_zero_quota_in("us-central1"), _zero_quota_in("us-east1"), None])

    region_fallback.provision_with_region_fallback(
        params=params,
        test_id="t",
        network_name="qa-vpc",
        region_candidates=lambda: [("us-east1", ["b"]), ("us-east4", ["a"])],
        provision_once=provision_once,
        error_factory=ProvisionUnrecoverableError,
    )

    assert provision_once.call_count == 3
    assert [c.args[1] for c in region_fallback.cleanup_region.call_args_list] == ["us-central1", "us-east1"]
    assert params["gce_datacenter"] == "us-east4"
    assert params["availability_zone"] == "a"


def test_region_fallback_raises_zero_quota_error_when_no_region_has_the_quota(region_fallback):
    """Fails fast with a ZeroQuotaError, which the on-demand last resort leaves alone."""
    # Imported here for the same circular import as in the region_fallback fixture.
    from sdcm.sct_provision.instances_provider import _is_capacity_exhaustion  # noqa: PLC0415

    params = _params()
    provision_once = MagicMock(
        side_effect=[_zero_quota_in("us-central1"), _zero_quota_in("us-east1"), _zero_quota_in("us-east4")]
    )

    with pytest.raises(ZeroQuotaError, match="all fallback candidates") as error:
        region_fallback.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=lambda: [("us-east1", ["b"]), ("us-east4", ["a"])],
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )

    assert provision_once.call_count == 3
    assert not _is_capacity_exhaustion(error.value)
    assert params["gce_datacenter"] == "us-central1"  # restored
    assert "SCT_GCE_DATACENTER" not in os.environ


def test_region_fallback_keeps_the_callers_error_when_capacity_ran_out_too(region_fallback):
    """A region that ran out of spot capacity may have it on-demand, so the last resort stays open."""
    params = _params()
    provision_once = MagicMock(
        side_effect=[ZoneResourcesExhaustedError("ZONE_RESOURCE_POOL_EXHAUSTED"), _zero_quota_in("us-east4")]
    )

    with pytest.raises(ProvisionUnrecoverableError) as error:
        region_fallback.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=lambda: [("us-east4", ["a"])],
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )

    assert type(error.value) is ProvisionUnrecoverableError


def test_multi_dc_fallback_moves_only_the_dc_without_the_quota(region_fallback):
    params = _params(gce_datacenter="us-east1 us-central1")
    provision_once = MagicMock(side_effect=[_zero_quota_in("us-central1"), None])
    with patch.object(region_fallback, "GceAZResolver") as resolver:
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda index: (
            [("us-east4", ["a"])] if index == 1 else []
        )
        region_fallback.provision_with_dc_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )

    assert provision_once.call_count == 2
    assert params["gce_datacenter"] == "us-east1 us-east4"


def test_multi_dc_fallback_keeps_the_callers_error_when_another_dc_ran_out_of_capacity(region_fallback):
    """The relocation errors are collected over all DCs: one capacity error keeps the last resort open."""
    params = _params(gce_datacenter="us-east1 us-central1")
    provision_once = MagicMock(
        side_effect=[
            ZoneResourcesExhaustedError("ZONE_RESOURCE_POOL_EXHAUSTED in us-east1-b"),
            _zero_quota_in("us-central1"),
            _zero_quota_in("us-east4"),
        ]
    )
    with (
        patch.object(region_fallback, "GceAZResolver") as resolver,
        pytest.raises(ProvisionUnrecoverableError, match="DC 1 exhausted") as error,
    ):
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda index: (
            [("us-west1", ["a"])] if index == 0 else [("us-east4", ["a"])]
        )
        region_fallback.provision_with_dc_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )

    assert type(error.value) is ProvisionUnrecoverableError
    assert provision_once.call_count == 3
    assert params["gce_datacenter"] == "us-east1 us-central1"  # restored
