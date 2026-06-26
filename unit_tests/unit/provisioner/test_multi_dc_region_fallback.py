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

import os
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from sdcm.provision.aws.az_resolver import AZResolver
from sdcm.provision.aws.capacity_errors import (
    ProvisioningCapacityExhausted,
    attach_failed_region,
    get_failed_region,
)
from sdcm.provision.aws.region_fallback import (
    enforce_multi_dc_fallback_supported,
    remap_dc_ami,
    switch_dc_region,
)
from sdcm.sct_provision.aws.cluster import DBCluster
from sdcm.sct_provision.aws.layout import SCTProvisionAWSLayout


class _BaseParams(dict):
    @property
    def region_names(self):
        return (self.get("region_name") or "").split()


class _LayoutParams(_BaseParams):
    """SCTConfiguration stand-in accepted by SCTProvisionAWSLayout for the multi-DC fallback path."""

    ami_id_params = ["ami_id_db_scylla"]


class _AmiParams(_BaseParams):
    """Stand-in for SCTConfiguration covering the bits remap/switch touch."""

    ami_id_params = ["ami_id_db_scylla", "ami_id_loader", "ami_id_monitor"]


class _StubParams(_BaseParams):
    """Minimal SCTConfiguration stand-in for DBCluster tests."""


def _make_multi_dc_layout_params(**overrides):
    return _LayoutParams(
        {
            "cluster_backend": "aws",
            "region_name": "us-east-1 eu-west-1",
            "availability_zone": "a",
            "instance_type_db": "i4i.large",
            "fallback_to_next_region": True,
            "use_capacity_reservation": False,
            "use_dedicated_host": False,
            "n_db_nodes": "3 3",
            **overrides,
        }
    )


def _instance_in(region: str) -> SimpleNamespace:
    """boto3 Instance-shaped stub whose region is resolvable by the layout's _instance_region."""
    return SimpleNamespace(
        instance_id=f"i-{region}",
        meta=SimpleNamespace(client=SimpleNamespace(meta=SimpleNamespace(region_name=region))),
    )


def _tagged_capacity_error(region: str) -> ProvisioningCapacityExhausted:
    exc = ProvisioningCapacityExhausted("no capacity")
    attach_failed_region(exc, region)
    return exc


def _fake_find_equivalent_ami(ami_id, source_region, target_regions):  # noqa: ARG001
    return [{"ami_id": f"{ami_id}@{target_regions[0]}"}]


def _fake_convert_name_to_ami_if_needed(name, regions):
    # the real helper is @lru_cache-decorated, so the regions arg must be hashable (a tuple, not a list)
    assert isinstance(regions, tuple), f"regions must be a hashable tuple for lru_cache, got {type(regions).__name__}"
    return f"ami-{name}@{regions[0]}"


@pytest.fixture(name="restore_region_env")
def restore_region_env_fixture():
    saved = os.environ.get("SCT_REGION_NAME")
    yield
    if saved is None:
        os.environ.pop("SCT_REGION_NAME", None)
    else:
        os.environ["SCT_REGION_NAME"] = saved


def _db_cluster(**param_overrides) -> DBCluster:
    """Build a DBCluster without pydantic validation so a stub params is enough for region/node mapping."""
    params = _StubParams(
        {
            "region_name": "us-east-1 eu-west-1 us-west-2",
            "n_db_nodes": "3 0 3",
            "availability_zone": "a",
            "user_prefix": "test",
            "use_zero_nodes": False,
            "n_db_zero_token_nodes": None,
            "db_type": "scylla",
            **param_overrides,
        }
    )
    return DBCluster.model_construct(params=params, test_id="testid123", common_tags={})


def _patch_cluster_internals():
    return (
        patch.object(DBCluster, "_instance_parameters", return_value=MagicMock()),
        patch.object(DBCluster, "_node_tags", return_value=[]),
        patch.object(DBCluster, "_node_names", return_value=[]),
    )


def test_nodes_map_to_absolute_region_ids_when_a_middle_dc_has_zero_nodes():
    """A zero-node middle DC must not shift later DCs: n_db_nodes='3 0 3' -> regions 0 and 2, not 0 and 1."""
    cluster = _db_cluster(n_db_nodes="3 0 3")
    assert [node.region_id for node in cluster.nodes] == [0, 0, 0, 2, 2, 2]


def test_db_cluster_provision_skips_given_region_ids():
    """skip_region_ids lets the multi-DC loop re-provision only a relocated DC, not the healthy ones."""
    cluster = _db_cluster(n_db_nodes="3 3 3")
    plan = MagicMock()
    plan.provision_instances.return_value = [object()]

    with ExitStack() as stack:
        mock_plan = stack.enter_context(patch.object(DBCluster, "provision_plan", return_value=plan))
        for ctx in _patch_cluster_internals():
            stack.enter_context(ctx)
        cluster.provision(skip_region_ids={0, 1})

    assert sorted({call.args[0] for call in mock_plan.call_args_list}) == [2]


def _capacity_client_error() -> ClientError:
    return ClientError({"Error": {"Code": "InsufficientInstanceCapacity", "Message": "x"}}, "RunInstances")


def _plan_factory(fail_region_id: int, *, raise_client_error: bool):
    """provision_plan side_effect: succeed everywhere except `fail_region_id`, which signals exhaustion."""

    def factory(region_id, _az):
        plan = MagicMock()
        if region_id != fail_region_id:
            plan.provision_instances.return_value = [object()]
        elif raise_client_error:
            plan.provision_instances.side_effect = _capacity_client_error()  # on-demand path
        else:
            plan.provision_instances.return_value = []  # spot/empty path -> ProvisioningCapacityExhausted
        return plan

    return factory


@pytest.mark.parametrize(
    ("raise_client_error", "expected_exc"),
    [
        pytest.param(True, ClientError, id="on_demand_client_error"),
        pytest.param(False, ProvisioningCapacityExhausted, id="spot_empty_result"),
    ],
)
def test_provision_tags_capacity_error_with_failing_region(raise_client_error, expected_exc):
    """The exhausted DC's region must be recoverable from the propagated capacity error, for both shapes."""
    cluster = _db_cluster(n_db_nodes="3 0 3")

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(
                DBCluster,
                "provision_plan",
                side_effect=_plan_factory(2, raise_client_error=raise_client_error),
            )
        )
        for ctx in _patch_cluster_internals():
            stack.enter_context(ctx)

        with pytest.raises(expected_exc) as exc_info:
            cluster.provision()

    assert get_failed_region(exc_info.value) == "us-west-2"


def test_remap_dc_ami_remaps_only_the_relocated_index():
    """Relocating DC 0 remaps element[0] of every per-region AMI list, leaving other DCs untouched."""
    params = _AmiParams(
        {
            "region_name": "us-east-1 eu-west-1",
            "ami_id_db_scylla": "ami-EAST ami-WEST",
            "ami_id_loader": "ami-LE ami-LW",
            "ami_id_monitor": "ami-MON",
        }
    )
    with patch("sdcm.provision.aws.region_fallback.find_equivalent_ami", side_effect=_fake_find_equivalent_ami):
        remap_dc_ami(params, dc_index=0, source_region="us-east-1", target_region="us-west-2")

    assert params["ami_id_db_scylla"] == "ami-EAST@us-west-2 ami-WEST"
    assert params["ami_id_loader"] == "ami-LE@us-west-2 ami-LW"
    assert params["ami_id_monitor"] == "ami-MON@us-west-2"  # length 1, dc_index 0 -> remapped


def test_remap_dc_ami_reresolves_named_param_by_name_not_tags():
    """Named AMI intents must be re-resolved by name in the target region, not via find_equivalent_ami."""
    params = _AmiParams(
        {
            "region_name": "us-east-1 eu-west-1",
            "ami_id_db_scylla": "ami-DBE ami-DBW",
            "ami_id_loader": "ami-LE ami-LW",
        }
    )
    params._ami_params_snapshot = {
        "ami_id_db_scylla": "scylla-2026.1.5",
        "ami_id_loader": "ubuntu-jammy-22.04",
    }

    expected_db = "ami-scylla-2026.1.5@us-west-2 ami-DBW"
    expected_loader = "ami-ubuntu-jammy-22.04@us-west-2 ami-LW"

    with (
        patch(
            "sdcm.provision.aws.region_fallback.convert_name_to_ami_if_needed",
            side_effect=_fake_convert_name_to_ami_if_needed,
        ),
        patch(
            "sdcm.provision.aws.region_fallback.find_equivalent_ami",
            side_effect=AssertionError("named AMI params must be resolved by name, not find_equivalent_ami"),
        ),
    ):
        remap_dc_ami(params, dc_index=0, source_region="us-east-1", target_region="us-west-2")

    assert params["ami_id_db_scylla"] == expected_db
    assert params["ami_id_loader"] == expected_loader


def test_switch_dc_region_splices_index_keeps_global_az_and_invalidates(restore_region_env):  # noqa: ARG001
    """Switching DC 0 rewrites only region_names[0] + its AMI, leaves availability_zone global/unchanged."""
    params = _AmiParams(
        {
            "region_name": "us-east-1 eu-west-1",
            "availability_zone": "a,b",
            "ami_id_db_scylla": "ami-EAST ami-WEST",
        }
    )
    invalidate = MagicMock()
    with patch("sdcm.provision.aws.region_fallback.find_equivalent_ami", side_effect=_fake_find_equivalent_ami):
        switch_dc_region(
            params, dc_index=0, region="us-west-2", source_region="us-east-1", invalidate_caches=invalidate
        )

    assert params["region_name"] == "us-west-2 eu-west-1"
    assert os.environ["SCT_REGION_NAME"] == "us-west-2 eu-west-1"
    assert params["availability_zone"] == "a,b"  # global AZ unchanged
    assert params["ami_id_db_scylla"] == "ami-EAST@us-west-2 ami-WEST"
    invalidate.assert_called_once_with()


@pytest.mark.parametrize(
    "feature",
    [
        pytest.param("use_capacity_reservation", id="capacity_reservation"),
        pytest.param("use_dedicated_host", id="dedicated_host"),
    ],
)
def test_enforce_multi_dc_fallback_supported_rejects_region_scoped_reservations(feature):
    """Multi-DC fallback + region-scoped CR/DH is refused (fail fast) in v1."""
    params = _AmiParams({"region_name": "us-east-1 eu-west-1", feature: True})
    with pytest.raises(ValueError, match="region-scoped"):
        enforce_multi_dc_fallback_supported(params)


def test_multi_dc_fallback_relocates_failed_db_dc_then_provisions_support_clusters(restore_region_env):  # noqa: ARG001
    """One DB DC exhausts -> relocate only it (skip the healthy DC on retry), then provision monitor/loader once."""
    params = _make_multi_dc_layout_params()
    layout = SCTProvisionAWSLayout(params)

    class _FakeDB:
        def __init__(self):
            self._provisioned_instances = []
            self.skips = []

        def provision(self, skip_region_ids=None):
            self.skips.append(set(skip_region_ids or set()))
            if len(self.skips) == 1:
                self._provisioned_instances.append(_instance_in("us-east-1"))  # DC0 ok
                raise _tagged_capacity_error("eu-west-1")  # DC1 exhausts
            self._provisioned_instances.append(_instance_in("us-west-2"))  # relocated DC1 ok

    db = _FakeDB()
    monitor, loader = MagicMock(), MagicMock()
    layout.__dict__.update(
        {
            "db_cluster": db,
            "monitoring_cluster": monitor,
            "loader_cluster": loader,
            "cs_db_cluster": None,
            "placement_group": None,
        }
    )

    def fake_switch(dc_index, region, source_region):  # noqa: ARG001
        names = params["region_name"].split()
        names[dc_index] = region
        params["region_name"] = " ".join(names)

    with (
        patch.object(AZResolver, "resolve"),
        patch("sdcm.sct_provision.aws.layout.run_pre_flight_capacity_probe"),
        patch.object(AZResolver, "get_dc_fallback_candidates", return_value=[("us-west-2", ["a"])]),
        patch.object(SCTProvisionAWSLayout, "_cleanup_dc_region") as cleanup,
        patch.object(SCTProvisionAWSLayout, "_switch_dc_region", side_effect=fake_switch),
    ):
        layout.provision()

    assert db.skips == [set(), {0}]  # first attempt all DCs; retry skips the healthy DC0
    cleanup.assert_called_once_with("eu-west-1")
    monitor.provision.assert_called_once()
    loader.provision.assert_called_once()
    assert params["region_name"] == "us-east-1 us-west-2"
