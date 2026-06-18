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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from sdcm.cluster import NoMonitorSet
from sdcm.cluster_aws import AWSCluster
from sdcm.exceptions import CapacityReservationError
from sdcm.provision.aws import utils as aws_utils
from sdcm.provision.aws.az_resolver import AZResolver
from sdcm.provision.aws.capacity_errors import ProvisioningCapacityExhausted, RegionAMINotFoundError
from sdcm.provision.aws.region_fallback import switch_region
from sdcm.sct_provision.aws.layout import SCTProvisionAWSLayout
from sdcm.tester import ClusterTester


class _DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _capacity_error() -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": "InsufficientInstanceCapacity", "Message": "Insufficient capacity."}},
        operation_name="RunInstances",
    )


def _make_layout_params(**overrides):
    params = _DotDict(
        {
            "cluster_backend": "aws",
            "region_name": "us-east-1",
            "availability_zone": "a",
            "instance_type_db": "i4i.large",
            "instance_type_loader": "t3.small",
            "instance_type_monitor": "t3.small",
            "pre_filter_unavailable_availability_zones": True,
            "fallback_to_next_availability_zone": True,
            "aws_fallback_to_next_availability_zone": False,
            "use_capacity_reservation": False,
            "instance_provision": "spot",
            "test_id": "test-id-123",
            "n_db_nodes": 3,
            **overrides,
        }
    )
    params.region_names = params["region_name"].split()
    return params


@pytest.fixture(name="patched_aws_region")
def patched_aws_region_fixture():
    with patch("sdcm.provision.aws.az_resolver.AwsRegion") as mock_cls:
        instance = mock_cls.return_value
        instance.get_common_availability_zones.return_value = ["us-east-1a", "us-east-1b", "us-east-1c"]
        instance.region_name = "us-east-1"
        yield instance


@pytest.fixture(name="patched_capacity_reservation")
def patched_capacity_reservation_fixture():
    with (
        patch("sdcm.sct_provision.aws.layout.SCTCapacityReservation") as mock_cr,
        patch("sdcm.sct_provision.aws.layout.SCTDedicatedHosts"),
    ):
        mock_cr.is_capacity_reservation_enabled.return_value = False
        mock_cr.reserve.return_value = None
        yield mock_cr


def test_provision_retries_on_capacity_error(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """Core fallback flow: capacity error -> cleanup -> cache clear -> retry next AZ -> success."""
    params = _make_layout_params()
    layout = SCTProvisionAWSLayout(params)
    seen_azs: list[str] = []

    def fake_do_provision():
        seen_azs.append(params["availability_zone"])
        if len(seen_azs) == 1:
            raise _capacity_error()

    with (
        patch.object(layout, "_do_provision", side_effect=fake_do_provision),
        patch.object(layout, "_cleanup_partial_provision") as mock_cleanup,
        patch.object(layout, "_clear_cluster_caches") as mock_clear,
    ):
        layout.provision()
        assert seen_azs == ["a", "b"]
        mock_cleanup.assert_called_once()
        mock_clear.assert_called_once()
    assert params["availability_zone"] == "b"


def test_provision_non_capacity_error_propagates(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """Non-capacity errors (e.g. AuthFailure) must not trigger AZ retry."""
    layout = SCTProvisionAWSLayout(_make_layout_params())
    other_error = ClientError({"Error": {"Code": "AuthFailure", "Message": "Not authorized"}}, "RunInstances")
    with patch.object(layout, "_do_provision", side_effect=other_error):
        with patch.object(layout, "_cleanup_partial_provision") as mock_cleanup:
            with pytest.raises(ClientError):
                layout.provision()
            mock_cleanup.assert_not_called()


def test_provision_retries_on_spot_capacity_exhausted(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """Spot path raises ProvisioningCapacityExhausted (no ClientError) when capacity is unavailable;
    AZ fallback must treat it the same as an on-demand capacity ClientError."""
    params = _make_layout_params(instance_provision="spot")
    layout = SCTProvisionAWSLayout(params)
    seen_azs: list[str] = []

    def fake_do_provision():
        seen_azs.append(params["availability_zone"])
        if len(seen_azs) == 1:
            raise ProvisioningCapacityExhausted("End of provision plan reached, but no instances provisioned")

    with (
        patch.object(layout, "_do_provision", side_effect=fake_do_provision),
        patch.object(layout, "_cleanup_partial_provision") as mock_cleanup,
        patch.object(layout, "_clear_cluster_caches") as mock_clear,
    ):
        layout.provision()
        assert seen_azs == ["a", "b"]
        mock_cleanup.assert_called_once()
        mock_clear.assert_called_once()
    assert params["availability_zone"] == "b"


def test_provision_capacity_reservation_enabled_skips_layout_fallback(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """CR owns AZ iteration; layout-level retry would double-fallback and orphan reservations."""
    patched_capacity_reservation.is_capacity_reservation_enabled.return_value = True
    layout = SCTProvisionAWSLayout(_make_layout_params(use_capacity_reservation=True, instance_provision="on_demand"))
    with patch.object(layout, "_do_provision", side_effect=_capacity_error()) as mock_do:
        with pytest.raises(ClientError):
            layout.provision()
    assert mock_do.call_count == 1


def _fake_instance(instance_id: str, region: str) -> SimpleNamespace:
    """Build a stub with the boto3 Instance shape used by `_cleanup_partial_provision`."""
    return SimpleNamespace(
        instance_id=instance_id,
        meta=SimpleNamespace(client=SimpleNamespace(meta=SimpleNamespace(region_name=region))),
    )


def test_cleanup_partial_provision_groups_terminations_by_region():
    """Partial instances from different regions must be terminated against each region's client."""
    layout = SCTProvisionAWSLayout(_make_layout_params(region_name="us-east-1 eu-west-1"))
    layout.__dict__["db_cluster"] = SimpleNamespace(
        _provisioned_instances=[_fake_instance("i-aaa", "us-east-1"), _fake_instance("i-bbb", "eu-west-1")]
    )
    layout.__dict__["loader_cluster"] = SimpleNamespace(_provisioned_instances=[_fake_instance("i-ccc", "us-east-1")])

    us_client, eu_client = MagicMock(), MagicMock()
    fake_clients = {"us-east-1": us_client, "eu-west-1": eu_client}
    with patch.dict("sdcm.sct_provision.aws.layout.ec2_clients", fake_clients, clear=False):
        layout._cleanup_partial_provision()

    us_client.terminate_instances.assert_called_once()
    eu_client.terminate_instances.assert_called_once()
    assert sorted(us_client.terminate_instances.call_args.kwargs["InstanceIds"]) == ["i-aaa", "i-ccc"]
    assert eu_client.terminate_instances.call_args.kwargs["InstanceIds"] == ["i-bbb"]


def _aws_describe_instance(instance_id: str, az: str, node_index: int = 0) -> dict:
    return {
        "InstanceId": instance_id,
        "Placement": {"AvailabilityZone": az},
        "Tags": [{"Key": "NodeIndex", "Value": str(node_index)}],
    }


def _fake_aws_cluster(availability_zone: str) -> SimpleNamespace:
    return SimpleNamespace(
        region_names=["us-east-1"],
        node_type="scylla-db",
        params={"availability_zone": availability_zone},
        test_config=SimpleNamespace(test_id=lambda: "test-id-123"),
    )


@pytest.fixture(name="patched_ec2_for_get_instances")
def patched_ec2_for_get_instances_fixture():
    with (
        patch("sdcm.cluster_aws.list_instances_aws") as mock_list,
        patch("sdcm.cluster_aws.ec2_client.EC2ClientWrapper") as mock_wrapper_cls,
    ):
        mock_wrapper_cls.return_value.get_instance.side_effect = lambda iid: SimpleNamespace(instance_id=iid)
        yield mock_list


def test_get_instances_finds_instances_relocated_by_az_fallback(patched_ec2_for_get_instances):
    """Instances live in AZ 'b' after fallback while params still say 'a'; az_idx=0 must still find them."""
    patched_ec2_for_get_instances.return_value = {
        "us-east-1": [
            _aws_describe_instance("i-0b1", "us-east-1b", node_index=0),
            _aws_describe_instance("i-0b2", "us-east-1b", node_index=1),
        ]
    }
    instances = AWSCluster._get_instances(_fake_aws_cluster(availability_zone="a"), dc_idx=0, az_idx=0)
    assert [i.instance_id for i in instances] == ["i-0b1", "i-0b2"]


def test_get_instances_returns_empty_when_az_idx_out_of_bounds(patched_ec2_for_get_instances):
    """az_idx beyond the number of populated AZs returns [] rather than raising IndexError."""
    patched_ec2_for_get_instances.return_value = {
        "us-east-1": [_aws_describe_instance("i-0a1", "us-east-1a", node_index=0)],
    }
    result = AWSCluster._get_instances(_fake_aws_cluster(availability_zone="a"), dc_idx=0, az_idx=5)
    assert result == []


def _make_tester_params(**overrides) -> _DotDict:
    return _DotDict(
        {
            "region_name": "us-east-1",
            "availability_zone": "a",
            "n_db_nodes": 1,
            **overrides,
        }
    )


def _make_tester_stub(params: dict) -> SimpleNamespace:
    """Build a stub with only the attributes the legacy-fallback helpers touch."""
    return SimpleNamespace(
        params=params, log=MagicMock(), db_cluster=None, cs_db_cluster=None, loaders=None, monitors=None, credentials=[]
    )


def _raise_or_none(queue):
    item = queue.pop(0)
    if isinstance(item, BaseException):
        raise item
    return item


def test_cleanup_destroys_existing_clusters_and_clears_credentials():
    stub = _make_tester_stub(_make_tester_params())
    db_cluster, loaders, monitors = MagicMock(), MagicMock(), MagicMock()
    stub.db_cluster = db_cluster
    stub.loaders = loaders
    stub.monitors = monitors
    stub.credentials = [MagicMock(), MagicMock()]

    ClusterTester._cleanup_legacy_partial_aws_clusters(stub)

    db_cluster.destroy.assert_called_once()
    loaders.destroy.assert_called_once()
    monitors.destroy.assert_called_once()
    assert stub.db_cluster is None
    assert stub.loaders is None
    assert stub.monitors is None
    assert stub.credentials == []


def test_cleanup_skips_no_monitor_set_and_swallows_destroy_errors():
    """`NoMonitorSet` has no instances to terminate; `destroy()` failures must not propagate."""
    stub = _make_tester_stub(_make_tester_params())
    stub.monitors = NoMonitorSet()
    broken = MagicMock()
    broken.destroy.side_effect = RuntimeError("boom")
    stub.db_cluster = broken

    ClusterTester._cleanup_legacy_partial_aws_clusters(stub)

    broken.destroy.assert_called_once()
    assert stub.db_cluster is None
    assert isinstance(stub.monitors, NoMonitorSet)


@pytest.mark.parametrize(
    ("first_exc", "expected_calls", "expected_cleanup_calls", "expected_final_az"),
    [
        pytest.param(None, 1, 0, "a", id="success_first_attempt"),
        pytest.param(_capacity_error(), 2, 1, "b", id="capacity_error_then_retry"),
        pytest.param(ProvisioningCapacityExhausted("spot empty"), 2, 1, "b", id="spot_exhaustion_then_retry"),
    ],
)
def test_provision_legacy_with_az_fallback_succeeds(
    first_exc, expected_calls, expected_cleanup_calls, expected_final_az
):
    """Both on-demand capacity errors (ClientError) and spot exhaustion (ProvisioningCapacityExhausted)
    trigger AZ retry; clean first attempts skip cleanup entirely."""
    stub = _make_tester_stub(_make_tester_params(availability_zone="a"))
    calls = [first_exc, None]
    stub._provision_legacy_aws_clusters = MagicMock(side_effect=lambda *_a, **_k: _raise_or_none(calls))
    stub._cleanup_legacy_partial_aws_clusters = MagicMock()

    region_exhausted, _ = ClusterTester._provision_legacy_with_az_fallback(
        stub, loader_info={}, db_info={}, monitor_info={}, candidates=[["a"], ["b"]], last_error=None
    )

    assert region_exhausted is False
    assert stub._provision_legacy_aws_clusters.call_count == expected_calls
    assert stub._cleanup_legacy_partial_aws_clusters.call_count == expected_cleanup_calls
    assert stub.params["availability_zone"] == expected_final_az


def test_provision_legacy_with_az_fallback_returns_region_exhausted_when_all_candidates_fail():
    stub = _make_tester_stub(_make_tester_params(availability_zone="a"))
    error = _capacity_error()
    stub._provision_legacy_aws_clusters = MagicMock(side_effect=error)
    stub._cleanup_legacy_partial_aws_clusters = MagicMock()

    region_exhausted, last_error = ClusterTester._provision_legacy_with_az_fallback(
        stub, loader_info={}, db_info={}, monitor_info={}, candidates=[["a"], ["b"]], last_error=None
    )

    assert region_exhausted is True
    assert last_error is error
    assert stub._provision_legacy_aws_clusters.call_count == 2


def test_provision_legacy_with_az_fallback_non_capacity_error_propagates():
    stub = _make_tester_stub(_make_tester_params(availability_zone="a"))
    other_error = ClientError({"Error": {"Code": "AuthFailure", "Message": "denied"}}, "RunInstances")
    stub._provision_legacy_aws_clusters = MagicMock(side_effect=other_error)
    stub._cleanup_legacy_partial_aws_clusters = MagicMock()

    with pytest.raises(ClientError):
        ClusterTester._provision_legacy_with_az_fallback(
            stub, loader_info={}, db_info={}, monitor_info={}, candidates=[["a"], ["b"]], last_error=None
        )
    assert stub._provision_legacy_aws_clusters.call_count == 1


class _RegionParams(_DotDict):
    """Params stub that derives `region_names` from `region_name` like `SCTConfiguration`."""

    @property
    def region_names(self):
        return (self.get("region_name") or "").split()

    def resolve_amis(self, region_names, source_region=None):
        calls = self.setdefault("resolve_amis_calls", [])
        calls.append((list(region_names), source_region))


def _make_region_params(**overrides):
    return _RegionParams(
        {
            "cluster_backend": "aws",
            "region_name": "us-east-1",
            "availability_zone": "a",
            "instance_type_db": "i4i.large",
            "instance_type_loader": "t3.small",
            "instance_type_monitor": "t3.small",
            "fallback_to_next_region": True,
            "fallback_to_next_availability_zone": False,
            "use_capacity_reservation": False,
            "instance_provision": "on_demand",
            "test_id": "test-id-123",
            "n_db_nodes": 1,
            **overrides,
        }
    )


@pytest.fixture(name="restore_region_env")
def restore_region_env_fixture():
    saved = {k: os.environ.get(k) for k in ("SCT_REGION_NAME", "SCT_AVAILABILITY_ZONE")}

    yield

    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_region_fallback_relocates_on_capacity_exhaustion(patched_capacity_reservation, restore_region_env):  # noqa: ARG001
    params = _make_region_params()
    layout = SCTProvisionAWSLayout(params)
    seen_regions = []

    def fake_do_provision():
        seen_regions.append(params["region_name"])
        if len(seen_regions) == 1:
            raise _capacity_error()

    with (
        patch.object(AZResolver, "resolve"),
        patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]),
        patch.object(layout, "_do_provision", side_effect=fake_do_provision),
        patch.object(layout, "_cleanup_region") as mock_cleanup,
    ):
        layout.provision()

    assert seen_regions == ["us-east-1", "eu-west-1"]
    assert params["region_name"] == "eu-west-1"
    assert os.environ["SCT_REGION_NAME"] == "eu-west-1"
    assert params.get("resolve_amis_calls") == [(["eu-west-1"], "us-east-1")]
    mock_cleanup.assert_called_once()


def test_region_fallback_non_capacity_error_propagates_and_restores(patched_capacity_reservation, restore_region_env):  # noqa: ARG001
    params = _make_region_params()
    layout = SCTProvisionAWSLayout(params)
    auth_error = ClientError({"Error": {"Code": "AuthFailure", "Message": "denied"}}, "RunInstances")

    with (
        patch.object(AZResolver, "resolve"),
        patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]),
        patch.object(layout, "_do_provision", side_effect=auth_error),
        patch.object(layout, "_cleanup_region") as mock_cleanup,
    ):
        with pytest.raises(ClientError):
            layout.provision()

    mock_cleanup.assert_not_called()
    assert params["region_name"] == "us-east-1"  # original region restored


def test_region_fallback_gate_rejects_multi_region(restore_region_env):  # noqa: ARG001
    layout = SCTProvisionAWSLayout(_make_region_params(region_name="us-east-1 eu-west-1"))
    with pytest.raises(ValueError, match="single configured region"):
        layout.provision()


def test_region_fallback_all_regions_exhausted_raises_and_restores(patched_capacity_reservation, restore_region_env):  # noqa: ARG001
    params = _make_region_params()
    layout = SCTProvisionAWSLayout(params)

    with (
        patch.object(AZResolver, "resolve"),
        patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]),
        patch.object(layout, "_do_provision", side_effect=_capacity_error()),
        patch.object(layout, "_cleanup_region"),
    ):
        with pytest.raises(RuntimeError, match="all fallback candidates"):
            layout.provision()

    assert params["region_name"] == "us-east-1"
    assert os.environ.get("SCT_REGION_NAME") is None


def test_region_fallback_capacity_reservation_exhaustion_relocates(patched_capacity_reservation, restore_region_env):  # noqa: ARG001
    patched_capacity_reservation.is_capacity_reservation_enabled.return_value = True
    params = _make_region_params(use_capacity_reservation=True)
    layout = SCTProvisionAWSLayout(params)
    seen_regions = []

    def fake_do_provision():
        seen_regions.append(params["region_name"])
        if len(seen_regions) == 1:
            raise CapacityReservationError("no CR capacity in any AZ")

    with (
        patch.object(AZResolver, "resolve"),
        patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]),
        patch.object(layout, "_do_provision", side_effect=fake_do_provision),
        patch.object(layout, "_cleanup_region"),
    ):
        layout.provision()

    assert seen_regions == ["us-east-1", "eu-west-1"]
    assert params["region_name"] == "eu-west-1"


def test_region_fallback_skips_region_without_equivalent_ami(patched_capacity_reservation, restore_region_env):  # noqa: ARG001
    params = _make_region_params()
    layout = SCTProvisionAWSLayout(params)

    def raise_no_ami(region_names, source_region=None):  # noqa: ARG001
        raise RegionAMINotFoundError("no equivalent AMI in eu-west-1")

    params.resolve_amis = raise_no_ami
    with (
        patch.object(AZResolver, "resolve"),
        patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]),
        patch.object(layout, "_do_provision", side_effect=_capacity_error()),
        patch.object(layout, "_cleanup_region"),
    ):
        with pytest.raises(RuntimeError, match="all fallback candidates"):
            layout.provision()
    assert params["region_name"] == "us-east-1"


def _make_region_tester_stub(**param_overrides):
    params = _RegionParams(
        {
            "region_name": "us-east-1",
            "availability_zone": "a",
            "fallback_to_next_region": True,
            "cluster_backend": "aws",
            "test_id": "t-1",
            **param_overrides,
        }
    )
    stub = SimpleNamespace(params=params, log=MagicMock(), test_config=MagicMock())

    def bind(method):
        return lambda *args, **kwargs: method(stub, *args, **kwargs)

    stub._enforce_single_region_gate = bind(ClusterTester._enforce_single_region_gate)
    stub._switch_region_legacy = bind(ClusterTester._switch_region_legacy)
    stub._restore_region_legacy = bind(ClusterTester._restore_region_legacy)
    stub._cleanup_region_legacy = MagicMock()
    return stub


def test_legacy_region_fallback_relocates_on_exhaustion(restore_region_env):  # noqa: ARG001
    stub = _make_region_tester_stub()
    seen_regions = []

    def fake_attempt(loader_info, db_info, monitor_info):  # noqa: ARG001
        seen_regions.append(stub.params["region_name"])
        if len(seen_regions) == 1:
            return True, _capacity_error(), [["a"]]
        return False, None, [["a"]]

    stub._attempt_region_legacy = fake_attempt
    with patch.object(AZResolver, "get_region_fallback_candidates", return_value=[("eu-west-1", ["a"])]):
        ClusterTester._get_cluster_aws_with_region_fallback(stub, {}, {}, {})

    assert seen_regions == ["us-east-1", "eu-west-1"]
    assert stub.params["region_name"] == "eu-west-1"
    assert stub.params.get("resolve_amis_calls") == [(["eu-west-1"], "us-east-1")]
    stub._cleanup_region_legacy.assert_called_once()
    stub.test_config.persist_resolved_placement_if_changed.assert_called_once()


def test_switch_region_sets_placement_resolves_amis_and_invalidates(restore_region_env):  # noqa: ARG001
    params = _DotDict({"region_name": "us-east-1", "availability_zone": "a"})
    params.region_names = ["us-east-1"]
    params.resolve_amis = MagicMock()
    invalidate = MagicMock()

    with patch("sdcm.provision.aws.region_fallback.SCTCapacityReservation") as mock_cr:
        mock_cr.reservations = {"stale": 1}
        switch_region(params, "eu-west-1", ["b", "c"], source_region="us-east-1", invalidate_caches=invalidate)
        assert mock_cr.reservations == {}

    assert os.environ["SCT_REGION_NAME"] == "eu-west-1"
    assert params["region_name"] == "eu-west-1"
    assert params["availability_zone"] == "b,c"
    params.resolve_amis.assert_called_once_with(["eu-west-1"], source_region="us-east-1")
    invalidate.assert_called_once_with()


def test_cleanup_abandoned_region_cancels_crs_and_terminates_non_runner_instances():
    region = "us-east-1"
    instances = [
        {"InstanceId": "i-runner", "Tags": [{"Key": "NodeType", "Value": "sct-runner"}]},
        {"InstanceId": "i-db", "Tags": [{"Key": "NodeType", "Value": "scylla-db"}]},
    ]
    client = MagicMock()

    with (
        patch(
            "sdcm.provision.aws.capacity_reservation.SCTCapacityReservation.cancel_all_regions"
        ) as cancel_all_regions,
        patch("sdcm.provision.aws.utils.list_instances_aws", return_value={region: instances}),
        patch.dict(aws_utils.ec2_clients, {region: client}, clear=False),
    ):
        aws_utils.cleanup_abandoned_region("t-1", region)

    cancel_all_regions.assert_called_once_with("t-1")
    client.terminate_instances.assert_called_once_with(InstanceIds=["i-db"])


def test_cleanup_abandoned_region_never_raises_on_failure():
    with (
        patch(
            "sdcm.provision.aws.capacity_reservation.SCTCapacityReservation.cancel_all_regions",
            side_effect=RuntimeError("boom"),
        ),
        patch("sdcm.provision.aws.utils.list_instances_aws", side_effect=RuntimeError("boom")),
    ):
        aws_utils.cleanup_abandoned_region("t-1", "us-east-1")  # must not raise
