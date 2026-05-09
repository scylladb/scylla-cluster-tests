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

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from sdcm.cluster_aws import AWSCluster
from sdcm.provision.aws.capacity_errors import ProvisioningCapacityExhausted
from sdcm.sct_provision.aws.layout import SCTProvisionAWSLayout


class _DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _make_params(**overrides):
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


def _capacity_error() -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": "InsufficientInstanceCapacity", "Message": "Insufficient capacity."}},
        operation_name="RunInstances",
    )


def test_provision_retries_on_capacity_error(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """Core fallback flow: capacity error -> cleanup -> cache clear -> retry next AZ -> success."""
    params = _make_params()
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
    layout = SCTProvisionAWSLayout(_make_params())
    other_error = ClientError({"Error": {"Code": "AuthFailure", "Message": "Not authorized"}}, "RunInstances")
    with patch.object(layout, "_do_provision", side_effect=other_error):
        with patch.object(layout, "_cleanup_partial_provision") as mock_cleanup:
            with pytest.raises(ClientError):
                layout.provision()
            mock_cleanup.assert_not_called()


def test_provision_retries_on_spot_capacity_exhausted(patched_aws_region, patched_capacity_reservation):  # noqa: ARG001
    """Spot path raises ProvisioningCapacityExhausted (no ClientError) when capacity is unavailable;
    AZ fallback must treat it the same as an on-demand capacity ClientError."""
    params = _make_params(instance_provision="spot")
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
    layout = SCTProvisionAWSLayout(_make_params(use_capacity_reservation=True, instance_provision="on_demand"))
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
    layout = SCTProvisionAWSLayout(_make_params(region_name="us-east-1 eu-west-1"))
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
