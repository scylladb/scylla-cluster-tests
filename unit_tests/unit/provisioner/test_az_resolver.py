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

from unittest.mock import patch

import pytest

from sdcm.provision.aws.az_resolver import AZResolver, NoValidAvailabilityZoneError, _node_count_positive


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
            "n_db_nodes": 3,
            "n_loaders": 1,
            "n_monitor_nodes": 1,
            "pre_filter_unavailable_availability_zones": True,
            **overrides,
        }
    )
    params.region_names = params["region_name"].split()
    return params


@pytest.fixture(name="mock_aws_region_cls")
def mock_aws_region_cls_fixture():
    """Patch AwsRegion with a mock returning configurable AZ lists."""
    with patch("sdcm.provision.aws.az_resolver.AwsRegion") as mock_cls:
        instance = mock_cls.return_value
        # default: every type is offered in a, b, c, d
        instance.get_common_availability_zones.return_value = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"]
        instance.region_name = "us-east-1"
        yield mock_cls, instance


def test_required_instance_types_collects_all_role_types_when_active():
    params = _make_params(
        instance_type_db="i4i.large",
        instance_type_loader="t3.small",
        instance_type_monitor="t3.medium",
        instance_type_db_oracle="i3.large",
        instance_type_db_target="m6g.large",
        nemesis_grow_shrink_instance_type="i4i.xlarge",
        zero_token_instance_type_db="t3.micro",
        instance_type_vector_store="t4g.medium",
        n_loaders=1,
        n_monitor_nodes=1,
        n_test_oracle_db_nodes=1,
        db_type="mixed_scylla",
        n_db_zero_token_nodes=1,
        use_zero_nodes=True,
        n_vector_store_nodes=2,
    )
    types = AZResolver(params).required_instance_types()
    assert set(types) == {
        "i4i.large",
        "t3.small",
        "t3.medium",
        "i3.large",
        "m6g.large",
        "i4i.xlarge",
        "t3.micro",
        "t4g.medium",
    }


def test_required_instance_types_skips_empty_and_missing():
    params = _make_params(
        instance_type_db="i4i.large",
        instance_type_loader="",
        instance_type_monitor=None,
        # instance_type_db_oracle missing entirely
        n_loaders=0,
        n_monitor_nodes=0,
    )
    assert AZResolver(params).required_instance_types() == ["i4i.large"]


def test_required_instance_types_excludes_types_with_zero_node_count():
    """Instance types whose node count is 0 or feature flag is off must not constrain AZ filtering."""
    params = _make_params(
        instance_type_db="i4i.large",
        instance_type_loader="c6i.xlarge",  # excluded: n_loaders=0
        instance_type_monitor="t3.large",  # excluded: n_monitor_nodes=0
        instance_type_vector_store="t4g.medium",  # excluded: n_vector_store_nodes=0
        instance_type_db_oracle="i8g.2xlarge",  # excluded: db_type != mixed
        zero_token_instance_type_db="i4i.large",  # excluded: use_zero_nodes=False
        n_loaders=0,
        n_monitor_nodes=0,
        n_vector_store_nodes=0,
        n_test_oracle_db_nodes=1,
        db_type="scylla",
        use_zero_nodes=False,
    )
    assert AZResolver(params).required_instance_types() == ["i4i.large"]


def test_resolve_disabled_returns_unchanged(mock_aws_region_cls):
    params = _make_params(pre_filter_unavailable_availability_zones=False, availability_zone="a,b")
    AZResolver(params).resolve()
    assert params["availability_zone"] == "a,b"


def test_resolve_replaces_invalid_az_with_valid_alternative(mock_aws_region_cls):
    _, region_instance = mock_aws_region_cls
    # configured "a" not in the supported list - must be replaced
    region_instance.get_common_availability_zones.return_value = ["us-east-1b", "us-east-1c"]
    params = _make_params(availability_zone="a")
    AZResolver(params).resolve()
    assert params["availability_zone"] in {"b", "c"}


def test_resolve_multi_az_drops_unsupported_and_fills_alternatives(mock_aws_region_cls):
    _, region_instance = mock_aws_region_cls
    region_instance.get_common_availability_zones.return_value = ["us-east-1a", "us-east-1c", "us-east-1d"]
    # configured a,b,e — only "a" supported; must replace b + e with valid alternatives
    params = _make_params(availability_zone="a,b,e")
    AZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert len(result) == 3
    assert set(result) <= {"a", "c", "d"}  # only valid AZs, no duplicates implied by len check
    assert "a" in result
    assert "b" not in result
    assert "e" not in result
    assert len(set(result)) == 3


def test_resolve_raises_when_no_valid_az_in_region(mock_aws_region_cls):
    _, region_instance = mock_aws_region_cls
    region_instance.get_common_availability_zones.return_value = []
    params = _make_params(availability_zone="a")
    with pytest.raises(NoValidAvailabilityZoneError):
        AZResolver(params).resolve()


def test_resolve_target_intersection_when_target_differs_from_db(mock_aws_region_cls):
    _, region_instance = mock_aws_region_cls
    # only "c" supports both db (x86) and target (arm)
    region_instance.get_common_availability_zones.return_value = ["us-east-1c"]
    params = _make_params(
        instance_type_db="i4i.large",
        instance_type_db_target="m6g.large",
        availability_zone="a",
    )
    AZResolver(params).resolve()
    assert params["availability_zone"] == "c"


@pytest.fixture(name="mock_multi_region")
def mock_multi_region_fixture():
    """Patch AwsRegion to return configurable per-region AZ lists."""
    region_returns: dict[str, list[str]] = {}

    class _RegionStub:
        def __init__(self, region_name):
            self.region_name = region_name

        def get_common_availability_zones(self, instance_types, preferred_azs):  # noqa: ARG002
            return region_returns.get(self.region_name, [])

    with patch("sdcm.provision.aws.az_resolver.AwsRegion", _RegionStub):
        yield region_returns


def test_resolve_multi_region_intersects_supported_az_letters(mock_multi_region):
    mock_multi_region["us-east-1"] = ["us-east-1a", "us-east-1b"]
    mock_multi_region["eu-west-1"] = ["eu-west-1b", "eu-west-1c"]
    params = _make_params(region_name="us-east-1 eu-west-1", availability_zone="a")
    AZResolver(params).resolve()
    assert params["availability_zone"] == "b"


def test_resolve_multi_region_raises_when_no_common_letter(mock_multi_region):
    mock_multi_region["us-east-1"] = ["us-east-1a", "us-east-1b"]
    mock_multi_region["eu-west-1"] = ["eu-west-1c", "eu-west-1d"]
    params = _make_params(region_name="us-east-1 eu-west-1", availability_zone="a")
    with pytest.raises(NoValidAvailabilityZoneError):
        AZResolver(params).resolve()


def test_resolve_multi_region_multi_az_drops_unsupported_and_fills(mock_multi_region):
    """Multi-AZ × multi-region: letter must be supported in EVERY region; drop+fill applies."""
    mock_multi_region["us-east-1"] = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d"]
    mock_multi_region["eu-west-1"] = ["eu-west-1b", "eu-west-1c", "eu-west-1d"]
    params = _make_params(region_name="us-east-1 eu-west-1", availability_zone="a,b,c")
    AZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert set(result) == {"b", "c", "d"}


@pytest.mark.parametrize(
    "value, expected",
    [
        # scalars
        (None, False),
        (True, True),
        (False, False),
        (0, False),
        (1, True),
        (3, True),
        (-1, False),
        (0.0, False),
        (1.5, True),
        # lists
        ([], False),
        ([0, 0], False),
        ([2, 0], True),
        ([-1, 0], False),
        (["2", "0"], True),
        (["0"], False),
        # strings
        ("", False),
        ("0", False),
        ("3", True),
        ("3 4", True),
        ("0 0", False),
        ("-1", False),
        ("abc", False),
        # unsupported types
        ({"a": 1}, False),
    ],
)
def test_node_count_positive(value, expected):
    assert _node_count_positive(value) is expected


def test_required_instance_types_skips_target_when_none():
    """`instance_type_db_target` unset / None must not constrain AZ filtering."""
    params = _make_params(
        instance_type_db="i4i.large",
        instance_type_db_target=None,
        nemesis_grow_shrink_instance_type="",
        instance_type_loader="",
        n_loaders=0,
        instance_type_monitor="",
        n_monitor_nodes=0,
    )
    assert AZResolver(params).required_instance_types() == ["i4i.large"]


@pytest.mark.parametrize("az_value", ["", None])
def test_resolve_with_unset_availability_zone_stays_unset(mock_aws_region_cls, az_value):
    """Empty / None `availability_zone` means no explicit user choice; resolver leaves it effectively unset."""
    _, region_instance = mock_aws_region_cls
    region_instance.get_common_availability_zones.return_value = ["us-east-1b", "us-east-1c"]
    params = _make_params(availability_zone=az_value)
    AZResolver(params).resolve()
    assert not params["availability_zone"]
