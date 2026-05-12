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

from sdcm.provision.gce.zone_resolver import GceAZResolver, NoValidAvailabilityZoneError, _node_count_positive
from sdcm.utils.gce_utils import get_alternative_zones


class _DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _make_params(**overrides):
    params = _DotDict(
        {
            "cluster_backend": "gce",
            "gce_datacenter": "us-east1",
            "availability_zone": "b",
            "gce_instance_type_db": "n2-highmem-8",
            "gce_instance_type_loader": "e2-standard-2",
            "gce_instance_type_monitor": "n2-highmem-8",
            "n_db_nodes": 3,
            "n_loaders": 1,
            "n_monitor_nodes": 1,
            "pre_filter_unavailable_availability_zones": True,
            **overrides,
        }
    )
    params.gce_datacenters = (
        params["gce_datacenter"].split() if isinstance(params["gce_datacenter"], str) else params["gce_datacenter"]
    )
    return params


@pytest.fixture(name="mock_gce_zone_resolver")
def mock_gce_zone_resolver_fixture():
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        instance = mock_cls.return_value
        instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
        yield mock_cls, instance


def test_required_machine_types_collects_all_role_types_when_active():
    params = _make_params(
        gce_instance_type_db="n2-highmem-8",
        gce_instance_type_loader="e2-standard-2",
        gce_instance_type_monitor="n2-highmem-4",
        instance_type_db_oracle="n2-highmem-16",
        instance_type_db_target="n2d-standard-8",
        nemesis_grow_shrink_instance_type="n2-highmem-16",
        zero_token_instance_type_db="e2-medium",
        instance_type_vector_store="e2-medium",
        n_loaders=1,
        n_monitor_nodes=1,
        n_test_oracle_db_nodes=1,
        db_type="mixed_scylla",
        n_db_zero_token_nodes=1,
        use_zero_nodes=True,
        n_vector_store_nodes=2,
    )
    types = GceAZResolver(params).required_machine_types()
    assert set(types) == {
        "n2-highmem-8",
        "e2-standard-2",
        "n2-highmem-4",
        "n2-highmem-16",
        "n2d-standard-8",
        "e2-medium",
    }


def test_required_machine_types_excludes_types_with_zero_node_count():
    params = _make_params(
        gce_instance_type_db="n2-highmem-8",
        gce_instance_type_loader="e2-standard-2",
        gce_instance_type_monitor="n2-highmem-8",
        instance_type_vector_store="e2-medium",
        instance_type_db_oracle="n2-highmem-16",
        zero_token_instance_type_db="e2-medium",
        n_loaders=0,
        n_monitor_nodes=0,
        n_vector_store_nodes=0,
        n_test_oracle_db_nodes=1,
        db_type="scylla",
        use_zero_nodes=False,
    )
    assert GceAZResolver(params).required_machine_types() == ["n2-highmem-8"]


def test_resolve_disabled_returns_unchanged(mock_gce_zone_resolver):
    params = _make_params(pre_filter_unavailable_availability_zones=False, availability_zone="b,c")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] == "b,c"


def test_resolve_replaces_invalid_zone_with_valid_alternative(mock_gce_zone_resolver):
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-c", "us-east1-d"]
    params = _make_params(availability_zone="b")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] in {"c", "d"}


def test_resolve_multi_az_drops_unsupported_and_fills_alternatives(mock_gce_zone_resolver):
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
    params = _make_params(availability_zone="b,f,e")
    GceAZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert len(result) == 3
    assert "b" in result
    assert "f" not in result
    assert "e" not in result
    assert set(result) <= {"b", "c", "d"}


def test_resolve_raises_when_no_valid_zone_in_region(mock_gce_zone_resolver):
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = []
    params = _make_params(availability_zone="b")
    with pytest.raises(NoValidAvailabilityZoneError):
        GceAZResolver(params).resolve()


@pytest.fixture(name="mock_multi_region")
def mock_multi_region_fixture():
    region_returns: dict[str, list[str]] = {}

    class _ResolverStub:
        def get_common_zones(self, region, machine_types, preferred_zones=None):  # noqa: ARG002
            return region_returns.get(region, [])

        def get_per_type_zones(self, region, machine_types):  # noqa: ARG002
            zones = region_returns.get(region, [])
            return {mt: zones for mt in machine_types}

    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver", return_value=_ResolverStub()):
        yield region_returns


def test_resolve_multi_region_intersects_supported_zone_letters(mock_multi_region):
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c"]
    mock_multi_region["us-east4"] = ["us-east4-c", "us-east4-a"]
    params = _make_params(gce_datacenter="us-east1 us-east4", availability_zone="b")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] == "c"


def test_resolve_multi_region_raises_when_no_common_letter(mock_multi_region):
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c"]
    mock_multi_region["us-east4"] = ["us-east4-a", "us-east4-d"]
    params = _make_params(gce_datacenter="us-east1 us-east4", availability_zone="b")
    with pytest.raises(NoValidAvailabilityZoneError):
        GceAZResolver(params).resolve()


def test_resolve_multi_region_multi_az_drops_unsupported_and_fills(mock_multi_region):
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c", "us-east1-d"]
    mock_multi_region["us-east4"] = ["us-east4-b", "us-east4-c", "us-east4-d"]
    params = _make_params(gce_datacenter="us-east1 us-east4", availability_zone="a,b,c")
    GceAZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert set(result) == {"b", "c", "d"}


@pytest.fixture(name="mock_discovery_resolver")
def mock_discovery_resolver_fixture():
    class _DiscoveryStub:
        def get_zones_for_region(self, region):
            return [f"{region}-b", f"{region}-c", f"{region}-d"]

        def get_common_zones(self, region, machine_types, preferred_zones=None):  # noqa: ARG002
            return [f"{region}-b", f"{region}-c"]

    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver", return_value=_DiscoveryStub()):
        yield


@pytest.mark.parametrize("az_value", ["", None])
def test_resolve_with_unset_availability_zone_discovers_valid_zone(mock_discovery_resolver, az_value):
    params = _make_params(availability_zone=az_value)
    GceAZResolver(params).resolve()
    assert params["availability_zone"] in {"b", "c"}


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, False),
        (True, True),
        (False, False),
        (0, False),
        (1, True),
        (3, True),
        ([], False),
        ([0, 0], False),
        ([2, 0], True),
        ("", False),
        ("0", False),
        ("3", True),
        ("3 4", True),
        ({"a": 1}, False),
    ],
)
def test_node_count_positive(value, expected):
    assert _node_count_positive(value) is expected


def test_required_machine_types_skips_target_when_none():
    params = _make_params(
        gce_instance_type_db="n2-highmem-8",
        instance_type_db_target=None,
        nemesis_grow_shrink_instance_type="",
        gce_instance_type_loader="",
        n_loaders=0,
        gce_instance_type_monitor="",
        n_monitor_nodes=0,
    )
    assert GceAZResolver(params).required_machine_types() == ["n2-highmem-8"]


@pytest.fixture(name="mock_zone_letters")
def mock_zone_letters_fixture():
    """Mock _get_zone_letters_for_region so tests don't call GCE API."""
    zone_map = {
        "us-east1": ["b", "c", "d"],
        "us-east4": ["a", "b", "c"],
        "us-west1": ["a", "b", "c"],
        "us-central1": ["a", "b", "c", "f"],
    }
    with patch(
        "sdcm.utils.gce_utils._get_zone_letters_for_region",
        side_effect=lambda region: zone_map.get(region, []),
    ):
        yield


def test_get_alternative_zones_excludes_exhausted_zone_letter(mock_zone_letters):
    alternatives = get_alternative_zones("us-east1", "b")
    assert "b" not in alternatives


def test_get_alternative_zones_excludes_exhausted_full_zone_name(mock_zone_letters):
    alternatives = get_alternative_zones("us-east1", "us-east1-b")
    assert "b" not in alternatives


def test_get_alternative_zones_returns_remaining_zone_letters(mock_zone_letters):
    alternatives = get_alternative_zones("us-east1", "c")
    assert "d" in alternatives


def test_get_alternative_zones_returns_empty_for_unknown_region(mock_zone_letters):
    assert get_alternative_zones("unknown-region-1", "a") == []


def test_get_alternative_zones_returns_deterministic_order(mock_zone_letters):
    """Alternative zones are returned in deterministic order for predictable fallback."""
    results = [tuple(get_alternative_zones("us-central1", "a")) for _ in range(20)]
    assert len(set(results)) == 1
    assert results[0] == ("b", "c", "f")


def test_get_alternative_zones_single_zone_region_returns_empty():
    with patch("sdcm.utils.gce_utils._get_zone_letters_for_region", return_value=["a"]):
        assert get_alternative_zones("single-region-1", "a") == []
