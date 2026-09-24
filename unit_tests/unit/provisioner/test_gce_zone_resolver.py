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

from sdcm.provision.gce.capacity_advisor import ZoneCapacityAdvice
from sdcm.provision.gce.zone_resolver import GceAZResolver, NoValidAvailabilityZoneError, _node_count_positive
from sdcm.utils.gce_utils import get_alternative_zones

from unit_tests.lib.dot_dict import DotDict


def _make_params(**overrides):
    params = DotDict(
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
        gce_instance_type_db_oracle="n2-highmem-16",
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
        gce_instance_type_db_oracle="n2-highmem-16",
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


def test_resolve_raises_when_regions_cannot_supply_the_configured_zone_count(mock_multi_region):
    """'b,c,d' asks for three racks; two zones would silently build a two-rack cluster."""
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c", "us-east1-d"]
    mock_multi_region["us-west1"] = ["us-west1-b", "us-west1-c"]
    params = _make_params(gce_datacenter="us-east1 us-west1", availability_zone="b,c,d")

    with pytest.raises(NoValidAvailabilityZoneError, match="requests 3 zone.*only 2"):
        GceAZResolver(params).resolve()

    # the caller must see the value it configured, not a silently narrowed one
    assert params["availability_zone"] == "b,c,d"


def test_resolve_raises_when_a_single_region_cannot_supply_the_configured_zone_count(mock_gce_zone_resolver):
    """The shortfall is about the count, not about how many regions are configured."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c"]
    params = _make_params(availability_zone="b,c,d")

    with pytest.raises(NoValidAvailabilityZoneError, match="requests 3 zone.*only 2"):
        GceAZResolver(params).resolve()


def test_resolve_does_not_raise_when_the_zone_count_is_preserved(mock_gce_zone_resolver):
    """Substituting an unsupported zone is fine - only a shortfall in the count is fatal."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-c", "us-east1-d"]
    params = _make_params(availability_zone="b,c")

    GceAZResolver(params).resolve()

    assert sorted(params["availability_zone"].split(",")) == ["c", "d"]


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


class TestDefaultZoneIsNotAChoice:
    """Same rule as AWS: a zone nobody chose must not outrank the capacity signal.

    GCE has no zone pinned in `defaults/gce_config.yaml` today, so this mostly guards the rule from
    drifting apart between the two resolvers - the AWS one has the identical logic, and a divergence
    would mean the same config means different things per backend.
    """

    @staticmethod
    def _params(explicit: bool, **overrides):
        defaults = {"instance_provision": "spot", "use_spot_placement_scores": True, "availability_zone": "b"}
        params = _make_params(**{**defaults, **overrides})
        params.is_explicitly_set = lambda name: explicit and name == "availability_zone"
        return params

    def test_a_defaults_only_zone_lets_the_advice_choose(self):
        assert GceAZResolver(self._params(explicit=False))._score_may_choose_az() is True

    def test_a_deliberate_pin_is_honoured(self):
        assert GceAZResolver(self._params(explicit=True))._score_may_choose_az() is False

    def test_the_override_flag_still_beats_a_deliberate_pin(self):
        params = self._params(explicit=True, spot_score_overrides_configured_az=True)
        assert GceAZResolver(params)._score_may_choose_az() is True

    def test_on_demand_runs_never_reorder_regardless_of_provenance(self):
        assert (
            GceAZResolver(self._params(explicit=False, instance_provision="on_demand"))._score_may_choose_az() is False
        )

    def test_params_without_provenance_keep_todays_behaviour(self):
        params = _make_params(instance_provision="spot", use_spot_placement_scores=True, availability_zone="b")
        assert GceAZResolver(params)._score_may_choose_az() is False


class TestRegionFallbackOrdering:
    """Region-fallback candidates are obtainability-ordered, once for the whole list.

    Ranking costs one advice call per zone of each candidate region, so it happens once here rather than
    per candidate as the consumer walks the generator lazily.
    """

    def test_candidates_are_ordered_by_capacity_advice(self):
        params = _make_params(instance_provision="spot", use_spot_placement_scores=True)
        with (
            patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-east4", "us-west1"]),
            patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as zone_resolver,
            patch("sdcm.provision.gce.zone_resolver.rank_regions", return_value=["us-west1", "us-east1"]) as rank,
        ):
            zone_resolver.return_value.get_zones_for_region.return_value = ["us-east1-b"]
            ordered = GceAZResolver(params)._ordered_fallback_regions(exclude={"us-east4"})

        assert ordered == ["us-west1", "us-east1"]
        assert rank.call_args.kwargs["regions"] == ["us-east1", "us-west1"], "the excluded region is never scored"

    def test_unavailable_advice_keeps_the_configured_order(self):
        params = _make_params(instance_provision="spot", use_spot_placement_scores=True)
        with (
            patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-west1"]),
            patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as zone_resolver,
            patch("sdcm.provision.gce.zone_resolver.rank_regions", return_value=None),
        ):
            zone_resolver.return_value.get_zones_for_region.return_value = ["us-east1-b"]
            assert GceAZResolver(params)._ordered_fallback_regions(exclude=set()) == ["us-east1", "us-west1"]

    def test_on_demand_runs_never_call_the_advice_api(self):
        params = _make_params(instance_provision="on_demand", use_spot_placement_scores=True)
        with (
            patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-west1"]),
            patch("sdcm.provision.gce.zone_resolver.rank_regions") as rank,
        ):
            assert GceAZResolver(params)._ordered_fallback_regions(exclude=set()) == ["us-east1", "us-west1"]
        rank.assert_not_called()


class TestCapacityAdviceZoneRanking:
    """The resolver method that wires Capacity Advisor into zone selection.

    The AWS counterpart has this covered; the GCE one did not, which left the `gce_spot_obtainability_min`
    guard untested - the guard whose whole job is to refuse a cluster with fewer zones than the test asked
    for, rather than quietly building one. On GCE the zone count is also the rack count.
    """

    @staticmethod
    def _params(**overrides):
        defaults = {
            "instance_provision": "spot",
            "use_spot_placement_scores": True,
            "availability_zone": "b,c",
        }
        params = _make_params(**{**defaults, **overrides})
        params.is_explicitly_set = lambda _: False
        return params

    def test_zones_are_reordered_best_first(self):
        resolver = GceAZResolver(self._params())
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "b", "c"]):
            assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"]) == ["d", "b", "c"]

    def test_unavailable_advice_returns_the_input_unchanged(self):
        """Callers compare by identity to tell "not ranked" from "ranked to the same order"."""
        resolver = GceAZResolver(self._params())
        letters = ["b", "c", "d"]
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=None):
            assert resolver._rank_letters_by_capacity_advice(["us-east1"], letters) is letters

    def test_multi_region_configs_are_not_ranked(self):
        """A zone letter must be valid in every region, and a letter good in one may be poor in another."""
        resolver = GceAZResolver(self._params())
        letters = ["b", "c"]
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
            assert resolver._rank_letters_by_capacity_advice(["us-east1", "us-west1"], letters) is letters
        rank.assert_not_called()

    def test_a_threshold_leaving_too_few_zones_fails_loudly(self):
        """Silently returning one zone would build a 1-rack cluster for a 2-rack test."""
        resolver = GceAZResolver(self._params(gce_spot_obtainability_min=0.5))
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d"]):
            with pytest.raises(NoValidAvailabilityZoneError, match="gce_spot_obtainability_min=0.5"):
                resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"])

    def test_a_threshold_keeping_enough_zones_is_fine(self):
        resolver = GceAZResolver(self._params(gce_spot_obtainability_min=0.5))
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "b"]):
            assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"]) == ["d", "b"]

    def test_on_demand_runs_never_call_the_advice_api(self):
        resolver = GceAZResolver(self._params(instance_provision="on_demand"))
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
            resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c"])
        rank.assert_not_called()

    def test_a_single_candidate_is_not_worth_an_api_call(self):
        resolver = GceAZResolver(self._params())
        with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
            assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b"]) == ["b"]
        rank.assert_not_called()


class TestRolesNeverSplitAcrossZones:
    """Per-role advice must not turn into per-role placement.

    Each role is asked separately and their preference orders routinely disagree - on a real run the DB type
    scored 0.90 in every `us-east1` zone while the loader type scored 0.10 in two of them. What must never
    follow is a cluster with the DB nodes in one zone and the loaders in another: the roles collapse to one
    ranked list before anything is chosen, and the configured zone count decides how many come out. On GCE
    that count is also `racks_count`.
    """

    @staticmethod
    def _params(availability_zone):
        return _make_params(
            instance_provision="spot",
            use_spot_placement_scores=True,
            availability_zone=availability_zone,
            spot_score_overrides_configured_az=True,
        )

    @staticmethod
    def _conflicting_advice():
        """db prefers b, loaders prefer c, monitor prefers d."""
        return [
            [
                ZoneCapacityAdvice(zone="us-east1-b", obtainability=0.9),
                ZoneCapacityAdvice(zone="us-east1-c", obtainability=0.2),
                ZoneCapacityAdvice(zone="us-east1-d", obtainability=0.2),
            ],
            [
                ZoneCapacityAdvice(zone="us-east1-b", obtainability=0.2),
                ZoneCapacityAdvice(zone="us-east1-c", obtainability=0.9),
                ZoneCapacityAdvice(zone="us-east1-d", obtainability=0.3),
            ],
            [
                ZoneCapacityAdvice(zone="us-east1-b", obtainability=0.2),
                ZoneCapacityAdvice(zone="us-east1-c", obtainability=0.3),
                ZoneCapacityAdvice(zone="us-east1-d", obtainability=0.9),
            ],
        ]

    def test_one_configured_zone_resolves_to_exactly_one(self):
        params = self._params("b")
        with (
            patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
            patch("sdcm.provision.gce.capacity_advisor.get_zone_advice", side_effect=self._conflicting_advice()),
        ):
            GceAZResolver(params).resolve()

        resolved = params["availability_zone"]
        assert "," not in resolved, f"roles were split across zones: {resolved!r}"
        assert len(resolved) == 1

    def test_three_configured_zones_resolve_to_exactly_three(self):
        """`availability_zone: 'b,c,d'` asks for three racks; advice may reorder them, never drop one."""
        params = self._params("b,c,d")
        with (
            patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
            patch("sdcm.provision.gce.capacity_advisor.get_zone_advice", side_effect=self._conflicting_advice()),
        ):
            GceAZResolver(params).resolve()

        assert sorted(params["availability_zone"].split(",")) == ["b", "c", "d"]

    def test_fallback_candidates_keep_the_configured_cardinality(self):
        """A single-zone config must stay single-zone through every fallback candidate too."""
        params = self._params("b")
        with (
            patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
            patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "c", "b"]),
            patch("sdcm.provision.gce.zone_resolver.GceZoneResolver"),
        ):
            candidates = list(GceAZResolver(params).get_az_fallback_candidates())

        assert candidates, "expected at least one fallback candidate"
        assert all(len(candidate) == 1 for candidate in candidates), f"cardinality changed: {candidates}"
