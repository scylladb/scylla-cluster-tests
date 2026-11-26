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


def make_params(**overrides):
    """Return a minimal GCE SCT config as DotDict, with ``overrides`` applied on top."""
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


def default_zone_params(explicit, **overrides):
    """Spot params whose availability_zone is explicitly pinned when ``explicit`` is true."""
    defaults = {"instance_provision": "spot", "use_spot_placement_scores": True, "availability_zone": "b"}
    params = make_params(**{**defaults, **overrides})
    params.is_explicitly_set = lambda name: explicit and name == "availability_zone"
    return params


def capacity_params(**overrides):
    """Spot params with an unpinned two-zone availability_zone, for the capacity advice ranking."""
    defaults = {
        "instance_provision": "spot",
        "use_spot_placement_scores": True,
        "availability_zone": "b,c",
    }
    params = make_params(**{**defaults, **overrides})
    params.is_explicitly_set = lambda _: False
    return params


def roles_params(availability_zone):
    """Spot params with ``availability_zone`` and the advice-overrides-pin flag set."""
    return make_params(
        instance_provision="spot",
        use_spot_placement_scores=True,
        availability_zone=availability_zone,
        spot_score_overrides_configured_az=True,
    )


def conflicting_advice():
    """Per-role advice where db, loaders and monitor each prefer a different zone."""
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


@pytest.fixture
def mock_gce_zone_resolver():
    """GceZoneResolver patched out; get_common_zones returns us-east1 zones b, c and d."""
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        instance = mock_cls.return_value
        instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
        yield mock_cls, instance


@pytest.fixture
def mock_multi_region():
    """GceZoneResolver patched with a stub whose zones per region come from a dict the test fills."""
    region_returns: dict[str, list[str]] = {}

    class ResolverStub:
        """Resolver stub answering zones per region from ``region_returns``."""

        def get_common_zones(self, region, machine_types, preferred_zones=None):  # noqa: ARG002
            """Zones of ``region`` from the shared dict."""
            return region_returns.get(region, [])

        def get_per_type_zones(self, region, machine_types):  # noqa: ARG002
            """Zones of ``region`` mapped to every machine type."""
            zones = region_returns.get(region, [])
            return {mt: zones for mt in machine_types}

    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver", return_value=ResolverStub()):
        yield region_returns


@pytest.fixture
def mock_discovery_resolver():
    """GceZoneResolver patched with a stub that discovers zones b, c and d in any region."""

    class DiscoveryStub:
        """Resolver stub whose zones follow the region name."""

        def get_zones_for_region(self, region):
            """Zones b, c and d of ``region``."""
            return [f"{region}-b", f"{region}-c", f"{region}-d"]

        def get_common_zones(self, region, machine_types, preferred_zones=None):  # noqa: ARG002
            """Zones b and c of ``region``."""
            return [f"{region}-b", f"{region}-c"]

    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver", return_value=DiscoveryStub()):
        yield


@pytest.fixture
def mock_zone_letters():
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


def test_required_machine_types_collects_all_role_types_when_active():
    """required_machine_types lists the instance type of every active role, deduplicated."""
    params = make_params(
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
    """A role with zero nodes contributes no machine type."""
    params = make_params(
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
    """With pre-filtering disabled, resolve leaves availability_zone as configured."""
    params = make_params(pre_filter_unavailable_availability_zones=False, availability_zone="b,c")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] == "b,c"


def test_resolve_replaces_invalid_zone_with_valid_alternative(mock_gce_zone_resolver):
    """A zone missing from the region is replaced with one that the region supports."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-c", "us-east1-d"]
    params = make_params(availability_zone="b")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] in {"c", "d"}


def test_resolve_multi_az_drops_unsupported_and_fills_alternatives(mock_gce_zone_resolver):
    """Unsupported zones in a multi-AZ config are dropped and replaced, keeping the zone count."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
    params = make_params(availability_zone="b,f,e")
    GceAZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert len(result) == 3
    assert "b" in result
    assert "f" not in result
    assert "e" not in result
    assert set(result) <= {"b", "c", "d"}


def test_resolve_raises_when_no_valid_zone_in_region(mock_gce_zone_resolver):
    """Resolve raises NoValidAvailabilityZoneError when the region supports no zone."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = []
    params = make_params(availability_zone="b")
    with pytest.raises(NoValidAvailabilityZoneError):
        GceAZResolver(params).resolve()


def test_resolve_multi_region_intersects_supported_zone_letters(mock_multi_region):
    """Across regions, only a zone letter present in every region is chosen."""
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c"]
    mock_multi_region["us-east4"] = ["us-east4-c", "us-east4-a"]
    params = make_params(gce_datacenter="us-east1 us-east4", availability_zone="b")
    GceAZResolver(params).resolve()
    assert params["availability_zone"] == "c"


def test_resolve_multi_region_raises_when_no_common_letter(mock_multi_region):
    """Resolve raises NoValidAvailabilityZoneError when the regions share no zone letter."""
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c"]
    mock_multi_region["us-east4"] = ["us-east4-a", "us-east4-d"]
    params = make_params(gce_datacenter="us-east1 us-east4", availability_zone="b")
    with pytest.raises(NoValidAvailabilityZoneError):
        GceAZResolver(params).resolve()


def test_resolve_multi_region_multi_az_drops_unsupported_and_fills(mock_multi_region):
    """Multi-AZ resolution across regions keeps only letters common to all regions."""
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c", "us-east1-d"]
    mock_multi_region["us-east4"] = ["us-east4-b", "us-east4-c", "us-east4-d"]
    params = make_params(gce_datacenter="us-east1 us-east4", availability_zone="a,b,c")
    GceAZResolver(params).resolve()
    result = params["availability_zone"].split(",")
    assert set(result) == {"b", "c", "d"}


def test_resolve_raises_when_regions_cannot_supply_the_configured_zone_count(mock_multi_region):
    """'b,c,d' asks for three racks; two zones would silently build a two-rack cluster."""
    mock_multi_region["us-east1"] = ["us-east1-b", "us-east1-c", "us-east1-d"]
    mock_multi_region["us-west1"] = ["us-west1-b", "us-west1-c"]
    params = make_params(gce_datacenter="us-east1 us-west1", availability_zone="b,c,d")

    with pytest.raises(NoValidAvailabilityZoneError, match="requests 3 zone.*only 2"):
        GceAZResolver(params).resolve()

    # the caller must see the value it configured, not a silently narrowed one
    assert params["availability_zone"] == "b,c,d"


def test_resolve_raises_when_a_single_region_cannot_supply_the_configured_zone_count(mock_gce_zone_resolver):
    """The shortfall is about the count, not about how many regions are configured."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-b", "us-east1-c"]
    params = make_params(availability_zone="b,c,d")

    with pytest.raises(NoValidAvailabilityZoneError, match="requests 3 zone.*only 2"):
        GceAZResolver(params).resolve()


def test_resolve_does_not_raise_when_the_zone_count_is_preserved(mock_gce_zone_resolver):
    """Substituting an unsupported zone is fine - only a shortfall in the count is fatal."""
    _, resolver_instance = mock_gce_zone_resolver
    resolver_instance.get_common_zones.return_value = ["us-east1-c", "us-east1-d"]
    params = make_params(availability_zone="b,c")

    GceAZResolver(params).resolve()

    assert sorted(params["availability_zone"].split(",")) == ["c", "d"]


@pytest.mark.parametrize(
    "az_value",
    [
        pytest.param("", id="empty-string"),
        pytest.param(None, id="none"),
    ],
)
def test_resolve_with_unset_availability_zone_discovers_valid_zone(mock_discovery_resolver, az_value):
    """An unset availability_zone is discovered as one of the zones the region supports."""
    params = make_params(availability_zone=az_value)
    GceAZResolver(params).resolve()
    assert params["availability_zone"] in {"b", "c"}


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, False, id="none"),
        pytest.param(True, True, id="true"),
        pytest.param(False, False, id="false"),
        pytest.param(0, False, id="zero"),
        pytest.param(1, True, id="one"),
        pytest.param(3, True, id="three"),
        pytest.param([], False, id="empty-list"),
        pytest.param([0, 0], False, id="all-zero-list"),
        pytest.param([2, 0], True, id="one-positive-list"),
        pytest.param("3", False, id="string-three"),
        pytest.param("3 4", False, id="string-pair"),
        pytest.param({"a": 1}, False, id="dict"),
    ],
)
def test_node_count_positive(value, expected):
    """_node_count_positive is true only for a positive int or a list holding one."""
    assert _node_count_positive(value) is expected


def test_required_machine_types_skips_target_when_none():
    """A None target type and empty or zero-count roles contribute no machine type."""
    params = make_params(
        gce_instance_type_db="n2-highmem-8",
        instance_type_db_target=None,
        nemesis_grow_shrink_instance_type="",
        gce_instance_type_loader="",
        n_loaders=0,
        gce_instance_type_monitor="",
        n_monitor_nodes=0,
    )
    assert GceAZResolver(params).required_machine_types() == ["n2-highmem-8"]


def test_get_alternative_zones_excludes_exhausted_zone_letter(mock_zone_letters):
    """The exhausted zone letter is not among the alternatives."""
    alternatives = get_alternative_zones("us-east1", "b")
    assert "b" not in alternatives


def test_get_alternative_zones_excludes_exhausted_full_zone_name(mock_zone_letters):
    """A full zone name is accepted and its letter excluded from the alternatives."""
    alternatives = get_alternative_zones("us-east1", "us-east1-b")
    assert "b" not in alternatives


def test_get_alternative_zones_returns_remaining_zone_letters(mock_zone_letters):
    """The other zone letters of the region are returned as alternatives."""
    alternatives = get_alternative_zones("us-east1", "c")
    assert "d" in alternatives


def test_get_alternative_zones_returns_empty_for_unknown_region(mock_zone_letters):
    """An unknown region has no alternatives."""
    assert get_alternative_zones("unknown-region-1", "a") == []


def test_get_alternative_zones_returns_deterministic_order(mock_zone_letters):
    """Alternative zones are returned in deterministic order for predictable fallback."""
    results = [tuple(get_alternative_zones("us-central1", "a")) for _ in range(20)]
    assert len(set(results)) == 1
    assert results[0] == ("b", "c", "f")


def test_get_alternative_zones_single_zone_region_returns_empty():
    """A region with a single zone letter has no alternatives."""
    with patch("sdcm.utils.gce_utils._get_zone_letters_for_region", return_value=["a"]):
        assert get_alternative_zones("single-region-1", "a") == []


# Same rule as AWS: a zone nobody chose must not outrank the capacity signal.
# GCE has no zone pinned in `defaults/gce_config.yaml` today, so this mostly guards the rule from
# drifting apart between the two resolvers - the AWS one has the identical logic, and a divergence
# would mean the same config means different things per backend.


def test_a_defaults_only_zone_lets_the_advice_choose():
    """A zone nobody chose (defaults only) lets the capacity advice choose it."""
    assert GceAZResolver(default_zone_params(explicit=False))._score_may_choose_az() is True


def test_a_deliberate_pin_is_honoured():
    """A zone explicitly pinned by the user is not reordered by the capacity advice."""
    assert GceAZResolver(default_zone_params(explicit=True))._score_may_choose_az() is False


def test_the_override_flag_still_beats_a_deliberate_pin():
    """spot_score_overrides_configured_az lets the advice choose even over an explicit pin."""
    params = default_zone_params(explicit=True, spot_score_overrides_configured_az=True)
    assert GceAZResolver(params)._score_may_choose_az() is True


def test_on_demand_runs_never_reorder_regardless_of_provenance():
    """On-demand runs never let the capacity advice choose the zone."""
    assert (
        GceAZResolver(default_zone_params(explicit=False, instance_provision="on_demand"))._score_may_choose_az()
        is False
    )


def test_params_without_provenance_keep_todays_behaviour():
    """Params without provenance info keep the pinned behaviour: the advice does not choose."""
    params = make_params(instance_provision="spot", use_spot_placement_scores=True, availability_zone="b")
    assert GceAZResolver(params)._score_may_choose_az() is False


# Region-fallback candidates are obtainability-ordered, once for the whole list.
# Ranking costs one advice call per zone of each candidate region, so it happens once here rather than
# per candidate as the consumer walks the generator lazily.


def test_candidates_are_ordered_by_capacity_advice():
    """Fallback regions are ordered by capacity advice, and the excluded region is never scored."""
    params = make_params(instance_provision="spot", use_spot_placement_scores=True)
    with (
        patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-east4", "us-west1"]),
        patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as zone_resolver,
        patch("sdcm.provision.gce.zone_resolver.rank_regions", return_value=["us-west1", "us-east1"]) as rank,
    ):
        zone_resolver.return_value.get_zones_for_region.return_value = ["us-east1-b"]
        ordered = GceAZResolver(params)._ordered_fallback_regions(exclude={"us-east4"})

    assert ordered == ["us-west1", "us-east1"]
    assert rank.call_args.kwargs["regions"] == ["us-east1", "us-west1"], "the excluded region is never scored"


def test_unavailable_advice_keeps_the_configured_order():
    """Without capacity advice, fallback regions keep the configured order."""
    params = make_params(instance_provision="spot", use_spot_placement_scores=True)
    with (
        patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-west1"]),
        patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as zone_resolver,
        patch("sdcm.provision.gce.zone_resolver.rank_regions", return_value=None),
    ):
        zone_resolver.return_value.get_zones_for_region.return_value = ["us-east1-b"]
        assert GceAZResolver(params)._ordered_fallback_regions(exclude=set()) == ["us-east1", "us-west1"]


def test_fallback_on_demand_runs_never_call_the_advice_api():
    """On-demand fallback never calls the capacity advice API and keeps the configured order."""
    params = make_params(instance_provision="on_demand", use_spot_placement_scores=True)
    with (
        patch("sdcm.provision.gce.zone_resolver.SUPPORTED_GCE_REGIONS", ["us-east1", "us-west1"]),
        patch("sdcm.provision.gce.zone_resolver.rank_regions") as rank,
    ):
        assert GceAZResolver(params)._ordered_fallback_regions(exclude=set()) == ["us-east1", "us-west1"]
    rank.assert_not_called()


# The resolver method that wires Capacity Advisor into zone selection.
# The AWS counterpart has this covered; the GCE one did not, which left the `gce_spot_obtainability_min`
# guard untested - the guard whose whole job is to refuse a cluster with fewer zones than the test asked
# for, rather than quietly building one. On GCE the zone count is also the rack count.


def test_zones_are_reordered_best_first():
    """Configured zones are reordered by the capacity advice ranking."""
    resolver = GceAZResolver(capacity_params())
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "b", "c"]):
        assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"]) == ["d", "b", "c"]


def test_unavailable_advice_returns_the_input_unchanged():
    """Callers compare by identity to tell "not ranked" from "ranked to the same order"."""
    resolver = GceAZResolver(capacity_params())
    letters = ["b", "c", "d"]
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=None):
        assert resolver._rank_letters_by_capacity_advice(["us-east1"], letters) is letters


def test_multi_region_configs_are_not_ranked():
    """A zone letter must be valid in every region, and a letter good in one may be poor in another."""
    resolver = GceAZResolver(capacity_params())
    letters = ["b", "c"]
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
        assert resolver._rank_letters_by_capacity_advice(["us-east1", "us-west1"], letters) is letters
    rank.assert_not_called()


def test_a_threshold_leaving_too_few_zones_fails_loudly():
    """Silently returning one zone would build a 1-rack cluster for a 2-rack test."""
    resolver = GceAZResolver(capacity_params(gce_spot_obtainability_min=0.5))
    with (
        patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d"]),
        pytest.raises(NoValidAvailabilityZoneError, match="gce_spot_obtainability_min=0.5"),
    ):
        resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"])


def test_a_threshold_dropping_every_zone_fails_loudly_without_a_configured_zone():
    """The usual GCE path: nothing configured, so the caller picks `ranked[0]` and needs at least one."""
    resolver = GceAZResolver(capacity_params(availability_zone="", gce_spot_obtainability_min=0.5))
    with (
        patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=[]),
        pytest.raises(NoValidAvailabilityZoneError, match="but 1 are needed"),
    ):
        resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"])


def test_a_threshold_keeping_enough_zones_is_fine():
    """A threshold that keeps enough zones returns the ranked letters."""
    resolver = GceAZResolver(capacity_params(gce_spot_obtainability_min=0.5))
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "b"]):
        assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c", "d"]) == ["d", "b"]


def test_rank_on_demand_runs_never_call_the_advice_api():
    """On-demand runs never call the capacity advice API when ranking letters."""
    resolver = GceAZResolver(capacity_params(instance_provision="on_demand"))
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
        resolver._rank_letters_by_capacity_advice(["us-east1"], ["b", "c"])
    rank.assert_not_called()


def test_a_single_candidate_is_not_worth_an_api_call():
    """A single candidate letter is returned as is, without calling the advice API."""
    resolver = GceAZResolver(capacity_params())
    with patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles") as rank:
        assert resolver._rank_letters_by_capacity_advice(["us-east1"], ["b"]) == ["b"]
    rank.assert_not_called()


# Per-role advice must not turn into per-role placement.
# Each role is asked separately and their preference orders routinely disagree - on a real run the DB type
# scored 0.90 in every `us-east1` zone while the loader type scored 0.10 in two of them. What must never
# follow is a cluster with the DB nodes in one zone and the loaders in another: the roles collapse to one
# ranked list before anything is chosen, and the configured zone count decides how many come out. On GCE
# that count is also `racks_count`.


def test_one_configured_zone_resolves_to_exactly_one():
    """Conflicting per-role advice still resolves a single configured zone to exactly one zone."""
    params = roles_params("b")
    with (
        patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
        patch("sdcm.provision.gce.capacity_advisor.get_zone_advice", side_effect=conflicting_advice()),
    ):
        GceAZResolver(params).resolve()

    resolved = params["availability_zone"]
    assert "," not in resolved, f"roles were split across zones: {resolved!r}"
    assert len(resolved) == 1


def test_three_configured_zones_resolve_to_exactly_three():
    """`availability_zone: 'b,c,d'` asks for three racks; advice may reorder them, never drop one."""
    params = roles_params("b,c,d")
    with (
        patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
        patch("sdcm.provision.gce.capacity_advisor.get_zone_advice", side_effect=conflicting_advice()),
    ):
        GceAZResolver(params).resolve()

    assert sorted(params["availability_zone"].split(",")) == ["b", "c", "d"]


def test_fallback_candidates_keep_the_configured_cardinality():
    """A single-zone config must stay single-zone through every fallback candidate too."""
    params = roles_params("b")
    with (
        patch.object(GceAZResolver, "_common_supported_letters", return_value=["b", "c", "d"]),
        patch("sdcm.provision.gce.zone_resolver.rank_zone_letters_for_roles", return_value=["d", "c", "b"]),
        patch("sdcm.provision.gce.zone_resolver.GceZoneResolver"),
    ):
        candidates = list(GceAZResolver(params).get_az_fallback_candidates())

    assert candidates, "expected at least one fallback candidate"
    assert all(len(candidate) == 1 for candidate in candidates), f"cardinality changed: {candidates}"
