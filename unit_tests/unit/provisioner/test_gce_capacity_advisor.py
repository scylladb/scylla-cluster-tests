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

"""GCE Capacity Advisor (`compute.beta advice.capacity`) — SCT-896.

The API is in Preview and reachable only through the discovery client, so the client is patched wholesale
rather than mocked at the transport level. What matters here is the contract the rest of SCT relies on:
best-first ordering, and an empty result on *any* failure so callers keep their existing order.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.gce import capacity_advisor
from sdcm.provision.gce.capacity_advisor import (
    ZoneCapacityAdvice,
    get_zone_advice,
    rank_regions,
    rank_zone_letters_for_roles,
)


@pytest.fixture(autouse=True)
def clear_advice_cache():
    """The module memoizes advice in a TTLCache; drop it between tests so each starts clean."""
    get_zone_advice.cache_clear()
    yield
    get_zone_advice.cache_clear()


def recommendation_payload(obtainability, uptime="3600s"):
    """Capacity-advice response holding one recommendation with the given obtainability and uptime."""
    return {"recommendations": [{"scores": {"obtainability": obtainability, "estimatedUptime": uptime}}]}


def advice_for_letters(mapping):
    """ZoneCapacityAdvice for us-east1 zones, mapping each zone letter to its obtainability."""
    return [ZoneCapacityAdvice(zone=f"us-east1-{letter}", obtainability=value) for letter, value in mapping.items()]


@pytest.fixture
def mock_advice():
    """Patch the discovery client. `execute()` is driven per test via return_value/side_effect."""
    service = MagicMock()
    service.__enter__ = MagicMock(return_value=service)
    service.__exit__ = MagicMock(return_value=False)
    with patch.object(capacity_advisor, "_advice_service", return_value=(service, "gcp-sct-project-1")):
        yield service.advice.return_value.capacity.return_value.execute


def test_zones_sorted_best_first(mock_advice):
    """get_zone_advice returns the zones ordered by obtainability, best first."""
    mock_advice.side_effect = [recommendation_payload(0.4), recommendation_payload(0.9), recommendation_payload(0.7)]

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c", "us-east1-d"])

    assert [item.zone for item in advice] == ["us-east1-c", "us-east1-d", "us-east1-b"]
    assert advice[0].obtainability == 0.9


def test_zone_letter_is_derived_from_the_zone_name():
    """SCT config works in zone letters, the API in full zone names."""
    assert ZoneCapacityAdvice(zone="us-central1-a", obtainability=0.5).zone_letter == "a"


def test_estimated_uptime_is_parsed_from_the_duration_string(mock_advice):
    """Duration strings in the estimated uptime, such as '600s', are parsed into seconds."""
    mock_advice.return_value = recommendation_payload(0.5, uptime="600s")

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])

    assert advice[0].estimated_uptime_seconds == 600


def test_unparsable_uptime_does_not_lose_the_score(mock_advice):
    """A Preview API may change this field's shape; the obtainability is the part we actually rank on."""
    mock_advice.return_value = {"recommendations": [{"scores": {"obtainability": 0.5, "estimatedUptime": "soon"}}]}

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])

    assert advice[0].obtainability == 0.5
    assert advice[0].estimated_uptime_seconds is None


def test_request_pins_a_single_zone_per_call(mock_advice):
    """Ranking zones against each other requires scoring each on its own; a balanced multi-zone
    recommendation says nothing about the zones it did not pick.
    """
    mock_advice.return_value = recommendation_payload(0.5)
    built = []
    real_build_request_body = capacity_advisor._build_request_body

    def record(*args, **kwargs):
        built.append(real_build_request_body(*args, **kwargs))
        return built[-1]

    with patch.object(capacity_advisor, "_build_request_body", side_effect=record):
        get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"])

    # Verified against the live API: a bare zone name is rejected as "The URL is malformed".
    assert [b["distributionPolicy"]["zones"] for b in built] == [
        [{"zone": "zones/us-east1-b"}],
        [{"zone": "zones/us-east1-c"}],
    ]
    assert {b["distributionPolicy"]["targetShape"] for b in built} == {"ANY_SINGLE_ZONE"}


def test_request_asks_for_spot_and_passes_every_machine_type():
    """The request body asks for SPOT provisioning and lists every machine type and the size."""
    body = capacity_advisor._build_request_body(["n2-highmem-16", "n2-highmem-32"], 6, "us-east1-b", "SPOT")

    assert body["size"] == 6
    assert body["instanceProperties"]["scheduling"]["provisioningModel"] == "SPOT"
    assert body["instanceFlexibilityPolicy"]["instanceSelections"]["sct"]["machineTypes"] == [
        "n2-highmem-16",
        "n2-highmem-32",
    ]


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(PermissionError("403 compute.advice.capacity"), id="permission"),
        pytest.param(TimeoutError("transport"), id="transport"),
        pytest.param(ValueError("bad shape"), id="bad-shape"),
    ],
)
def test_any_error_falls_back_to_no_advice(mock_advice, failure):
    """Callers treat [] as "keep the existing order"; nothing here may propagate into provisioning."""
    mock_advice.side_effect = failure

    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"]) == []


def test_failed_query_is_not_cached(mock_advice):
    """One throttled or briefly-unauthorized call must not leave a whole TTL of unranked provisioning."""
    mock_advice.side_effect = [PermissionError("403"), recommendation_payload(0.8)]

    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"]) == []
    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])[0].obtainability == 0.8


def test_successful_advice_is_cached(mock_advice):
    """A successful query is memoized, so a repeated identical call does not hit the API again."""
    mock_advice.return_value = recommendation_payload(0.8)

    get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])
    get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])

    assert mock_advice.call_count == 1


def test_cache_key_ignores_argument_order(mock_advice):
    """Permuting machine types and zones hits the same cache entries, so the API is asked once per zone."""
    mock_advice.return_value = recommendation_payload(0.8)

    get_zone_advice(["b-type", "a-type"], 3, "us-east1", ["us-east1-c", "us-east1-b"])
    get_zone_advice(["a-type", "b-type"], 3, "us-east1", ["us-east1-b", "us-east1-c"])

    assert mock_advice.call_count == 2, "two zones, one call each - and then served from the cache"


@pytest.mark.parametrize(
    "types, size, zones",
    [
        pytest.param([], 3, ["us-east1-b"], id="no-types"),
        pytest.param(["n2-highmem-16"], 3, [], id="no-zones"),
        pytest.param(["n2-highmem-16"], 0, ["us-east1-b"], id="zero-size"),
    ],
)
def test_degenerate_inputs_never_call_the_api(mock_advice, types, size, zones):
    """Empty machine types, zones or a zero size return [] without querying the API."""
    assert get_zone_advice(types, size, "us-east1", zones) == []
    mock_advice.assert_not_called()


def test_a_zone_without_a_score_is_skipped_not_guessed(mock_advice):
    """A zone whose response has no recommendation is left out of the ranking instead of given a guessed score."""
    mock_advice.side_effect = [{"recommendations": []}, recommendation_payload(0.6)]

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"])

    assert [item.zone for item in advice] == ["us-east1-c"]


def test_regions_ordered_by_their_best_zone(mock_advice):
    """A region is worth as much as the best zone we could actually land in, so max - not mean - decides."""
    mock_advice.side_effect = [
        recommendation_payload(0.1),  # us-east1-b
        recommendation_payload(0.9),  # us-east1-c  -> best 0.9
        recommendation_payload(0.5),  # us-west1-a
        recommendation_payload(0.5),  # us-west1-b  -> best 0.5
    ]

    ranked = rank_regions(
        ["n2-highmem-16"],
        3,
        ["us-east1", "us-west1"],
        {"us-east1": ["us-east1-b", "us-east1-c"], "us-west1": ["us-west1-a", "us-west1-b"]},
    )

    assert ranked == ["us-east1", "us-west1"]


def test_regions_without_advice_go_last(mock_advice):
    """A region whose advice query fails is ranked after regions that returned advice."""
    mock_advice.side_effect = [PermissionError("403"), recommendation_payload(0.4)]

    ranked = rank_regions(
        ["n2-highmem-16"],
        3,
        ["us-east1", "us-west1"],
        {"us-east1": ["us-east1-b"], "us-west1": ["us-west1-a"]},
    )

    assert ranked == ["us-west1", "us-east1"]


def test_no_advice_at_all_returns_none(mock_advice):
    """When no region gets any advice, rank_regions returns None so the caller keeps its own order."""
    mock_advice.side_effect = PermissionError("403")

    assert rank_regions(["n2-highmem-16"], 3, ["us-east1"], {"us-east1": ["us-east1-b"]}) is None


def test_region_without_known_zones_is_left_alone(mock_advice):
    """A region with no zone list keeps its original position in the ranking."""
    mock_advice.return_value = recommendation_payload(0.4)

    ranked = rank_regions(["n2-highmem-16"], 3, ["us-east1", "us-west1"], {"us-east1": ["us-east1-b"]})

    assert ranked == ["us-east1", "us-west1"]


def test_one_unsupported_zone_does_not_cost_the_others(mock_advice):
    """GCP rejects the whole call when one zone cannot host the machine type - verified live against
    `us-west1`, where `us-west1-c` does not offer `z3-highmem-8-highlssd` and the region therefore came
    back entirely unranked. Ranking the zones that DO work is the whole job.
    """
    unsupported = RuntimeError("Machine specification is not supported in locations: [us-west1-c].")
    mock_advice.side_effect = [recommendation_payload(0.9), unsupported, recommendation_payload(0.4)]

    advice = get_zone_advice(["z3-highmem-8-highlssd"], 6, "us-west1", ["us-west1-a", "us-west1-c", "us-west1-b"])

    assert [item.zone for item in advice] == ["us-west1-a", "us-west1-b"]


def test_a_cause_that_hits_every_zone_still_reports_unavailable(mock_advice):
    """A missing permission or a throttle fails all of them, and must not be cached as an answer."""
    mock_advice.side_effect = PermissionError("403 compute.advice.capacity")

    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"]) == []
    mock_advice.side_effect = [recommendation_payload(0.7), recommendation_payload(0.5)]
    assert len(get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"])) == 2


@pytest.mark.parametrize(
    "message,expected_hint",
    [
        pytest.param(
            "HttpError 400 ... The service is not available for this project.",
            "Preview enrolment",
            id="not-enrolled",
        ),
        pytest.param(
            "HttpError 400 ... Machine specification is not supported in locations: [us-west1-c].",
            "not offered",
            id="unsupported-zone",
        ),
        pytest.param("HttpError 403 ... compute.advice.capacity", "roles/compute.viewer", id="permission"),
    ],
)
def test_the_hint_names_the_actual_cause(message, expected_hint):
    """These three read nothing alike, and the enrolment one is NOT a permission problem - pointing at IAM
    sends the reader to a console that is already correct. Seen live: enrolment is per provisioning model,
    so SPOT advice works on our project while on-demand returns 'not available for this project'.
    """
    assert expected_hint in capacity_advisor._unavailable_hint(message)


def test_the_scarcest_role_decides_the_zone():
    """A zone has to hold the loaders and the monitor too, not just the DB nodes.

    This is the gap a real run fell into: the DB type scored 0.90 in every `us-east1` zone, the run took
    `us-east1-b`, and provisioning then failed creating a *loader* there. Measured after the fact, the loader
    type scored 0.10 in `b` and `c` and 0.90 in `d` - the one zone good for the whole cluster.
    """
    roles = [("db", ["z3-highmem-8-highlssd"], 6), ("loaders", ["n4a-standard-4"], 2)]
    with patch(
        "sdcm.provision.gce.capacity_advisor.get_zone_advice",
        side_effect=[
            advice_for_letters({"b": 0.9, "c": 0.9, "d": 0.9}),  # db: indistinguishable
            advice_for_letters({"b": 0.1, "c": 0.1, "d": 0.9}),  # loaders: the real constraint
        ],
    ):
        ranked = rank_zone_letters_for_roles(roles, region="us-east1", zone_letters=["b", "c", "d"])

    assert ranked == ["d", "b", "c"], "d is the only zone that can take the whole cluster"


def test_a_role_without_advice_abandons_the_ranking():
    """If any role gets no advice, the zone ranking is abandoned and None is returned."""
    roles = [("db", ["z3-highmem-8-highlssd"], 6), ("loaders", ["n4a-standard-4"], 2)]
    with patch(
        "sdcm.provision.gce.capacity_advisor.get_zone_advice",
        side_effect=[advice_for_letters({"b": 0.9, "d": 0.5}), []],
    ):
        assert rank_zone_letters_for_roles(roles, region="us-east1", zone_letters=["b", "d"]) is None


def test_roles_are_asked_separately_never_merged():
    """Each role is queried on its own, with its own machine types and size."""
    roles = [("db", ["z3-highmem-8-highlssd"], 6), ("monitor", ["e2-standard-2"], 1)]
    with patch(
        "sdcm.provision.gce.capacity_advisor.get_zone_advice",
        side_effect=[advice_for_letters({"b": 0.2}), advice_for_letters({"b": 0.9})],
    ) as advice:
        rank_zone_letters_for_roles(roles, region="us-east1", zone_letters=["b"])

    assert [c.kwargs["machine_types"] for c in advice.call_args_list] == [
        ["z3-highmem-8-highlssd"],
        ["e2-standard-2"],
    ]
    assert [c.kwargs["size"] for c in advice.call_args_list] == [6, 1]


def test_minimum_applies_to_the_combined_score():
    """A zone strong for the DB but hopeless for the loaders must fail the threshold, not pass it."""
    roles = [("db", ["z3-highmem-8-highlssd"], 6), ("loaders", ["n4a-standard-4"], 2)]
    with patch(
        "sdcm.provision.gce.capacity_advisor.get_zone_advice",
        side_effect=[advice_for_letters({"b": 0.9, "d": 0.9}), advice_for_letters({"b": 0.1, "d": 0.9})],
    ):
        ranked = rank_zone_letters_for_roles(roles, region="us-east1", zone_letters=["b", "d"], min_obtainability=0.5)
    assert ranked == ["d"]
