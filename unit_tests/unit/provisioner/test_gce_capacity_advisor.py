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
    rank_zone_letters,
)


@pytest.fixture(autouse=True)
def _clear_advice_cache():
    """The module memoizes advice in a TTLCache; drop it between tests so each starts clean."""
    get_zone_advice.cache_clear()
    yield
    get_zone_advice.cache_clear()


def _recommendation(obtainability, uptime="3600s"):
    return {"recommendations": [{"scores": {"obtainability": obtainability, "estimatedUptime": uptime}}]}


@pytest.fixture(name="mock_advice")
def mock_advice_fixture():
    """Patch the discovery client. `execute()` is driven per test via return_value/side_effect."""
    service = MagicMock()
    service.__enter__ = MagicMock(return_value=service)
    service.__exit__ = MagicMock(return_value=False)
    with patch.object(capacity_advisor, "_advice_service", return_value=(service, "gcp-sct-project-1")):
        yield service.advice.return_value.capacity.return_value.execute


def test_zones_sorted_best_first(mock_advice):
    mock_advice.side_effect = [_recommendation(0.4), _recommendation(0.9), _recommendation(0.7)]

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c", "us-east1-d"])

    assert [item.zone for item in advice] == ["us-east1-c", "us-east1-d", "us-east1-b"]
    assert advice[0].obtainability == 0.9


def test_zone_letter_is_derived_from_the_zone_name():
    """SCT config works in zone letters, the API in full zone names."""
    assert ZoneCapacityAdvice(zone="us-central1-a", obtainability=0.5).zone_letter == "a"


def test_estimated_uptime_is_parsed_from_the_duration_string(mock_advice):
    mock_advice.return_value = _recommendation(0.5, uptime="600s")

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
    recommendation says nothing about the zones it did not pick."""
    mock_advice.return_value = _recommendation(0.5)
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


def test_request_asks_for_spot_and_passes_every_machine_type(mock_advice):  # noqa: ARG001
    body = capacity_advisor._build_request_body(["n2-highmem-16", "n2-highmem-32"], 6, "us-east1-b", "SPOT")

    assert body["size"] == 6
    assert body["instanceProperties"]["scheduling"]["provisioningModel"] == "SPOT"
    assert body["instanceFlexibilityPolicy"]["instanceSelections"]["sct"]["machineTypes"] == [
        "n2-highmem-16",
        "n2-highmem-32",
    ]


@pytest.mark.parametrize(
    "failure", [PermissionError("403 compute.advice.capacity"), TimeoutError("transport"), ValueError("bad shape")]
)
def test_any_error_falls_back_to_no_advice(mock_advice, failure):
    """Callers treat [] as "keep the existing order"; nothing here may propagate into provisioning."""
    mock_advice.side_effect = failure

    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"]) == []


def test_failed_query_is_not_cached(mock_advice):
    """One throttled or briefly-unauthorized call must not leave a whole TTL of unranked provisioning."""
    mock_advice.side_effect = [PermissionError("403"), _recommendation(0.8)]

    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"]) == []
    assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])[0].obtainability == 0.8


def test_successful_advice_is_cached(mock_advice):
    mock_advice.return_value = _recommendation(0.8)

    get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])
    get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b"])

    assert mock_advice.call_count == 1


def test_cache_key_ignores_argument_order(mock_advice):
    mock_advice.return_value = _recommendation(0.8)

    get_zone_advice(["b-type", "a-type"], 3, "us-east1", ["us-east1-c", "us-east1-b"])
    get_zone_advice(["a-type", "b-type"], 3, "us-east1", ["us-east1-b", "us-east1-c"])

    assert mock_advice.call_count == 2, "two zones, one call each - and then served from the cache"


@pytest.mark.parametrize(
    "types, size, zones",
    [([], 3, ["us-east1-b"]), (["n2-highmem-16"], 3, []), (["n2-highmem-16"], 0, ["us-east1-b"])],
)
def test_degenerate_inputs_never_call_the_api(mock_advice, types, size, zones):
    assert get_zone_advice(types, size, "us-east1", zones) == []
    mock_advice.assert_not_called()


def test_a_zone_without_a_score_is_skipped_not_guessed(mock_advice):
    mock_advice.side_effect = [{"recommendations": []}, _recommendation(0.6)]

    advice = get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"])

    assert [item.zone for item in advice] == ["us-east1-c"]


class TestRankZoneLetters:
    def test_letters_are_reordered_best_first(self, mock_advice):
        mock_advice.side_effect = [_recommendation(0.2), _recommendation(0.9)]

        assert rank_zone_letters(["n2-highmem-16"], 3, "us-east1", ["b", "c"]) == ["c", "b"]

    def test_unscored_letters_keep_their_order_and_go_last(self, mock_advice):
        """A zone the API declined to score is not proven bad, so it stays a candidate - just a later one."""
        mock_advice.side_effect = [{"recommendations": []}, _recommendation(0.9)]

        assert rank_zone_letters(["n2-highmem-16"], 3, "us-east1", ["b", "c"]) == ["c", "b"]

    def test_unavailable_advice_returns_none_not_empty(self, mock_advice):
        """None means 'could not ask' and must keep the existing order; [] means 'asked, nothing qualified'."""
        mock_advice.side_effect = PermissionError("403")

        assert rank_zone_letters(["n2-highmem-16"], 3, "us-east1", ["b", "c"]) is None

    def test_minimum_drops_low_zones(self, mock_advice):
        mock_advice.side_effect = [_recommendation(0.2), _recommendation(0.9)]

        assert rank_zone_letters(["n2-highmem-16"], 3, "us-east1", ["b", "c"], min_obtainability=0.5) == ["c"]

    def test_default_minimum_never_drops_anything(self, mock_advice):
        """GCP frames obtainability as a likelihood, not a guarantee - so 0.0 must keep every zone."""
        mock_advice.side_effect = [_recommendation(0.0), _recommendation(0.01)]

        assert sorted(rank_zone_letters(["n2-highmem-16"], 3, "us-east1", ["b", "c"])) == ["b", "c"]

    def test_empty_input_is_not_an_api_call(self, mock_advice):
        assert rank_zone_letters(["n2-highmem-16"], 3, "us-east1", []) == []
        mock_advice.assert_not_called()


class TestRankRegions:
    def test_regions_ordered_by_their_best_zone(self, mock_advice):
        """A region is worth as much as the best zone we could actually land in, so max - not mean - decides."""
        mock_advice.side_effect = [
            _recommendation(0.1),  # us-east1-b
            _recommendation(0.9),  # us-east1-c  -> best 0.9
            _recommendation(0.5),  # us-west1-a
            _recommendation(0.5),  # us-west1-b  -> best 0.5
        ]

        ranked = rank_regions(
            ["n2-highmem-16"],
            3,
            ["us-east1", "us-west1"],
            {"us-east1": ["us-east1-b", "us-east1-c"], "us-west1": ["us-west1-a", "us-west1-b"]},
        )

        assert ranked == ["us-east1", "us-west1"]

    def test_regions_without_advice_go_last(self, mock_advice):
        mock_advice.side_effect = [PermissionError("403"), _recommendation(0.4)]

        ranked = rank_regions(
            ["n2-highmem-16"],
            3,
            ["us-east1", "us-west1"],
            {"us-east1": ["us-east1-b"], "us-west1": ["us-west1-a"]},
        )

        assert ranked == ["us-west1", "us-east1"]

    def test_no_advice_at_all_returns_none(self, mock_advice):
        mock_advice.side_effect = PermissionError("403")

        assert rank_regions(["n2-highmem-16"], 3, ["us-east1"], {"us-east1": ["us-east1-b"]}) is None

    def test_region_without_known_zones_is_left_alone(self, mock_advice):
        mock_advice.return_value = _recommendation(0.4)

        ranked = rank_regions(["n2-highmem-16"], 3, ["us-east1", "us-west1"], {"us-east1": ["us-east1-b"]})

        assert ranked == ["us-east1", "us-west1"]


class TestPartialZoneFailures:
    """GCP rejects the whole call when one zone cannot host the machine type - verified live against
    `us-west1`, where `us-west1-c` does not offer `z3-highmem-8-highlssd` and the region therefore came
    back entirely unranked. Ranking the zones that DO work is the whole job."""

    def test_one_unsupported_zone_does_not_cost_the_others(self, mock_advice):
        unsupported = RuntimeError("Machine specification is not supported in locations: [us-west1-c].")
        mock_advice.side_effect = [_recommendation(0.9), unsupported, _recommendation(0.4)]

        advice = get_zone_advice(["z3-highmem-8-highlssd"], 6, "us-west1", ["us-west1-a", "us-west1-c", "us-west1-b"])

        assert [item.zone for item in advice] == ["us-west1-a", "us-west1-b"]

    def test_a_cause_that_hits_every_zone_still_reports_unavailable(self, mock_advice):
        """A missing permission or a throttle fails all of them, and must not be cached as an answer."""
        mock_advice.side_effect = PermissionError("403 compute.advice.capacity")

        assert get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"]) == []
        mock_advice.side_effect = [_recommendation(0.7), _recommendation(0.5)]
        assert len(get_zone_advice(["n2-highmem-16"], 3, "us-east1", ["us-east1-b", "us-east1-c"])) == 2


@pytest.mark.parametrize(
    "message,expected_hint",
    [
        ("HttpError 400 ... The service is not available for this project.", "Preview enrolment"),
        ("HttpError 400 ... Machine specification is not supported in locations: [us-west1-c].", "not offered"),
        ("HttpError 403 ... compute.advice.capacity", "roles/compute.viewer"),
    ],
    ids=["not-enrolled", "unsupported-zone", "permission"],
)
def test_the_hint_names_the_actual_cause(message, expected_hint):
    """These three read nothing alike, and the enrolment one is NOT a permission problem - pointing at IAM
    sends the reader to a console that is already correct. Seen live: enrolment is per provisioning model,
    so SPOT advice works on our project while on-demand returns 'not available for this project'."""
    assert expected_hint in capacity_advisor._unavailable_hint(message)
