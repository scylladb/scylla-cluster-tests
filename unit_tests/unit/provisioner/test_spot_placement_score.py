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

from unittest.mock import MagicMock, patch

import botocore.session
import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)

from sdcm.provision.aws.constants import SPOT_FLEET_ALLOCATION_STRATEGY

from botocore.parsers import ResponseParserError

from sdcm.provision.aws import spot_placement_score
from sdcm.provision.aws.spot_placement_score import (
    rank_az_letters_for_roles,
    PlacementScore,
    _chunk_regions,
    get_scores,
    rank_regions,
)

# moto 5.x does not implement GetSpotPlacementScores, so the client is patched directly.


@pytest.fixture(autouse=True)
def _clear_score_cache():
    """The module memoizes scores in a TTLCache; drop it between tests so each starts clean."""
    get_scores.cache_clear()
    yield
    get_scores.cache_clear()


def _score(region, zone_id, score):
    return {"Region": region, "AvailabilityZoneId": zone_id, "Score": score}


@pytest.fixture(name="mock_ec2")
def mock_ec2_fixture():
    """Patch the cached EC2 client dict and the AZ-id -> letter lookup."""
    client = MagicMock()
    zone_maps = {
        "eu-west-1": {"euw1-az1": "b", "euw1-az2": "c", "euw1-az3": "a"},
        "eu-west-2": {"euw2-az1": "a", "euw2-az2": "b", "euw2-az3": "c"},
    }
    with (
        patch.dict(spot_placement_score.ec2_clients, {"eu-west-1": client, "eu-west-2": client}, clear=False),
        patch.object(spot_placement_score, "_zone_id_to_letter", side_effect=lambda region: zone_maps.get(region, {})),
    ):
        yield client


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetSpotPlacementScores")


def test_scores_sorted_best_first_with_az_letters(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az3", 3),
            _score("eu-west-1", "euw1-az1", 9),
            _score("eu-west-1", "euw1-az2", 7),
        ]
    }
    scores = get_scores(["i4i.large", "i4i.xlarge", "i7i.large"], 6, ["eu-west-1"])

    assert scores == [
        PlacementScore(region="eu-west-1", score=9, az_letter="b"),
        PlacementScore(region="eu-west-1", score=7, az_letter="c"),
        PlacementScore(region="eu-west-1", score=3, az_letter="a"),
    ]
    assert scores[0].location == "eu-west-1b"


def test_az_letters_reordered_by_score(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az3", 1),  # a
            _score("eu-west-1", "euw1-az1", 9),  # b
            _score("eu-west-1", "euw1-az2", 5),  # c
        ]
    }
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"]) == ["b", "c", "a"]


def test_unscored_az_letters_kept_last_in_original_order(mock_ec2):
    """An AZ absent from the top-10 response was not proven bad, so it must not be dropped."""
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [_score("eu-west-1", "euw1-az2", 8)]  # only 'c'
    }
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"]) == ["c", "a", "b"]


def test_min_score_drops_low_scoring_azs(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az3", 2),  # a
            _score("eu-west-1", "euw1-az1", 8),  # b
            _score("eu-west-1", "euw1-az2", 5),  # c
        ]
    }
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"], min_score=5) == [
        "b",
        "c",
    ]


def test_min_score_zero_keeps_everything(mock_ec2):
    """Default min_score=0 must never drop an AZ - the API contract is 'recommendation only'."""
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az3", 1),
            _score("eu-west-1", "euw1-az1", 1),
            _score("eu-west-1", "euw1-az2", 1),
        ]
    }
    assert sorted(rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"])) == [
        "a",
        "b",
        "c",
    ]


def test_region_ranking_uses_region_granularity(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            {"Region": "eu-west-1", "Score": 2},
            {"Region": "eu-west-2", "Score": 9},
        ]
    }
    assert rank_regions(["i4i.large"], 6, ["eu-west-1", "eu-west-2"]) == ["eu-west-2", "eu-west-1"]
    assert mock_ec2.get_spot_placement_scores.call_args.kwargs["SingleAvailabilityZone"] is False


def test_unscored_regions_kept_last(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {"SpotPlacementScores": [{"Region": "eu-west-2", "Score": 4}]}
    assert rank_regions(["i4i.large"], 6, ["eu-west-1", "eu-west-2"]) == ["eu-west-2", "eu-west-1"]


@pytest.mark.parametrize("error_code", ["AccessDenied", "UnauthorizedOperation", "InvalidParameterValue"])
def test_client_errors_fall_back_to_given_order(mock_ec2, error_code):
    """Any API failure - notably a missing IAM permission - must preserve the caller's ordering."""
    mock_ec2.get_spot_placement_scores.side_effect = _client_error(error_code)

    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    # None (not []) signals "unavailable" so a min_score rejecting everything stays distinguishable
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"]) is None
    assert rank_regions(["i4i.large"], 6, ["eu-west-1", "eu-west-2"]) == ["eu-west-1", "eu-west-2"]


def test_pagination_follows_next_token(mock_ec2):
    mock_ec2.get_spot_placement_scores.side_effect = [
        {"SpotPlacementScores": [_score("eu-west-1", "euw1-az3", 4)], "NextToken": "page-2"},
        {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]},
    ]
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b"]) == ["b", "a"]
    assert mock_ec2.get_spot_placement_scores.call_count == 2
    assert mock_ec2.get_spot_placement_scores.call_args_list[1].kwargs["NextToken"] == "page-2"


def test_results_are_cached_across_calls(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]}
    get_scores(["i4i.large"], 6, ["eu-west-1"])
    get_scores(["i4i.large"], 6, ["eu-west-1"])
    assert mock_ec2.get_spot_placement_scores.call_count == 1


def test_cache_key_ignores_argument_order(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]}
    get_scores(["a1.large", "b2.large"], 6, ["eu-west-2", "eu-west-1"])
    get_scores(["b2.large", "a1.large"], 6, ["eu-west-1", "eu-west-2"])
    assert mock_ec2.get_spot_placement_scores.call_count == 1


def test_failed_query_is_not_cached(mock_ec2):
    """A throttled or briefly-unauthorized call must not blind the next hour of provisioning."""
    mock_ec2.get_spot_placement_scores.side_effect = [
        _client_error("RequestLimitExceeded"),
        {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]},
    ]
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == [PlacementScore(region="eu-west-1", score=6, az_letter="b")]
    assert mock_ec2.get_spot_placement_scores.call_count == 2


def test_unexpected_error_is_not_cached_either(mock_ec2):
    """The total guard returns [] for anything at all; none of it is an answer worth remembering."""
    mock_ec2.get_spot_placement_scores.side_effect = [
        RuntimeError("boom"),
        {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]},
    ]
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) != []
    assert mock_ec2.get_spot_placement_scores.call_count == 2


def test_empty_answer_is_not_cached(mock_ec2):
    """An empty response is a valid answer, but a cheap one to ask again - and indistinguishable from a miss."""
    mock_ec2.get_spot_placement_scores.side_effect = [
        {"SpotPlacementScores": []},
        {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 6)]},
    ]
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) != []
    assert mock_ec2.get_spot_placement_scores.call_count == 2


def test_unmappable_zone_id_is_skipped(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az9", 9),  # not in the account's zone map
            _score("eu-west-1", "euw1-az1", 4),
        ]
    }
    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == [PlacementScore(region="eu-west-1", score=4, az_letter="b")]


@pytest.mark.parametrize(
    "types, capacity, regions",
    [([], 6, ["eu-west-1"]), (["i4i.large"], 6, []), (["i4i.large"], 0, ["eu-west-1"])],
)
def test_degenerate_inputs_never_call_the_api(mock_ec2, types, capacity, regions):
    assert get_scores(types, capacity, regions) == []
    mock_ec2.get_spot_placement_scores.assert_not_called()


def test_region_lists_are_chunked_to_api_limit():
    """RegionName.N accepts at most 10 entries."""
    regions = [f"region-{index}" for index in range(23)]
    chunks = _chunk_regions(regions)

    assert [len(chunk) for chunk in chunks] == [10, 10, 3]
    assert [region for chunk in chunks for region in chunk] == regions


def test_more_than_ten_regions_issues_multiple_calls(mock_ec2):
    mock_ec2.get_spot_placement_scores.return_value = {"SpotPlacementScores": []}
    # `eu-west-1` first: the client is looked up by the first region, and only that one is patched
    regions = ["eu-west-1"] + [f"synthetic-region-{index}" for index in range(11)]
    get_scores(["i4i.large"], 6, regions, single_az=False)

    assert mock_ec2.get_spot_placement_scores.call_count == 2
    requested = [call.kwargs["RegionNames"] for call in mock_ec2.get_spot_placement_scores.call_args_list]
    assert [len(chunk) for chunk in requested] == [10, 2]


def test_transport_errors_fall_back_to_given_order(mock_ec2):
    """BotoCoreError subclasses are NOT ClientError. Scoring is on by default for AWS and runs on every
    provisioning path, so missing these would turn a transient network blip into a provisioning abort."""
    mock_ec2.get_spot_placement_scores.side_effect = EndpointConnectionError(endpoint_url="https://ec2")

    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"]) is None


def test_credentials_error_falls_back_to_given_order(mock_ec2):
    mock_ec2.get_spot_placement_scores.side_effect = NoCredentialsError()
    assert rank_regions(["i4i.large"], 6, ["eu-west-1", "eu-west-2"]) == ["eu-west-1", "eu-west-2"]
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b"]) is None


def test_fleet_allocation_strategy_is_valid_for_request_spot_fleet():
    """RequestSpotFleet takes camelCase ('capacityOptimized'); the hyphenated spelling belongs to the
    different CreateFleet API. botocore does not validate enums client-side, so the wrong value would only
    fail against real AWS with InvalidParameterValue."""
    shape = botocore.session.get_session().get_service_model("ec2").shape_for("SpotFleetRequestConfigData")
    assert SPOT_FLEET_ALLOCATION_STRATEGY in shape.members["AllocationStrategy"].enum


def test_response_parser_error_falls_back_to_given_order(mock_ec2):
    """`ResponseParserError` subclasses plain Exception - neither ClientError nor BotoCoreError.

    It fires whenever the endpoint returns a non-XML body: a proxy/gateway error page, a truncated
    response, or an endpoint that does not implement the action (which is how the moto-backed
    integration test caught this - it aborted provisioning for every spot run).
    """
    mock_ec2.get_spot_placement_scores.side_effect = ResponseParserError("invalid XML received")

    assert get_scores(["i4i.large"], 6, ["eu-west-1"]) == []
    # None (not []) signals "unavailable" so a min_score rejecting everything stays distinguishable
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"]) is None
    assert rank_regions(["i4i.large"], 6, ["eu-west-1", "eu-west-2"]) == ["eu-west-1", "eu-west-2"]


@pytest.mark.parametrize(
    "exc",
    [
        ResponseParserError("bad xml"),
        ValueError("something unexpected"),
        KeyError("missing"),
        RuntimeError("boom"),
    ],
)
def test_any_exception_degrades_rather_than_raising(mock_ec2, exc):
    """The contract is total: scoring is an optimization and must never fail a provisioning run."""
    mock_ec2.get_spot_placement_scores.side_effect = exc
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b"]) is None


def test_zone_id_lookup_failure_degrades(mock_ec2):
    """The AZ-id -> letter lookup is a second API call, with the same total-failure contract."""
    mock_ec2.get_spot_placement_scores.return_value = {"SpotPlacementScores": [_score("eu-west-1", "euw1-az1", 9)]}
    with patch.object(spot_placement_score, "_zone_id_to_letter", side_effect=ResponseParserError("bad xml")):
        # the real _zone_id_to_letter swallows this itself; assert the caller survives either way
        try:
            result = rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b"])
        except ResponseParserError:
            pytest.fail("_zone_id_to_letter failure must not propagate to the caller")
    assert result is None


def test_all_azs_below_min_score_returns_empty_not_input(mock_ec2):
    """Empty must be distinguishable from None, or a configured minimum is silently ignored.

    Returning the input list here would mean `spot_placement_score_min` is honoured when it drops *some*
    AZs but not when it drops *all* of them - the one case it exists to catch.
    """
    mock_ec2.get_spot_placement_scores.return_value = {
        "SpotPlacementScores": [
            _score("eu-west-1", "euw1-az3", 1),
            _score("eu-west-1", "euw1-az1", 2),
            _score("eu-west-1", "euw1-az2", 1),
        ]
    }
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", ["a", "b", "c"], min_score=8) == []


def test_empty_input_returns_empty(mock_ec2):
    assert rank_az_letters_for_roles([("db", ["i4i.large"], 6)], "eu-west-1", [], min_score=8) == []
    mock_ec2.get_spot_placement_scores.assert_not_called()


class TestRoleAwareRanking:
    """An AZ has to hold the loaders and the monitor too, not just the DB nodes.

    Measured in us-west-2: the DB type scored 3 in every AZ while `c` scored 1 for both the loader and the
    monitor. Ranking on DB types alone sees four equal AZs, falls back to the alphabetical tiebreak, and can
    land on the one AZ that cannot supply the rest of the cluster.
    """

    @staticmethod
    def _scores(mapping):
        return [PlacementScore(region="us-west-2", az_letter=letter, score=score) for letter, score in mapping.items()]

    def test_the_scarcest_role_decides_the_az(self):
        roles = [("db", ["i7i.2xlarge"], 6), ("loaders", ["c7g.xlarge"], 2)]
        with patch(
            "sdcm.provision.aws.spot_placement_score.get_scores",
            side_effect=[
                self._scores({"a": 3, "b": 3, "c": 3}),  # db: indistinguishable
                self._scores({"a": 3, "b": 1, "c": 3}),  # loaders: b is the trap
            ],
        ):
            ranked = rank_az_letters_for_roles(roles, region="us-west-2", az_letters=["a", "b", "c"])

        assert ranked == ["a", "c", "b"], "b must sort last on its loader score, not first on its db score"

    def test_a_role_without_scores_abandons_the_ranking(self):
        """A minimum over a partly-unknown set is a guess; today's order is the honest fallback."""
        roles = [("db", ["i7i.2xlarge"], 6), ("loaders", ["c7g.xlarge"], 2)]
        with patch(
            "sdcm.provision.aws.spot_placement_score.get_scores",
            side_effect=[self._scores({"a": 3, "b": 1}), []],
        ):
            assert rank_az_letters_for_roles(roles, region="us-west-2", az_letters=["a", "b"]) is None

    def test_roles_are_scored_separately_never_merged(self):
        """Merged into one request the types become interchangeable alternatives, and a plentiful
        monitor type answers for the whole cluster."""
        roles = [("db", ["i7i.2xlarge"], 6), ("monitor", ["t3.large"], 1)]
        with patch(
            "sdcm.provision.aws.spot_placement_score.get_scores",
            side_effect=[self._scores({"a": 1}), self._scores({"a": 9})],
        ) as get:
            rank_az_letters_for_roles(roles, region="us-west-2", az_letters=["a"])

        assert [call.kwargs["instance_types"] for call in get.call_args_list] == [["i7i.2xlarge"], ["t3.large"]]
        assert [call.kwargs["target_capacity"] for call in get.call_args_list] == [6, 1]

    def test_empty_or_zero_count_roles_are_skipped(self):
        roles = [("db", ["i7i.2xlarge"], 6), ("loaders", [], 2), ("monitor", ["t3.large"], 0)]
        with patch(
            "sdcm.provision.aws.spot_placement_score.get_scores", side_effect=[self._scores({"a": 3, "b": 2})]
        ) as get:
            assert rank_az_letters_for_roles(roles, region="us-west-2", az_letters=["a", "b"]) == ["a", "b"]
        assert get.call_count == 1
