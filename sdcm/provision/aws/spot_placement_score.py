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

"""Rank AWS regions and availability zones by `ec2:GetSpotPlacementScores`.

Scores are a *relative* ranking, never a go/no-go verdict. Two caveats from the AWS API docs drive the design:

- "We recommend that you specify at least three instance types. If you specify one or two instance types, or
  specify variations of a single instance type, the returned placement score will always be low." So a low score
  for a single instance type is a structural artifact of the query, not evidence that capacity is missing. This
  module therefore only ever orders candidates, and logs a warning when given fewer than three types.
- "The Spot placement score serves as a recommendation only. No score guarantees that your Spot request will be
  fully or partially fulfilled." The existing AZ/region capacity fallback stays the safety net; scores only
  reorder the candidate list it walks.

A high per-AZ score also "assumes that your fleet request will be configured to use a single Availability Zone
and the capacity-optimized allocation strategy" - see `SPOT_FLEET_ALLOCATION_STRATEGY` in `constants.py`.

Every failure path returns an empty result so callers fall back to their previous ordering: the scores are an
optimization and must never be able to fail a test run. The error handling is deliberately total, because the
exception surface is wider than it first appears - `ClientError` (API-level, e.g. `AccessDenied` for the
distinct `ec2:GetSpotPlacementScores` IAM action, which runners can lag), `BotoCoreError` (transport-level:
endpoint/DNS/timeout/credentials), and `botocore.parsers.ResponseParserError`, which subclasses plain
`Exception` and fires whenever the endpoint returns a non-XML body. Since scoring is enabled by default for
AWS and runs on every provisioning path, a miss here turns a blip - or an endpoint that simply does not
implement the action - into a provisioning abort.
"""

import logging
from dataclasses import dataclass

from botocore.exceptions import ClientError
from cachetools import TTLCache, cached
from cachetools.keys import hashkey

from sdcm.provision.aws.constants import (
    SPOT_PLACEMENT_SCORE_CACHE_TTL,
    SPOT_PLACEMENT_SCORE_MAX_REGIONS,
    SPOT_PLACEMENT_SCORE_RECOMMENDED_TYPES,
)
from sdcm.provision.aws.utils import ec2_clients
from sdcm.utils.aws_region import AwsRegion

LOGGER = logging.getLogger(__name__)

# Errors meaning "we are not allowed to ask", as opposed to "the answer is no". Treated the same way as any other
# client error (fall back to the previous ordering), but logged distinctly since the fix is an IAM policy change.
_PERMISSION_ERROR_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedOperation",
    }
)


@dataclass(frozen=True)
class PlacementScore:
    """A single scored placement. `az_letter` is None for region-granularity scores."""

    region: str
    score: int
    az_letter: str | None = None

    @property
    def location(self) -> str:
        return f"{self.region}{self.az_letter}" if self.az_letter else self.region


def _chunk_regions(regions: list[str]) -> list[list[str]]:
    """Split `regions` to satisfy the API's limit of 10 entries in RegionName.N."""
    return [
        regions[index : index + SPOT_PLACEMENT_SCORE_MAX_REGIONS]
        for index in range(0, len(regions), SPOT_PLACEMENT_SCORE_MAX_REGIONS)
    ]


def _zone_id_to_letter(region: str) -> dict[str, str]:
    """Map per-account AZ IDs to AZ letters, e.g. `{"euw1-az3": "a"}` in `eu-west-1`.

    The API reports `AvailabilityZoneId` ("euw1-az3"), while SCT config uses AZ letters ("a"). The mapping is
    shuffled per AWS account, so it must be resolved at runtime and never hardcoded.
    """
    try:
        response = AwsRegion(region_name=region).client.describe_availability_zones(
            Filters=[{"Name": "region-name", "Values": [region]}]
        )
    except Exception as exc:  # noqa: BLE001
        # Deliberately total. This function's whole contract is "never break the caller", and the exception
        # surface is wider than it looks: ClientError (API), BotoCoreError (transport - EndpointConnectionError,
        # ConnectTimeoutError, NoCredentialsError), and botocore.parsers.ResponseParserError, which subclasses
        # plain Exception and is raised whenever the endpoint returns a non-XML body (a proxy/gateway error
        # page, a truncated response, or an endpoint that does not implement the action at all).
        LOGGER.warning("Spot placement scores: cannot map AZ IDs in %s: %s", region, exc)
        return {}
    return {zone["ZoneId"]: zone["ZoneName"][len(region) :] for zone in response["AvailabilityZones"]}


def _query_scores(instance_types: list[str], target_capacity: int, regions: list[str], single_az: bool) -> list[dict]:
    """Call the API across region chunks, following `NextToken`. Returns [] on any client error."""
    # The call is region-agnostic (regions are a request parameter), but still needs *a* client to issue it, so
    # use the first requested region's cached client.
    client = ec2_clients[regions[0]]
    raw_scores = []
    for region_chunk in _chunk_regions(regions):
        next_token = None
        while True:
            request = {
                "InstanceTypes": instance_types,
                "TargetCapacity": target_capacity,
                "SingleAvailabilityZone": single_az,
                "RegionNames": region_chunk,
                # API minimum; the response is capped at the top 10 placements regardless
                "MaxResults": 10,
            }
            if next_token:
                request["NextToken"] = next_token
            try:
                response = client.get_spot_placement_scores(**request)
            except Exception as exc:  # noqa: BLE001
                # Deliberately total - see the note in `_zone_id_to_letter`. Notably this must catch
                # botocore.parsers.ResponseParserError (a bare Exception subclass), or an endpoint that does
                # not implement GetSpotPlacementScores aborts provisioning instead of degrading.
                code = exc.response.get("Error", {}).get("Code") if isinstance(exc, ClientError) else None
                if code in _PERMISSION_ERROR_CODES:
                    LOGGER.warning(
                        "Spot placement scores unavailable (%s): the ec2:GetSpotPlacementScores permission is "
                        "missing. Falling back to the default placement order.",
                        code,
                    )
                else:
                    LOGGER.warning("Spot placement scores unavailable (%s): %s", code, exc)
                return []
            raw_scores.extend(response.get("SpotPlacementScores", []))
            if not (next_token := response.get("NextToken")):
                break
    return raw_scores


@cached(
    cache=TTLCache(maxsize=64, ttl=SPOT_PLACEMENT_SCORE_CACHE_TTL),
    key=lambda instance_types, target_capacity, regions, single_az=True: hashkey(
        tuple(sorted(instance_types)), target_capacity, tuple(sorted(regions)), single_az
    ),
)
def get_scores(
    instance_types: list[str], target_capacity: int, regions: list[str], single_az: bool = True
) -> list[PlacementScore]:
    """Score `regions` (or their AZs when `single_az`) for placing `target_capacity` spot instances.

    Results are ordered best-first. Returns [] when scores cannot be obtained for ANY reason, which every
    caller must treat as "keep the existing order".

    The outer guard makes that contract independent of the internals: individual steps handle their own errors,
    but a single unhandled surprise here would abort provisioning on every AWS run, since scoring is enabled by
    default and `AZResolver.resolve()` sits on every provisioning path.
    """
    try:
        return _get_scores(
            instance_types=instance_types,
            target_capacity=target_capacity,
            regions=regions,
            single_az=single_az,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Spot placement scores unavailable (unexpected %s): %s", type(exc).__name__, exc)
        return []


def _get_scores(
    instance_types: list[str], target_capacity: int, regions: list[str], single_az: bool = True
) -> list[PlacementScore]:
    if not instance_types or not regions or target_capacity < 1:
        return []

    if len(instance_types) < SPOT_PLACEMENT_SCORE_RECOMMENDED_TYPES:
        LOGGER.info(
            "Spot placement scores requested for only %d instance type(s) %s. AWS returns structurally low "
            "scores below %d types, so treat these as a relative ranking only, not as absolute capacity.",
            len(instance_types),
            instance_types,
            SPOT_PLACEMENT_SCORE_RECOMMENDED_TYPES,
        )

    raw_scores = _query_scores(
        instance_types=instance_types, target_capacity=target_capacity, regions=regions, single_az=single_az
    )
    if not raw_scores:
        return []

    zone_letters: dict[str, dict[str, str]] = {}
    scores = []
    for entry in raw_scores:
        region = entry.get("Region")
        score = entry.get("Score")
        if not region or score is None:
            continue
        az_letter = None
        if single_az:
            if (zone_id := entry.get("AvailabilityZoneId")) is None:
                continue
            if region not in zone_letters:
                zone_letters[region] = _zone_id_to_letter(region)
            if (az_letter := zone_letters[region].get(zone_id)) is None:
                LOGGER.warning("Spot placement scores: no AZ letter for %s in %s; ignoring", zone_id, region)
                continue
        scores.append(PlacementScore(region=region, score=score, az_letter=az_letter))

    # Sort best-first, tie-broken by location so the order is deterministic run to run.
    scores.sort(key=lambda item: (-item.score, item.location))
    LOGGER.info(
        "Spot placement scores for %s (target capacity %d): %s",
        instance_types,
        target_capacity,
        ", ".join(f"{item.location}={item.score}" for item in scores),
    )
    return scores


def rank_az_letters(
    instance_types: list[str], target_capacity: int, region: str, az_letters: list[str], min_score: int = 0
) -> list[str]:
    """Reorder `az_letters` in `region` best-first by spot placement score.

    Unscored letters keep their relative order and go last - a letter absent from the response was outside the
    top 10, not proven bad. Letters scoring below `min_score` are dropped; `min_score=0` (the default) never
    drops anything, matching the API's "recommendation only" contract.
    """
    if not az_letters:
        return []
    scores = get_scores(instance_types=instance_types, target_capacity=target_capacity, regions=[region])
    if not scores:
        return list(az_letters)

    by_letter = {item.az_letter: item.score for item in scores if item.az_letter}
    scored = [letter for letter in az_letters if letter in by_letter and by_letter[letter] >= min_score]
    dropped = [letter for letter in az_letters if letter in by_letter and by_letter[letter] < min_score]
    unscored = [letter for letter in az_letters if letter not in by_letter]

    scored.sort(key=lambda letter: (-by_letter[letter], letter))
    if dropped:
        LOGGER.info("Spot placement scores: dropping AZ(s) %s in %s scoring below %d", dropped, region, min_score)
    if unscored:
        LOGGER.debug("Spot placement scores: no score for AZ(s) %s in %s; keeping them last", unscored, region)
    return scored + unscored


def rank_regions(instance_types: list[str], target_capacity: int, regions: list[str]) -> list[str]:
    """Reorder `regions` best-first by region-granularity spot placement score.

    Unscored regions keep their relative order and go last.
    """
    if not regions:
        return []
    scores = get_scores(
        instance_types=instance_types, target_capacity=target_capacity, regions=regions, single_az=False
    )
    if not scores:
        return list(regions)

    by_region = {item.region: item.score for item in scores}
    scored = sorted(
        (region for region in regions if region in by_region),
        key=lambda region: (-by_region[region], region),
    )
    unscored = [region for region in regions if region not in by_region]
    return scored + unscored
