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

"""Rank GCE zones and regions by Capacity Advisor obtainability (`compute.beta advice.capacity`).

The GCE counterpart to `sdcm/provision/aws/spot_placement_score.py`, and deliberately the same shape: an
ordering signal, never a veto, that degrades to today's behaviour the moment anything goes wrong.

Three things about the API drive the design:

- **It is beta-only.** `advice.capacity` and `advice.capacityHistory` exist in the `compute/beta` discovery
  document and not in `compute/v1`, and the typed `google-cloud-compute` SDK exposes only `calendar_mode` from
  the `advice` resource (checked in 1.48.0 and 1.50.0), so the typed client is a dead end regardless of
  version. The discovery client reaches it and is already a dependency - SCT uses the same `build(...)` pattern
  for `iam/v1` and `logging/v2`.
- **Obtainability is a probability, not a promise.** GCP documents it as 0.0-1.0, "the likelihood of
  successfully obtaining (provisioning) the requested number of VMs". Like the AWS placement score it is a
  recommendation, so it only reorders candidates that already passed the machine-type offerings filter.
- **A response describes one balanced recommendation, not every zone.** `recommendations[].shards[]` reports
  which zones GCP would pick, which cannot rank the zones we did not get back. So each zone is scored on its
  own, with `distributionPolicy.zones` pinned to it and `targetShape: ANY_SINGLE_ZONE` - the direct analogue of
  passing `SingleAvailabilityZone=True` to the AWS API.

`estimatedUptime` is returned alongside and caps at 3600s: GCP does not predict more than an hour of Spot
runtime for any request. That is worth surfacing in the log even though it does not drive the ordering,
because it is the clearest argument for keeping GCE spot on shorter tests.

The API is in Preview, so its contract may change. Everything here therefore degrades to an empty result on
any error, which callers must treat as "keep the existing order".
"""

import logging
from dataclasses import dataclass

from cachetools import TTLCache
from cachetools.keys import hashkey
from google.oauth2 import service_account
from googleapiclient.discovery import build

from sdcm.keystore import KeyStore
from sdcm.provision.gce.constants import (
    GCE_CAPACITY_ADVICE_CACHE_TTL,
    GCE_CAPACITY_ADVICE_API_VERSION,
)
from sdcm.utils.gce_utils import _gce_client_options

LOGGER = logging.getLogger(__name__)


class _AdviceUnavailableError(Exception):
    """The API could not answer (permissions, quota, transport, a Preview contract change).

    Distinct from "the API answered with nothing": this one must never be cached, or a single blip leaves
    every provisioning attempt unranked for a whole TTL.
    """


@dataclass(frozen=True)
class ZoneCapacityAdvice:
    """Capacity Advisor's verdict for placing a request in one zone."""

    zone: str
    obtainability: float
    estimated_uptime_seconds: int | None = None

    @property
    def zone_letter(self) -> str:
        """`us-east1-b` -> `b`. SCT config works in zone letters, the API in full zone names."""
        return self.zone.rsplit("-", 1)[-1]


def _advice_service():
    """Build a `compute/beta` discovery client. `compute/v1` does not expose the advice resource at all."""
    info = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(info)
    service = build(
        "compute",
        GCE_CAPACITY_ADVICE_API_VERSION,
        credentials=credentials,
        cache_discovery=False,
        **_gce_client_options(),
    )
    return service, info["project_id"]


def _build_request_body(machine_types: list[str], size: int, zone: str, provisioning_model: str) -> dict:
    """One zone, one shape: `ANY_SINGLE_ZONE` over a single pinned zone makes the scores comparable.

    Machine types go in as an `instanceSelections` entry, the API's way of saying "any of these will do" -
    the equivalent of passing several `InstanceTypes` to the AWS API, and worth doing for the same reason:
    a request for a single type is scored more pessimistically than a diversified one.
    """
    return {
        "size": size,
        "instanceProperties": {"scheduling": {"provisioningModel": provisioning_model}},
        "instanceFlexibilityPolicy": {"instanceSelections": {"sct": {"machineTypes": machine_types}}},
        # `zone` is a resource *reference*, not a name: a bare "us-east1-b" comes back as
        # "The URL is malformed". The partial form is enough and keeps the project out of the body.
        "distributionPolicy": {"zones": [{"zone": f"zones/{zone}"}], "targetShape": "ANY_SINGLE_ZONE"},
    }


def _parse_duration_seconds(value) -> int | None:
    """`estimatedUptime` is a google-duration string such as `"3600s"`."""
    if not value:
        return None
    try:
        return int(str(value).rstrip("s"))
    except ValueError:
        return None


def _query_zone(service, project: str, region: str, body: dict, zone: str) -> ZoneCapacityAdvice | None:
    try:
        response = service.advice().capacity(project=project, region=region, body=body).execute()
    except Exception as exc:  # noqa: BLE001
        # Deliberately total, for the same reason as the AWS module: the surface is wider than it looks -
        # googleapiclient.errors.HttpError for API-level problems (403 when `compute.advice.capacity` is
        # missing from the service account's role), but also transport errors, refresh failures and, on a
        # Preview API, a response shape we did not expect.
        raise _AdviceUnavailableError(f"{type(exc).__name__}: {exc}") from exc

    recommendations = response.get("recommendations") or []
    if not recommendations:
        return None
    scores = recommendations[0].get("scores") or {}
    obtainability = scores.get("obtainability")
    if obtainability is None:
        return None
    return ZoneCapacityAdvice(
        zone=zone,
        obtainability=float(obtainability),
        estimated_uptime_seconds=_parse_duration_seconds(scores.get("estimatedUptime")),
    )


def _unavailable_hint(message: str) -> str:
    """Point at the actual cause: the three failure modes read nothing alike and misdirect badly.

    "The service is not available for this project" is the Preview enrolment error, not a permission one -
    and enrolment is per provisioning model, so SPOT can work while STANDARD does not. Saying "check the
    permission" there sends whoever reads it to an IAM console that is already correct.
    """
    if "not available for this project" in message:
        return (
            "This is the Preview enrolment error, not a permission one: advice.capacity is not enabled for "
            "this project and provisioning model. Note enrolment is per model - SPOT can work while "
            "STANDARD (on-demand) does not."
        )
    if "not supported in locations" in message:
        return "The machine type is not offered in one of the zones asked about; the rest are still ranked."
    return "If this is a permission error, the service account needs compute.advice.capacity (in roles/compute.viewer)."


_advice_cache = TTLCache(maxsize=64, ttl=GCE_CAPACITY_ADVICE_CACHE_TTL)


def get_zone_advice(
    machine_types: list[str],
    size: int,
    region: str,
    zones: list[str],
    provisioning_model: str = "SPOT",
) -> list[ZoneCapacityAdvice]:
    """Score each of `zones` for placing `size` instances. Best-first; [] when unavailable for ANY reason.

    Only a non-empty answer is cached. Caching the `[]` returned on failure would turn one throttled or
    briefly-unauthorized call into a whole TTL of unranked provisioning, which is exactly when the advice is
    worth having; an empty-but-successful answer is cheap enough to ask again.
    """
    if not machine_types or not zones or size < 1:
        return []

    key = hashkey(tuple(sorted(machine_types)), size, region, tuple(sorted(zones)), provisioning_model)
    if (cached := _advice_cache.get(key)) is not None:
        return cached

    try:
        advice = _collect_zone_advice(machine_types, size, region, zones, provisioning_model)
    except _AdviceUnavailableError as exc:
        LOGGER.warning(
            "GCE capacity advice unavailable in %s (%s). Falling back to the default zone order. %s",
            region,
            exc,
            _unavailable_hint(str(exc)),
        )
        return []
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("GCE capacity advice unavailable (unexpected %s): %s", type(exc).__name__, exc)
        return []

    if advice:
        _advice_cache[key] = advice
    return advice


get_zone_advice.cache_clear = _advice_cache.clear


def _collect_zone_advice(
    machine_types: list[str], size: int, region: str, zones: list[str], provisioning_model: str
) -> list[ZoneCapacityAdvice]:
    service, project = _advice_service()
    results = []
    failures = []
    with service:
        for zone in zones:
            body = _build_request_body(machine_types, size, zone, provisioning_model)
            try:
                if (advice := _query_zone(service, project, region, body, zone)) is not None:
                    results.append(advice)
            except _AdviceUnavailableError as exc:
                # One zone failing must not cost us the other zones' scores. GCP rejects the whole request
                # with "Machine specification is not supported in locations: [us-west1-c]" when a single zone
                # cannot host the machine type, and ranking the zones that CAN is exactly what we are here
                # for. A cause that affects every zone - a missing permission, a throttle - fails all of them
                # anyway and is reported below, at the cost of one call per zone rather than one per region.
                LOGGER.debug("GCE capacity advice: no answer for zone %s (%s)", zone, exc)
                failures.append(exc)
    if not results and failures:
        raise failures[0]

    # Best-first, tie-broken by zone name so the order is deterministic run to run.
    results.sort(key=lambda item: (-item.obtainability, item.zone))
    if results:
        LOGGER.debug(
            "GCE capacity advice for %s (%d x %s): %s",
            region,
            size,
            machine_types,
            ", ".join(f"{item.zone}={item.obtainability:.2f}" for item in results),
        )
        _log_uptime_caveat(results)
    return results


def _log_uptime_caveat(results: list[ZoneCapacityAdvice]) -> None:
    """`estimatedUptime` caps at 3600s - GCP does not predict more than an hour of Spot runtime.

    It does not affect the ordering (every zone tends to hit the same cap), but it is the clearest signal that
    long GCE tests do not belong on spot, so it is worth having in the log next to the scores.
    """
    uptimes = [item.estimated_uptime_seconds for item in results if item.estimated_uptime_seconds is not None]
    if uptimes:
        LOGGER.debug(
            "GCE capacity advice: estimated Spot uptime %d-%ds (GCP caps this estimate at 3600s)",
            min(uptimes),
            max(uptimes),
        )


def rank_zone_letters_for_roles(
    role_requests: list[tuple[str, list[str], int]],
    region: str,
    zone_letters: list[str],
    min_obtainability: float = 0.0,
) -> list[str] | None:
    """Reorder `zone_letters` by the WORST obtainability across every role the cluster needs in that zone.

    `role_requests` is (role name, machine types, count) per role - DB, loaders, monitor - each asked in its
    own call. Merging them would defeat the purpose: `instanceSelections` means "any of these will do", so a
    small plentiful monitor type would answer for the whole request.

    The minimum is the right summary because the cluster needs *all* of these types in one zone, so the zone
    is worth what its scarcest role is worth. This is not hypothetical: a real run picked `us-east1-b` on a
    DB score of 0.90 and then failed to create a loader there - the loader type scored 0.10 in that zone and
    0.90 in `us-east1-d`, which the DB-only ranking could not see.
    """
    if not zone_letters:
        return []
    worst: dict[str, float] = {}
    for role, machine_types, count in role_requests:
        if not machine_types or count < 1:
            continue
        zones = [f"{region}-{letter}" for letter in zone_letters]
        advice = get_zone_advice(machine_types=machine_types, size=count, region=region, zones=zones)
        if not advice:
            return None  # one unscored role makes every minimum a guess; keep the existing order instead
        by_letter = {item.zone_letter: item.obtainability for item in advice}
        LOGGER.info(
            "GCE capacity advice %s: %s x%d in %s -> %s",
            role,
            ",".join(machine_types),
            count,
            region,
            " ".join(f"{letter}={by_letter[letter]:.2f}" for letter in sorted(by_letter)),
        )
        for letter, score in by_letter.items():
            worst[letter] = min(worst.get(letter, score), score)
    if not worst:
        return None

    scored = [letter for letter in zone_letters if letter in worst and worst[letter] >= min_obtainability]
    dropped = [letter for letter in zone_letters if letter in worst and worst[letter] < min_obtainability]
    unscored = [letter for letter in zone_letters if letter not in worst]

    scored.sort(key=lambda letter: (-worst[letter], letter))
    if dropped:
        LOGGER.info(
            "GCE capacity advice: dropping zone(s) %s in %s below obtainability %.2f",
            dropped,
            region,
            min_obtainability,
        )
    if unscored:
        LOGGER.debug("GCE capacity advice: no score for zone(s) %s in %s; keeping them last", unscored, region)
    return scored + unscored


def rank_regions(
    machine_types: list[str],
    size: int,
    regions: list[str],
    zones_per_region: dict[str, list[str]],
) -> list[str] | None:
    """Order whole `regions` best-first by their best zone's obtainability.

    A region is worth as much as the best zone we could actually land in, so the maximum - not the mean - is
    the right summary: a region with one excellent zone and two poor ones is a good place to provision a
    single-zone cluster. Regions with no advice keep their relative order and go last.
    """
    if not regions:
        return []
    best: dict[str, float] = {}
    for region in regions:
        zones = zones_per_region.get(region) or []
        if not zones:
            continue
        if advice := get_zone_advice(machine_types=machine_types, size=size, region=region, zones=zones):
            best[region] = max(item.obtainability for item in advice)
    if not best:
        return None

    scored = sorted((region for region in regions if region in best), key=lambda r: (-best[r], r))
    unscored = [region for region in regions if region not in best]
    LOGGER.debug("GCE capacity advice by region: %s", ", ".join(f"{region}={best[region]:.2f}" for region in scored))
    return scored + unscored
