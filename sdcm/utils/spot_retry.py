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

"""Decide whether a test attempt lost to a spot reclaim gets another attempt (SCT-707).

The Jenkins side (`vars/runWithSpotRetry.groovy`) only runs the loop; every rule lives here so it is
unit-tested. The decision is printed as one marker line that the pipeline greps out of hydra's output.
"""

import logging

LOGGER = logging.getLogger(__name__)

#: Hard ceiling on attempts per build, independent of the configured policy. The Groovy loop enforces it too.
MAX_ATTEMPTS = 3
STOP = "stop"
DECISION_MARKER = "SPOT_RETRY_DECISION="
_RETRY_PROVISION_TYPES = ("spot", "on_demand")
#: Severities fetched from Argus. A reclaim during the test is a CRITICAL `SpotTerminationEvent`; a reclaim
#: during provisioning with no on-demand fallback surfaces as an `OperationPreemptedError` in the ERROR or
#: CRITICAL event that fails the run. The provisioner deliberately publishes no event of its own there, because
#: `provision_instances_with_fallback` may still recover the run on on-demand.
EVENT_SEVERITIES = ("CRITICAL", "ERROR")


def validate_policy(policy: list[str]) -> None:
    """Reject a `spot_termination_retry` value that could retry forever or end on a reclaimable attempt."""
    if not policy:
        return
    if len(policy) > MAX_ATTEMPTS - 1:
        raise ValueError(
            f"spot_termination_retry allows at most {MAX_ATTEMPTS - 1} retries ({MAX_ATTEMPTS} attempts in total), "
            f"got {policy}"
        )
    if unknown := [value for value in policy if value not in _RETRY_PROVISION_TYPES]:
        raise ValueError(f"spot_termination_retry entries must be one of {_RETRY_PROVISION_TYPES}, got {unknown}")
    if policy[-1] != "on_demand":
        raise ValueError(
            f"spot_termination_retry must end with 'on_demand', so the last attempt cannot be reclaimed; got {policy}"
        )


def is_spot_reclaim(event: dict) -> bool:
    """True for an Argus event row that shows the attempt was lost to a spot reclaim."""
    return event.get("event_type") == "SpotTerminationEvent" or "OperationPreemptedError" in (
        event.get("message") or ""
    )


def decide(policy: list[str], attempt: int, events: list[dict]) -> str:
    """Return the `instance_provision` for the next attempt, or `STOP`.

    `attempt` is the 1-based number of the attempt that just failed, `events` its Argus event rows.
    """
    if attempt >= MAX_ATTEMPTS or attempt > len(policy):
        return STOP
    if not any(is_spot_reclaim(event) for event in events):
        return STOP
    return policy[attempt - 1]


def fetch_run_events(argus_client) -> list[dict]:
    """The run's CRITICAL and ERROR event rows, as stored by Argus. Raises on any Argus error."""
    events = []
    for severity in EVENT_SEVERITIES:
        response = argus_client.get(
            endpoint="/sct/$id/events/$severity/get",
            location_params={"id": str(argus_client.run_id), "severity": severity},
            params={"limit": 1000},
        )
        argus_client.check_response(response)
        events.extend(response.json()["response"])
    return events
