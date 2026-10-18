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

from datetime import datetime, timedelta

from argus.common.enums import TestStatus

#: Hard ceiling on attempts per build, independent of the configured policy. The Groovy loop enforces it too.
MAX_ATTEMPTS = 3
STOP = "stop"
DECISION_MARKER = "SPOT_RETRY_DECISION="
_RETRY_PROVISION_TYPES = ("spot", "on_demand")
#: Argus status of a reclaimed attempt that is retried. All attempts share the build number, and Argus shows a
#: build's worst status: ERROR ranks below PASSED, so the retry decides it. SCT sets ERROR nowhere else.
#: Becomes a RECLAIMED status that dashboards skip once Argus has one (ARGUS-281).
RETRIED_RUN_STATUS = TestStatus.ERROR
#: A reclaim can be reported a little after the failures it causes: the stress tools see the node go before the
#: spot monitor polls. A CRITICAL event more than this before the first reclaim is a failure of its own.
RECLAIM_GRACE = timedelta(seconds=60)


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
    """True for an Argus event row that shows the attempt was lost to a spot reclaim.

    A reclaim is a CRITICAL `SpotTerminationEvent`: the node checks publish it during the test, and
    `tester.teardown_on_exception` publishes it for a preemption during provisioning that the on-demand fallback
    did not recover. The provisioner itself publishes none, because the fallback may still save the run.
    """
    return event.get("event_type") == "SpotTerminationEvent"


def lost_to_reclaim(events: list[dict]) -> bool:
    """True when a reclaim ended the attempt: no other CRITICAL event came well before the first reclaim.

    A node reclaimed during the teardown of a run that had already failed must not get a retry, since a passing
    retry would hide the real failure. Raises on a row without a timestamp, which the caller answers with `stop`.
    """
    reclaims = [datetime.fromisoformat(event["ts"]) for event in events if is_spot_reclaim(event)]
    if not reclaims:
        return False
    cutoff = min(reclaims) - RECLAIM_GRACE
    return not any(datetime.fromisoformat(event["ts"]) < cutoff for event in events if not is_spot_reclaim(event))


def decide(policy: list[str], attempt: int, events: list[dict]) -> str:
    """Return the `instance_provision` for the next attempt, or `STOP`.

    `attempt` is the 1-based number of the attempt that just failed, `events` its CRITICAL Argus event rows.
    """
    if attempt >= MAX_ATTEMPTS or attempt > len(policy):
        return STOP
    if not lost_to_reclaim(events):
        return STOP
    return policy[attempt - 1]


def fetch_run_events(argus_client) -> list[dict]:
    """The run's CRITICAL event rows, as stored by Argus. Raises on any Argus error."""
    response = argus_client.get(
        endpoint="/sct/$id/events/CRITICAL/get",
        location_params={"id": str(argus_client.run_id)},
        params={"limit": 1000},
    )
    argus_client.check_response(response)
    return response.json()["response"]
