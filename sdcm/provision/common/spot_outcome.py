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

"""Record what provision type a cluster actually got, on every backend (SCT-850, SCT-896).

Every backend falls back to on-demand when spot capacity is unavailable, and every backend used to do it
silently: a run that asked for spot and quietly paid on-demand prices looked identical to one that got what it
asked for. That makes the saving the spot default exists for unmeasurable and the downgrade rate invisible.

SCT has three provisioning paths and they cannot share a call site:

- `ProvisionPlan` - the AWS upfront `provision-resources` step.
- `cluster_aws.fallback_provision_type()` - the legacy AWS in-test path (`add_nodes`, artifact tests).
- `provision_instances_with_fallback()` - the `Provisioner` ABC path shared by GCE, Azure and OCI.

They do share the *reporting*, which is what lives here. Recording it in one place is also what keeps the three
consistent: the severity rules, the no-events-device handling and the Argus handoff are easy to get subtly
different three times over, and a metric that means different things per backend is not a metric.
"""

import logging

from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events.system import SpotProvisionOutcomeEvent
from sdcm.test_config import TestConfig

LOGGER = logging.getLogger(__name__)


def record_spot_provision_outcome(
    requested: str,
    realized: str | None,
    region: str | None = None,
    availability_zone: str | None = None,
    instance_type: str | None = None,
    count: int = 0,
) -> None:
    """Record requested vs realized provision type. `realized=None` means provisioning failed outright.

    Never raises. This sits on the hot path of every provisioning attempt and every `add_nodes`; a reporting
    bug must not be able to fail node creation.
    """
    try:
        _record(
            requested=requested,
            realized=realized,
            region=region,
            availability_zone=availability_zone,
            instance_type=instance_type,
            count=count,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to record spot provisioning outcome: %s", exc)


def _record(
    requested: str,
    realized: str | None,
    region: str | None,
    availability_zone: str | None,
    instance_type: str | None,
    count: int,
) -> None:
    event = SpotProvisionOutcomeEvent(
        requested=requested,
        realized=realized,
        region=region,
        availability_zone=availability_zone,
        instance_type=instance_type,
        count=count,
    )
    # Log unconditionally, at a level matching the outcome. The upfront path runs inside
    # `sct.py provision-resources`, a separate process that never starts an events device - there
    # `publish_or_dump(default_logger=...)` would print this NORMAL outcome as an ERROR line and nothing would
    # reach events.log or Argus. The log line is the record that always survives.
    log = LOGGER.warning if event.downgraded or realized is None else LOGGER.info
    log("Spot provisioning outcome: %s", _message(event))

    # ...and still emit a real event when a device does exist (the in-test paths). Checked rather than relying
    # on publish_or_dump's fallback, which warns once per cluster in the provision-resources process where
    # there is legitimately no device.
    try:
        has_device = get_events_main_device() is not None
    except RuntimeError:
        has_device = False
    if has_device:
        event.publish_or_dump(warn_not_ready=False)
        return

    # Without this the event dies still flagged ready-to-publish and SctEvent.__del__ warns "has not been
    # published or dumped" once per cluster. The outcome is already in the log above.
    event.dont_publish()
    # A log line alone is not measurable - the whole point of this event is to make the spot-vs-on-demand split
    # reportable. Stash it for sct.py to submit to Argus once provisioning finishes; doing it here would mean
    # initialising an Argus client inside the provisioning hot path, where a network failure could take the run
    # down with it.
    TestConfig.SPOT_PROVISION_OUTCOMES.append(
        {
            "requested": event.requested,
            "realized": event.realized,
            "downgraded": event.downgraded,
            "region": event.region,
            "availability_zone": event.availability_zone,
            "instance_type": event.instance_type,
            "count": event.count,
            "severity": event.severity.name,
            "message": _message(event),
        }
    )


def _message(event: SpotProvisionOutcomeEvent) -> str:
    """The field text only - `str(event)` already carries an event-type/severity prefix, and `sct.py` adds its
    own when building the Argus payload, so passing the full rendering double-prefixes it."""
    return event.msgfmt.format(event).split(": ", 1)[-1]
