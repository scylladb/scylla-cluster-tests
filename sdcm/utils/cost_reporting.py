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

"""Send what SCT knows about a run's cost to Argus.

All the arithmetic happens in SCT: Argus stores what it is told and sums it. Argus also
stores a zero as a real figure, so an unknown cost is never sent - not as zero, and not as a
partial total that would read as the whole answer.

Every call here is advisory. A failure is logged and swallowed; cost reporting must never
change the outcome of a run.

Kept out of `sdcm.utils.argus` on purpose: that module is imported by `sdcm.test_config`,
which `sdcm.sct_config` imports, and the cost module imports `sdcm.sct_config` - so importing
cost from there would close a cycle.
"""

from __future__ import annotations

import logging

from argus.client.sct.client import ArgusSCTClient

from sdcm.utils.cloud_catalog.cost import RunCostEstimate

LOGGER = logging.getLogger(__name__)


def report_estimated_cost(client: ArgusSCTClient, estimate: RunCostEstimate) -> bool:
    """Send the run's pre-provisioning estimate. Returns whether anything was sent.

    The figure sent is the estimate at the lifecycle the run is configured for. The on-demand
    ceiling is not sent: Argus keeps one estimate per run, and it is compared against the
    actual cost, which a spot run should come close to unless it fell back.

    A partial estimate is not sent. It is a floor that leaves out whole roles, and once stored
    it would be compared against the real total as if it were the expected one.
    """
    if estimate.total is None or estimate.partial:
        LOGGER.info(
            "Not reporting the cost estimate to Argus: %s",
            f"no price for {', '.join(estimate.unpriced_roles)}" if estimate.unpriced_roles else "nothing priced",
        )
        return False
    try:
        client.set_estimated_cost(run_id=client.run_id, value=round(estimate.total, 2))
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to report the cost estimate to Argus", exc_info=True)
        return False
    LOGGER.info("Reported estimated cost $%.2f to Argus run %s", estimate.total, client.run_id)
    return True
