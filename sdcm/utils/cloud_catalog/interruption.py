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

"""Spot interruption buckets from the public AWS Spot Instance Advisor data (SCT-707).

Buckets follow the advisor ranges: 0 = <5%, 1 = 5-10%, 2 = 10-15%, 3 = 15-20%, 4 = >20% of instances
interrupted per month. Only AWS publishes per-type data: Azure's `SpotResources` eviction-rate table returns
nothing to the SCT service principal, and GCE and OCI have no per-type signal.
"""

import logging
from functools import lru_cache

import requests

from sdcm.utils.session import create_retry_session

LOGGER = logging.getLogger(__name__)

SPOT_ADVISOR_URL = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"


# One fetch per process, with retries. If it still fails in one sct.py command and not in another, they can
# size a spot run differently; a shared on-disk cache is the upgrade if that ever shows up.
@lru_cache(maxsize=1)
def aws_spot_buckets() -> dict[str, dict[str, int]]:
    """Return {region: {instance_type: bucket}} for Linux, or {} when the advisor can't be reached."""
    try:
        response = create_retry_session().get(SPOT_ADVISOR_URL, timeout=10)
        response.raise_for_status()
        return {
            # int(): a null bucket must fall back here, not break the spot sort with a None/int comparison
            region: {instance_type: int(info["r"]) for instance_type, info in systems.get("Linux", {}).items()}
            for region, systems in response.json()["spot_advisor"].items()
        }
    except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError) as exc:
        # a changed advisor schema must not fail every AWS spot config
        LOGGER.warning("AWS Spot Instance Advisor unavailable, spot sizing keeps the normal order: %s", exc)
        return {}
