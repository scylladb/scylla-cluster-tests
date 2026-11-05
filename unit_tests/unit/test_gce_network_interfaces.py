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

"""Unit tests for GCE networking: the Jenkins builder firewall."""

from unittest.mock import MagicMock, patch

import google.api_core.exceptions
from google.cloud import compute_v1
from google.cloud.compute_v1 import Firewall

from sdcm.utils.gce_region import BUILDER_NETWORK_TAG, GceRegion, firewall_needs_patch

PROJECT_ID = "sct-project"


def new_region(region_name: str | None = None) -> GceRegion:
    region = GceRegion.__new__(GceRegion)
    if region_name is not None:
        region.region_name = region_name
    return region


BUILDER_SOURCES = {"jenkins": ["192.0.2.10/32"], "office": ["198.51.100.10/32"]}


def _rule(**overrides):
    base = dict(
        name="r",
        direction="INGRESS",
        priority=100,
        source_ranges=["1.2.3.4/32"],
        allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["22"])],
        target_tags=["sct-builder"],
    )
    fields = base | overrides
    return Firewall(**{key: value for key, value in fields.items() if value is not None})


def test_firewall_needs_patch_detects_priority_and_target_tags():
    assert not firewall_needs_patch(_rule(), _rule())
    assert firewall_needs_patch(_rule(priority=1000), _rule())
    assert firewall_needs_patch(_rule(target_tags=[]), _rule())
    assert firewall_needs_patch(_rule(allowed=[], denied=[compute_v1.Denied(I_p_protocol="all")]), _rule())
    # a live rule at the API default must equal a desired rule that never set a priority
    assert not firewall_needs_patch(_rule(priority=1000), _rule(priority=None))


def _firewall_region():
    region = new_region()
    region.project = PROJECT_ID
    region.firewall_client = MagicMock()
    region.__dict__["network"] = MagicMock(self_link="net-link")
    return region


def test_builder_firewall_applies_allows_before_deny_and_waits():
    region = _firewall_region()
    with patch("sdcm.utils.gce_region.wait_for_extended_operation") as wait:
        region.configure_builder_firewall(BUILDER_SOURCES)

    inserted = [call.kwargs["firewall_resource"] for call in region.firewall_client.insert.call_args_list]
    # without the tag, the deny rule would cut off every host in the project
    assert all(list(rule.target_tags) == [BUILDER_NETWORK_TAG] for rule in inserted)
    # a deny rule without its allow rules would lock Jenkins out of new builders
    assert inserted[-1].name == "qa-vpc-builder-deny-all"
    # an untagged allow rule at priority 100 (qa-vpc-allow-ssh in one project) must not beat the deny rule
    assert max(rule.priority for rule in inserted[:-1]) < inserted[-1].priority < 100
    waited = [call.args[0] for call in wait.call_args_list]
    assert waited.count(region.firewall_client.insert.return_value) == 3


def test_stale_ssh_rule_kept_when_its_replacement_is_missing():
    region = _firewall_region()
    region.firewall_client.get.side_effect = google.api_core.exceptions.NotFound("no such rule")

    region.delete_stale_firewall_rules()

    region.firewall_client.delete.assert_not_called()
