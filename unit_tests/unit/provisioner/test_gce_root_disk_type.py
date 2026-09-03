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

import pytest

from sdcm.provision.gce.constants import (
    DISK_TYPE_HYPERDISK_BALANCED,
)
from sdcm.provision.gce.instance_provider import resolve_root_disk_type as _resolve_root_disk_type


@pytest.mark.parametrize(
    "instance_type",
    ["n4a-standard-4", "n4a-highcpu-8", "n4-standard-4", "c4a-standard-4", "z3-highmem-8-highlssd"],
)
@pytest.mark.parametrize("configured", ["pd-standard", "pd-balanced", "pd-ssd", None])
def test_hyperdisk_only_families_override_the_configured_root_disk_type(instance_type, configured):
    assert _resolve_root_disk_type(instance_type, configured) == DISK_TYPE_HYPERDISK_BALANCED


@pytest.mark.parametrize(
    "instance_type, configured, expected",
    [
        ("e2-standard-8", "pd-standard", "pd-standard"),
        ("e2-standard-8", None, "pd-standard"),
        ("n2-standard-8", "pd-ssd", "pd-ssd"),
        ("n2d-standard-8", "pd-balanced", "pd-balanced"),
    ],
)
def test_other_families_keep_the_configured_root_disk_type(instance_type, configured, expected):
    assert _resolve_root_disk_type(instance_type, configured) == expected
