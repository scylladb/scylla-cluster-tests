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

from sdcm.provision.gce.capacity_errors import (
    is_config_error,
    is_zone_capacity_error,
    is_quota_error,
    is_type_unavailable_error,
    classify_provisioning_error,
)


class _FakeError(Exception):
    pass


@pytest.mark.parametrize(
    "message,expected",
    [
        ("ZONE_RESOURCE_POOL_EXHAUSTED", True),
        ("Zone us-east1-b resource pool exhausted: ZONE_RESOURCE_POOL_EXHAUSTED", True),
        ("RESOURCE_POOL_EXHAUSTED in us-east1-c", True),
        ("stockout detected", True),
        ("PERMISSION_DENIED: cannot access", False),
        ("QUOTA_EXCEEDED", False),
        ("", False),
        # GCE returns the capacity code for an unsupported machine-type/disk-type combination too;
        # it must not be classified as capacity, or the region loop relocates the cluster for nothing.
        (
            "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: Error 400: "
            "[pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance., badRequest",
            False,
        ),
    ],
)
def test_is_zone_capacity_error(message, expected):
    assert is_zone_capacity_error(_FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        # Verbatim GCE wording - "are ... for", not "is ... with".
        ("Error 400: [pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance.", True),
        ("[e2-medium, local-ssd] features are not compatible for creating instance.", True),
        ("Requested boot disk architecture (X86_64) is not compatible with machine type architecture (ARM64).", True),
        ("ZONE_RESOURCE_POOL_EXHAUSTED", False),
        ("QUOTA_EXCEEDED", False),
        ("", False),
    ],
)
def test_is_config_error(message, expected):
    assert is_config_error(_FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        ("QUOTA_EXCEEDED for cpus in us-east1", True),
        ("quotaExceeded: limit reached", True),
        ("ZONE_RESOURCE_POOL_EXHAUSTED", False),
        ("PERMISSION_DENIED", False),
    ],
)
def test_is_quota_error(message, expected):
    assert is_quota_error(_FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        ("RESOURCE_NOT_FOUND: machine type n2d-highcpu-128", True),
        ("machineTypeNotFound in zone us-east1-b", True),
        ("ZONE_RESOURCE_POOL_EXHAUSTED", False),
        ("generic error", False),
    ],
)
def test_is_type_unavailable_error(message, expected):
    assert is_type_unavailable_error(_FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected_class",
    [
        ("ZONE_RESOURCE_POOL_EXHAUSTED", "capacity"),
        (
            "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: [pd-standard, n4-standard-16] "
            "features are not compatible for creating instance.",
            "config",
        ),
        ("QUOTA_EXCEEDED", "quota"),
        ("RESOURCE_NOT_FOUND", "type_unavailable"),
        ("PERMISSION_DENIED", "unknown"),
    ],
)
def test_classify_provisioning_error(message, expected_class):
    assert classify_provisioning_error(_FakeError(message)) == expected_class


def test_non_exception_types_handled():
    assert is_zone_capacity_error(ValueError("just a value error")) is False
