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
    classify_provisioning_error,
    is_config_error,
    is_preemption_error,
    is_quota_error,
    is_type_unavailable_error,
    is_zero_quota_error,
    is_zone_capacity_error,
)


class FakeError(Exception):
    """Exception carrying a GCE error message."""


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", True, id="exact-code"),
        pytest.param(
            "Zone us-east1-b resource pool exhausted: ZONE_RESOURCE_POOL_EXHAUSTED", True, id="wrapped-in-message"
        ),
        pytest.param("RESOURCE_POOL_EXHAUSTED in us-east1-c", True, id="pool-exhausted-in-zone"),
        pytest.param("stockout detected", True, id="stockout-text"),
        pytest.param("PERMISSION_DENIED: cannot access", False, id="permission-denied"),
        pytest.param("QUOTA_EXCEEDED", False, id="quota-not-capacity"),
        pytest.param("", False, id="empty"),
        # GCE returns the capacity code for an unsupported machine-type/disk-type combination too;
        # it must not be classified as capacity, or the region loop relocates the cluster for nothing.
        pytest.param(
            "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: Error 400: "
            "[pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance., badRequest",
            False,
            id="config-with-capacity-code",
        ),
    ],
)
def test_is_zone_capacity_error(message, expected):
    """is_zone_capacity_error matches GCE capacity codes, not config or quota errors."""
    assert is_zone_capacity_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        # Verbatim GCE wording - "are ... for", not "is ... with".
        pytest.param(
            "Error 400: [pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance.",
            True,
            id="verbatim-feature-incompatible",
        ),
        pytest.param(
            "[e2-medium, local-ssd] features are not compatible for creating instance.", True, id="local-ssd-on-e2"
        ),
        pytest.param(
            "Requested boot disk architecture (X86_64) is not compatible with machine type architecture (ARM64).",
            True,
            id="architecture-mismatch",
        ),
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", False, id="capacity-not-config"),
        pytest.param("QUOTA_EXCEEDED", False, id="quota-not-config"),
        pytest.param("", False, id="empty"),
    ],
)
def test_is_config_error(message, expected):
    """is_config_error matches GCE feature and architecture incompatibility messages only."""
    assert is_config_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param("QUOTA_EXCEEDED for cpus in us-east1", True, id="quota-exceeded-code"),
        pytest.param("quotaExceeded: limit reached", True, id="quota-exceeded-camel-case"),
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", False, id="capacity-not-quota"),
        pytest.param("PERMISSION_DENIED", False, id="permission-denied"),
    ],
)
def test_is_quota_error(message, expected):
    """is_quota_error matches QUOTA_EXCEEDED messages only."""
    assert is_quota_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param(
            "403 QUOTA_EXCEEDED: Quota 'CPUS_PER_VM_FAMILY' exceeded.  Limit: 0.0 in region us-central1.",
            True,
            id="cpus-family-zero-limit",
        ),
        pytest.param(
            "QUOTA_EXCEEDED: Quota 'LOCAL_SSD_TOTAL_GB_PER_VM_FAMILY' exceeded.  Limit: 0 in region us-east1.",
            True,
            id="local-ssd-zero-limit",
        ),
        pytest.param(
            "QUOTA_EXCEEDED: Quota 'CPUS_PER_VM_FAMILY' exceeded.  Limit: 24.0 in region us-central1.",
            False,
            id="cpus-family-nonzero-limit",
        ),
        pytest.param(
            "QUOTA_EXCEEDED: Quota 'CPUS' exceeded.  Limit: 0.5 in region us-central1.",
            False,
            id="global-cpus-fractional-limit",
        ),
        pytest.param(
            "QUOTA_EXCEEDED: Quota 'CPUS' exceeded.  Limit: 100.0 in region us-central1.",
            False,
            id="global-cpus-high-limit",
        ),
        pytest.param("QUOTA_EXCEEDED for cpus in us-east1", False, id="quota-without-limit"),
        # A zero limit without a quota marker is not a quota error at all
        pytest.param("Limit: 0.0", False, id="zero-limit-without-marker"),
    ],
)
def test_is_zero_quota_error(message, expected):
    """is_zero_quota_error matches quota errors whose limit is zero."""
    assert is_zero_quota_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        pytest.param("RESOURCE_NOT_FOUND: machine type n2d-highcpu-128", True, id="resource-not-found"),
        pytest.param("machineTypeNotFound in zone us-east1-b", True, id="machine-type-not-found-camel-case"),
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", False, id="capacity-not-unavailable"),
        pytest.param("generic error", False, id="generic-error"),
    ],
)
def test_is_type_unavailable_error(message, expected):
    """is_type_unavailable_error matches missing machine type messages only."""
    assert is_type_unavailable_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected",
    [
        # Verbatim GCE operation error for a spot VM reclaimed before it finished booting.
        pytest.param("Instance failed to start due to preemption.", True, id="failed-to-start-preemption"),
        pytest.param("Code: PREEMPTED", True, id="preempted-code"),
        # Capacity exhaustion is about the zone, preemption is about the pricing model - keep them apart.
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", False, id="capacity-not-preemption"),
        pytest.param("QUOTA_EXCEEDED", False, id="quota-not-preemption"),
        pytest.param("", False, id="empty"),
    ],
)
def test_is_preemption_error(message, expected):
    """is_preemption_error matches spot VM preemption messages only."""
    assert is_preemption_error(FakeError(message)) is expected


@pytest.mark.parametrize(
    "message,expected_class",
    [
        pytest.param("ZONE_RESOURCE_POOL_EXHAUSTED", "capacity", id="capacity"),
        pytest.param(
            "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS: [pd-standard, n4-standard-16] "
            "features are not compatible for creating instance.",
            "config",
            id="config-over-capacity-code",
        ),
        pytest.param("Instance failed to start due to preemption.", "preemption", id="preemption"),
        pytest.param("QUOTA_EXCEEDED", "quota", id="quota"),
        pytest.param("RESOURCE_NOT_FOUND", "type_unavailable", id="type-unavailable"),
        pytest.param("PERMISSION_DENIED", "unknown", id="unknown"),
    ],
)
def test_classify_provisioning_error(message, expected_class):
    """classify_provisioning_error maps each GCE error message to its category."""
    assert classify_provisioning_error(FakeError(message)) == expected_class


def test_non_exception_types_handled():
    """is_zone_capacity_error returns False for a plain ValueError."""
    assert is_zone_capacity_error(ValueError("just a value error")) is False
