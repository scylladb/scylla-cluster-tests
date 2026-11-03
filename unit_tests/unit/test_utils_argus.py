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

from sdcm.utils.argus import is_test_error

EVENT_HEADER = "2026-01-01 10:00:00.000: (TestFrameworkEvent Severity.ERROR) period_type=one-time event_id=1234"


@pytest.mark.parametrize(
    "events",
    [
        pytest.param(
            {"CRITICAL": [], "ERROR": ["2026-01-01 10:00:00.000: (SpotTerminationEvent Severity.CRITICAL) node=n1"]},
            id="spot-termination-in-error",
        ),
        pytest.param(
            {"CRITICAL": ["2026-01-01 10:00:00.000: (SpotTerminationEvent Severity.CRITICAL) node=n1"], "ERROR": []},
            id="spot-termination-in-critical",
        ),
        pytest.param(
            {"CRITICAL": [], "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp() message=ZonalAllocationFailed in zone"]},
            id="zonal-allocation-failed",
        ),
        pytest.param(
            {
                "CRITICAL": [],
                "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp()\nexception=403 FORBIDDEN QUOTA_EXCEEDED: no quota"],
            },
            id="quota-exceeded-two-line-setup",
        ),
        pytest.param(
            {
                "CRITICAL": [],
                "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp()\nClientError: InsufficientInstanceCapacity"],
            },
            id="insufficient-instance-capacity-two-line-setup",
        ),
        pytest.param(
            {"CRITICAL": [], "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp() message=InvalidInstanceType: x1.huge"]},
            id="invalid-instance-type",
        ),
        pytest.param(
            {
                "CRITICAL": [],
                "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp()\nmachine type m9 is not supported in region eu-west-9"],
            },
            id="not-supported-in-region",
        ),
        pytest.param(
            {
                "CRITICAL": [],
                "ERROR": [f"{EVENT_HEADER}, source=Test.SetUp()\nmachine type m9 is not available in region eu-west-9"],
            },
            id="not-available-in-region",
        ),
    ],
)
def test_is_test_error_infra_pattern_returns_true(events):
    assert is_test_error(events)


@pytest.mark.parametrize(
    "events",
    [
        pytest.param({"CRITICAL": [], "ERROR": []}, id="no-events"),
        pytest.param(
            {"CRITICAL": [], "ERROR": [f"{EVENT_HEADER}, source=Test.test_stress() message=stress command failed"]},
            id="unrelated-error",
        ),
        pytest.param(
            {"CRITICAL": [], "ERROR": [f"{EVENT_HEADER}, source=Test.test_run()\nInsufficientInstanceCapacity"]},
            id="setup-pattern-outside-setup",
        ),
        pytest.param(
            {"CRITICAL": [], "ERROR": [], "WARNING": ["2026-01-01 10:00:00.000: (SpotTerminationEvent) node=n1"]},
            id="pattern-only-in-warning",
        ),
    ],
)
def test_is_test_error_without_infra_pattern_returns_false(events):
    assert not is_test_error(events)
