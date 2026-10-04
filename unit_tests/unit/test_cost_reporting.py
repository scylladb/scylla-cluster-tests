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

"""What SCT sends to Argus about a run's cost, and when it sends nothing."""

from dataclasses import replace

import pytest

from sdcm.utils.cloud_catalog.cost import RunCostEstimate
from sdcm.utils.cost_reporting import report_estimated_cost


class _FakeArgus:
    run_id = "6c1fbb6f-0000-4000-8000-000000000001"

    def __init__(self, error: Exception | None = None):
        self.estimates: list[tuple[str, float]] = []
        self._error = error

    def set_estimated_cost(self, run_id, value):
        if self._error:
            raise self._error
        self.estimates.append((run_id, value))


def _estimate(**overrides) -> RunCostEstimate:
    priced = replace(RunCostEstimate.unavailable(duration_hours=4.25, is_spot=True), total=12.2328, partial=False)
    return replace(priced, **overrides)


def test_the_estimate_is_sent_against_the_run_rounded_to_cents():
    client = _FakeArgus()
    assert report_estimated_cost(client, _estimate()) is True
    assert client.estimates == [(_FakeArgus.run_id, 12.23)]


def test_the_lifecycle_figure_is_sent_not_the_on_demand_ceiling():
    client = _FakeArgus()
    report_estimated_cost(client, _estimate(on_demand_total=20.99, fallback_to_on_demand=True))
    assert client.estimates[0][1] == 12.23


@pytest.mark.parametrize(
    "estimate",
    [
        pytest.param(RunCostEstimate.unavailable(), id="nothing-priced"),
        pytest.param(_estimate(partial=True), id="partial-floor"),
    ],
)
def test_an_unknown_or_partial_estimate_sends_nothing(estimate):
    """Argus stores any number as real, so a floor would be compared as if it were the answer."""
    client = _FakeArgus()
    assert report_estimated_cost(client, estimate) is False
    assert client.estimates == []


def test_an_argus_failure_is_logged_never_raised():
    client = _FakeArgus(error=ConnectionError("argus is down"))
    assert report_estimated_cost(client, _estimate()) is False
