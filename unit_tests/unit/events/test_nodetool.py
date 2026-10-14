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
# Copyright (c) 2020 ScyllaDB
from unittest import mock

import pytest

from sdcm.sct_events.nodetool import NodetoolEvent


def test_nodetool_cmd_no_options():
    """A nodetool event without options renders command and node at begin, plus the duration at end."""
    event = NodetoolEvent(nodetool_command="scrub", node="1.0.0.121", options="", publish_event=False)
    event.event_id = "3c8e2362-a987-4eff-953f-9cd1ad2017e5"
    event.begin_event()
    assert str(event) == (
        "(NodetoolEvent Severity.NORMAL) period_type=begin event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5: "
        "nodetool_command=scrub node=1.0.0.121"
    )

    event.duration = 20.325
    event.end_event()
    assert str(event) == (
        "(NodetoolEvent Severity.NORMAL) period_type=end event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5 "
        "duration=20.325s: nodetool_command=scrub node=1.0.0.121"
    )


def test_nodetool_cmd_with_options():
    """Command arguments and options render together, and the end duration is measured from begin."""
    begin_event_timestamp = 1623596860.1202102
    event = NodetoolEvent(
        nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
        node="1.0.0.121",
        options="more options",
        publish_event=False,
    )
    event.event_id = "ef4aeb1a-c004-40e4-af14-9d87a0526408"
    event.begin_event()
    event.begin_timestamp = event.event_timestamp = begin_event_timestamp
    assert str(event) == (
        "(NodetoolEvent Severity.NORMAL) period_type=begin event_id=ef4aeb1a-c004-40e4-af14-9d87a0526408: "
        "nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted drop_table_during_repair_ks_0 more options"
    )

    # validate duration calculation
    with mock.patch("time.time", return_value=begin_event_timestamp + 1):
        event.end_event()
        assert str(event) == (
            "(NodetoolEvent Severity.NORMAL) period_type=end event_id=ef4aeb1a-c004-40e4-af14-9d87a0526408 "
            "duration=1s: nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted "
            "drop_table_during_repair_ks_0 more options"
        )


def test_nodetool_failure():
    """A failed nodetool event renders its errors and traceback after a human-readable duration."""
    event = NodetoolEvent(
        nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
        node="1.0.0.121",
        options="more options",
        publish_event=False,
    )
    event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
    event.begin_event()

    event.add_error(["Failed with status 1"])
    event.full_traceback = "Traceback:"
    event.duration = 90358
    event.end_event()
    assert str(event) == (
        "(NodetoolEvent Severity.NORMAL) period_type=end event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318 "
        "duration=1d1h5m58s: nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted "
        "drop_table_during_repair_ks_0 more "
        "options errors=['Failed with status 1']\nTraceback:"
    )


def test_nodetool_serialization_to_json():
    """A begun nodetool event serializes to JSON, so it can be sent to the events log."""
    event = NodetoolEvent(
        nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
        node="1.0.0.121",
        options="more options",
        publish_event=False,
    )
    event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
    event.begin_event()
    assert event.to_json()


@pytest.mark.parametrize(
    "duration_input, duration_formatted",
    [
        pytest.param(None, "", id="none"),
        pytest.param(2, "2s", id="seconds"),
        pytest.param(2.52, "2.52s", id="fractional-seconds"),
        pytest.param(326, "5m26s", id="minutes"),
        pytest.param(4598, "1h16m38s", id="hours"),
        pytest.param(87400, "1d0h16m40s", id="days"),
    ],
)
def test_duration_format(duration_input, duration_formatted):
    """duration_formatted renders seconds as a compact d/h/m/s string, empty when duration is unset."""
    event = NodetoolEvent(
        nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
        node="1.0.0.121",
        options="more options",
        publish_event=False,
    )

    event.duration = duration_input
    assert event.duration_formatted == duration_formatted


@pytest.mark.parametrize(
    "publish_event",
    [
        pytest.param(True, id="publish-called"),
        pytest.param(False, id="publish-not-called"),
    ],
)
def test_begin_event_publishes_only_when_publish_event_is_set(publish_event):
    """begin_event() publishes the event exactly when it was created with publish_event=True."""
    with mock.patch("sdcm.sct_events.base.SctEvent.publish") as publish:
        event = NodetoolEvent(nodetool_command="scrub", node="1.0.0.121", options="", publish_event=publish_event)
        event.begin_event()
    assert publish.called is publish_event
