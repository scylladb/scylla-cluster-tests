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

import pytest

from sdcm.exceptions import UnsupportedNemesis, KillNemesis
from sdcm.sct_events import Severity
from sdcm.sct_events.nemesis import DisruptionEvent

EVENT_ID = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"


def begin_string(nemesis_name):
    """Return the string a begun DisruptionEvent with EVENT_ID renders for nemesis_name on target_node."""
    return (
        f"(DisruptionEvent Severity.NORMAL) period_type=begin event_id={EVENT_ID}: "
        f"nemesis_name={nemesis_name} target_node=target_node"
    )


def test_disruption_skipped_event():
    """A skipped disruption ends with NORMAL severity and reports the skip reason in its string form."""
    with (
        pytest.raises(UnsupportedNemesis),
        DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event,
    ):
        try:
            raise UnsupportedNemesis("This nemesis can run on scylla_bench test only")
        except UnsupportedNemesis as exc:
            nemesis_event.event_id = EVENT_ID
            nemesis_event.skip(skip_reason=str(exc))
            nemesis_event.duration = 15
            raise

    assert str(nemesis_event) == (
        f"(DisruptionEvent Severity.NORMAL) period_type=end event_id={EVENT_ID} "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node skipped skip_reason=This nemesis can "
        "run on scylla_bench test only"
    )


def test_disruption_is_skipped_only_after_skip():
    """is_skipped and skip_reason are unset on a fresh disruption and set by skip(), which health checks rely on."""
    nemesis_event = DisruptionEvent(nemesis_name="TestNemesis", node="test_node", publish_event=False)
    assert nemesis_event.is_skipped is False
    assert nemesis_event.skip_reason == ""

    nemesis_event.skip(skip_reason="Unsupported Scylla version")
    assert nemesis_event.is_skipped is True
    assert nemesis_event.skip_reason == "Unsupported Scylla version"


@pytest.mark.parametrize(
    "severity_set_in_body, expected_severity",
    [
        pytest.param(Severity.NORMAL, Severity.ERROR, id="normal-raised-to-error"),
        pytest.param(Severity.CRITICAL, Severity.CRITICAL, id="critical-kept"),
    ],
)
def test_disruption_raised_exception_event(severity_set_in_body, expected_severity):
    """An exception escaping a disruption raises its severity to at least ERROR and records the error."""
    with (
        pytest.raises(ZeroDivisionError),
        DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event,
    ):
        nemesis_event.event_id = EVENT_ID
        assert str(nemesis_event) == begin_string("DeleteByRowsRange")

        try:
            1 / 0
        except ZeroDivisionError:
            nemesis_event.add_error(["division by zero"])
            nemesis_event.full_traceback = "traceback.format_exc()"
            nemesis_event.severity = severity_set_in_body
            nemesis_event.duration = 15
            raise

    assert "division by zero" in nemesis_event.errors_formatted
    assert nemesis_event.severity == expected_severity
    assert nemesis_event.duration_formatted == "15s"


def test_disruption_error_event():
    """A disruption that handles its error and sets ERROR severity renders the errors in its end string."""
    with DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event:
        nemesis_event.event_id = EVENT_ID
        assert str(nemesis_event) == begin_string("DeleteByRowsRange")

        try:
            1 / 0
        except ZeroDivisionError:
            nemesis_event.add_error(["division by zero"])
            nemesis_event.full_traceback = "traceback.format_exc()"
            nemesis_event.duration = 15
            nemesis_event.severity = Severity.ERROR

    assert (
        f"(DisruptionEvent Severity.ERROR) period_type=end event_id={EVENT_ID} "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node errors=division by zero\n"
    ) in str(nemesis_event)


def test_disruption_normal_event():
    """A clean disruption renders begin and end strings with NORMAL severity and the duration."""
    with DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event:
        nemesis_event.event_id = EVENT_ID
        assert str(nemesis_event) == begin_string("DeleteByRowsRange")
        nemesis_event.duration = 15

    assert str(nemesis_event) == (
        f"(DisruptionEvent Severity.NORMAL) period_type=end event_id={EVENT_ID} "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node"
    )


def test_kill_nemesis_during_con_event():
    """KillNemesis escaping a disruption leaves it NORMAL with no errors, since killing is not a failure."""
    with (
        pytest.raises(KillNemesis),
        DisruptionEvent(nemesis_name="SomeNemesis", node="target_node", publish_event=False) as nemesis_event,
    ):
        nemesis_event.event_id = EVENT_ID
        assert str(nemesis_event) == begin_string("SomeNemesis")

        try:
            raise KillNemesis()

        except KillNemesis as ex:
            nemesis_event.add_error([str(ex)])
            nemesis_event.duration = 15
            raise

        except Exception:  # noqa: BLE001
            pytest.fail("we shouldn't reach this code path")

    assert nemesis_event.errors_formatted == ""
    assert nemesis_event.severity == Severity.NORMAL
    assert nemesis_event.duration_formatted == "15s"
