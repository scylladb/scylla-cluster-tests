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

import json
import pickle
from typing import Type, Protocol, runtime_checkable
from unittest.mock import patch

import pytest

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import SctEvent, SctEventTypesRegistry, BaseFilter, LogEvent, LogEventProtocol
from sdcm.sct_events.nemesis import DisruptionEvent

Y = None  # define a global name for pickle.

SEVERITIES_YAML = b"Y: NORMAL\nZ: ERROR\nY.T: WARNING\nY.T.S: CRITICAL\nZ.T.S: NORMAL\nW.T.S: NORMAL\n"


@pytest.fixture(autouse=True)
def sct_event_registry(tmp_path):
    """Create a temporary severities config and swap the event registry for each test."""
    severities_conf = tmp_path / "severities.yaml"
    severities_conf.write_bytes(SEVERITIES_YAML)
    registry_backup = SctEvent._sct_event_types_registry
    new_registry = SctEventTypesRegistry(severities_conf=str(severities_conf))
    new_registry["SctEvent"] = SctEvent  # Re-register base class (normally done via __set_name__ at class creation)
    SctEvent._sct_event_types_registry = new_registry
    yield
    SctEvent._sct_event_types_registry = registry_backup


@pytest.fixture
def error_event(sct_event_registry):
    """ERROR-severity event of a fresh SctEvent subclass with a fixed event_id."""
    event = type("Y", (SctEvent,), {})()
    event.event_id = "test-event-id"
    event.severity = Severity.ERROR
    return event


def test_sct_event_is_in_registry():
    """SctEvent registers itself in the event types registry, so severities resolve for it."""
    assert "SctEvent" in SctEvent._sct_event_types_registry


def test_create_instance():
    """SctEvent is abstract and refuses direct instantiation."""
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        SctEvent()


def test_subclass_no_max_severity():
    """A non-abstract subclass without a configured severity is rejected at class creation."""
    with pytest.raises(ValueError, match="no max severity"):
        type("X", (SctEvent,), {})


def test_subclass_abstract_no_max_severity():
    """An abstract subclass needs no severity, takes its name as base and refuses instantiation."""

    class X(SctEvent, abstract=True):
        pass

    assert X.is_abstract()
    assert X.base == "X"
    assert X.type is None
    assert X.subtype is None
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        X()


def test_subclass_with_max_severity():
    """A subclass with a configured severity is concrete and instances satisfy SctEventProtocol."""

    class Y(SctEvent):
        pass

    assert not Y.is_abstract()
    assert Y.base == "Y"
    assert Y.type is None
    assert Y.subtype is None
    y = Y()
    assert y.base == "Y"
    assert y.type is None
    assert y.subtype is None
    assert isinstance(y, SctEventProtocol)


def test_subclass_twice_with_same_name():
    """Event class names are unique in the registry; reusing one raises TypeError."""
    _ = type("Y", (SctEvent,), {})

    with pytest.raises(TypeError, match="is already used"):
        type("Y", (SctEvent,), {})


def test_default_str():
    """str() of a bare event renders base, severity, period type and event_id."""

    class Y(SctEvent):
        pass

    y = Y()
    y.event_id = "d81c016f-2333-4047-8c91-7cde98c38a15"
    assert str(y) == "(Y Severity.UNKNOWN) period_type=not-set event_id=d81c016f-2333-4047-8c91-7cde98c38a15"


def test_equal():
    """Events of the same class or a subclass with equal id and timestamp compare equal both ways."""

    class Y(SctEvent):
        pass

    class Z(Y):
        pass

    y, y_other, z = Y(), Y(), Z()
    z.event_timestamp = y_other.event_timestamp = y.event_timestamp
    y.event_id = y_other.event_id = z.event_id = "d81c016f-2333-4047-8c91-7cde98c38a15"
    assert y == y  # noqa: PLR0124  # intentionally testing __eq__ reflexivity
    assert y == y_other
    assert y_other == y
    assert y == z
    assert z == y


def test_equal_not_subclass():
    """Events of unrelated classes compare unequal even with the same timestamp."""

    class Y(SctEvent):
        pass

    class Z(SctEvent):
        pass

    y, z = Y(), Z()
    z.event_timestamp = y.event_timestamp
    assert y != z
    assert z != y


def test_equal_pickle_unpickle():
    """An event survives a pickle round trip unchanged, as needed to pass it between processes."""
    global Y  # noqa: PLW0603

    class Y(SctEvent):
        pass

    y = Y()
    y_pickled = pickle.dumps(y)
    assert y == pickle.loads(y_pickled)


def test_to_json():
    """to_json() serializes the event's base attributes for the events log."""

    class Y(SctEvent):
        pass

    y = Y()
    y.event_id = "fa4a84a2-968b-474c-b188-b3bac4be8527"
    assert y.to_json() == (
        f'{{"base": "Y", "type": null, "subtype": null, "event_timestamp": {y.event_timestamp}, '
        f'"source_timestamp": null, '
        f'"severity": "UNKNOWN", "event_id": "fa4a84a2-968b-474c-b188-b3bac4be8527", "log_level": 30, '
        f'"subcontext": []}}'
    )


def test_publish():
    """publish() sends the event to the main device once and then marks it as not ready to publish."""

    class Y(SctEvent):
        pass

    y = Y()
    assert y._ready_to_publish
    with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
        y.publish()
    mock.assert_called_once()
    mock().publish_event.assert_called_once()

    assert not y._ready_to_publish
    with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
        y.publish()
    mock.assert_not_called()


def test_publish_or_dump():
    """publish_or_dump() publishes through a live main device once and then marks the event as not ready."""

    class Y(SctEvent):
        pass

    y = Y()
    assert y._ready_to_publish
    with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
        y.publish_or_dump()
    assert mock.call_count == 2
    mock().is_alive.assert_called_once()
    mock().publish_event.assert_called_once()

    assert not y._ready_to_publish
    with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
        y.publish_or_dump()
    mock.assert_not_called()


def test_formatted_event_timestamp():
    """formatted_event_timestamp renders a UTC timestamp and a placeholder when it is unset."""

    class Y(SctEvent):
        pass

    y = Y()
    y.event_timestamp = 0
    assert y.formatted_event_timestamp == "1970-01-01 00:00:00.000"
    y.event_timestamp = None
    assert y.formatted_event_timestamp == "0000-00-00 <UnknownTimestamp>"


def test_add_subevent_type_all_levels_not_abstract():
    """Concrete subevents nest as subclasses with base/type/subtype set per level."""

    @runtime_checkable
    class YT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class Y(SctEvent):
        T: Type[YT]

    Y.add_subevent_type("T")
    assert not Y.T.is_abstract()
    assert issubclass(Y.T, Y)
    assert Y.T.base == "Y"
    assert Y.T.type == "T"
    assert Y.T.subtype is None

    Y.T.add_subevent_type("S")
    assert not Y.T.S.is_abstract()
    assert issubclass(Y.T.S, Y.T)
    assert issubclass(Y.T.S, Y)
    assert Y.T.S.base == "Y"
    assert Y.T.S.type == "T"
    assert Y.T.S.subtype == "S"


def test_add_subevent_type_abstract_level_in_the_middle():
    """A middle level without severity must be abstract; its concrete subtype still nests under it."""

    @runtime_checkable
    class ZT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class Z(SctEvent):
        T: Type[ZT]

    with pytest.raises(ValueError, match="no max severity"):
        Z.add_subevent_type("T")
    Z.add_subevent_type("T", abstract=True)
    assert Z.T.is_abstract()
    assert issubclass(Z.T, Z)
    assert Z.T.base == "Z"
    assert Z.T.type == "T"
    assert Z.T.subtype is None

    Z.T.add_subevent_type("S")
    assert not Z.T.S.is_abstract()
    assert issubclass(Z.T.S, Z.T)
    assert issubclass(Z.T.S, Z)
    assert Z.T.S.base == "Z"
    assert Z.T.S.type == "T"
    assert Z.T.S.subtype == "S"


def test_add_subevent_type_subtype_is_not_abstract_only():
    """Under an abstract base, an abstract middle level can carry a concrete subtype."""

    @runtime_checkable
    class WT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class W(SctEvent, abstract=True):
        T: Type[WT]

    with pytest.raises(ValueError, match="no max severity"):
        W.add_subevent_type("T")
    W.add_subevent_type("T", abstract=True)
    assert W.T.is_abstract()
    assert issubclass(W.T, W)
    assert W.T.base == "W"
    assert W.T.type == "T"
    assert W.T.subtype is None

    W.T.add_subevent_type("S")
    assert not W.T.S.is_abstract()
    assert issubclass(W.T.S, W.T)
    assert issubclass(W.T.S, W)
    assert W.T.S.base == "W"
    assert W.T.S.type == "T"
    assert W.T.S.subtype == "S"


@pytest.mark.parametrize(
    ("name", "error_match"),
    [
        pytest.param("123", "valid Python identifier", id="starts_with_digit"),
        pytest.param("T.S", "valid Python identifier", id="dotted"),
        pytest.param("class", "valid Python identifier", id="keyword"),
        pytest.param("severity", "already has attribute", id="existing_attribute"),
    ],
)
def test_add_subevent_type_invalid_name_raises_assertion(name, error_match):
    """add_subevent_type() rejects names that are not identifiers or shadow an attribute of the event."""

    class Y(SctEvent):
        pass

    with pytest.raises(AssertionError, match=error_match):
        Y.add_subevent_type(name)


def test_add_subevent_type_repeated_or_too_deep_raises_assertion():
    """add_subevent_type() rejects re-adding a subevent and nesting below the subtype level."""

    @runtime_checkable
    class YT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class Y(SctEvent):
        T: Type[YT]

    Y.add_subevent_type("T")
    with pytest.raises(AssertionError, match="already has attribute"):
        Y.add_subevent_type("T")

    Y.T.add_subevent_type("S")
    with pytest.raises(AssertionError, match="max level .* is already reached"):
        Y.T.S.add_subevent_type("next")


def test_add_subevent_type_mixin_with_init():
    """A mixin's __init__ and kwargs passed to add_subevent_type() become subevent defaults."""

    @runtime_checkable
    class YT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class Y(SctEvent):
        T: Type[YT]

    class Mixin:
        severity = Severity.WARNING

        def __init__(self, attr1, attr2, severity):
            self.attr1 = attr1
            self.attr2 = attr2
            super().__init__(severity=severity)

    Y.add_subevent_type("T", mixin=Mixin, attr2="value2", severity=Severity.CRITICAL)
    assert not Y.T.is_abstract()
    assert issubclass(Y.T, Y)
    assert Y.T.base == "Y"
    assert Y.T.type == "T"
    assert Y.T.subtype is None
    assert Y.T.severity == Severity.WARNING

    with pytest.raises(TypeError, match="missing 1 required .* 'attr1'"):
        Y.T()

    yt = Y.T(attr1="value1")
    assert yt.base == "Y"
    assert yt.type == "T"
    assert yt.subtype is None
    assert yt.severity == Severity.CRITICAL
    assert yt.attr1 == "value1"
    assert yt.attr2 == "value2"

    Y.T.add_subevent_type("S", attr1="another_value1")
    assert not Y.T.S.is_abstract()
    assert issubclass(Y.T.S, Y.T)
    assert issubclass(Y.T.S, Y)
    assert Y.T.S.base == "Y"
    assert Y.T.S.type == "T"
    assert Y.T.S.subtype == "S"
    assert Y.T.S.severity == Severity.WARNING
    yts = Y.T.S()
    assert yts.base == "Y"
    assert yts.type == "T"
    assert yts.subtype == "S"
    assert yts.severity == Severity.CRITICAL
    assert yts.attr1 == "another_value1"
    assert yts.attr2 == "value2"


def test_add_subevent_type_mixin_without_init():
    """A mixin without __init__ supplies class attributes while the base __init__ sets instance state."""

    @runtime_checkable
    class YT(SctEventProtocol, Protocol):
        attr1: str

    class Y(SctEvent):
        T: Type[YT]

        def __init__(self, attr1):
            self.attr1 = attr1
            super().__init__(severity=Severity.ERROR)

    class Mixin:
        severity = Severity.WARNING

    Y.add_subevent_type("T", mixin=Mixin)
    assert Y.T.severity == Severity.WARNING

    yt = Y.T(attr1="value1")
    assert yt.severity == Severity.ERROR
    assert yt.attr1 == "value1"


def test_add_subevent_type_pickle():
    """Dynamically added subevents survive a pickle round trip."""
    global Y  # noqa: PLW0603

    class Y(SctEvent):
        T: Type[SctEvent]

    Y.add_subevent_type("T")

    yt = Y.T()
    yt_pickled = pickle.dumps(yt)

    assert yt == pickle.loads(yt_pickled)


# --- BaseFilter tests ---


def test_base_filter_create_instance():
    """BaseFilter is abstract; a subclass instance gets a uuid and starts active with no expiry."""
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        BaseFilter()

    class Y(BaseFilter):
        pass

    y = Y()
    assert y.uuid is not None
    assert not y.clear_filter
    assert y.expire_time is None


def test_base_filter_cancel():
    """cancel_filter() publishes the filter once more with clear_filter set, lifting the filter."""

    class Y(BaseFilter):
        pass

    with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
        y = Y()
        mock.assert_not_called()
        y.cancel_filter()
        mock.assert_called_once()
    assert y.clear_filter


def test_base_filter_context_manager():
    """Used as a context manager, a filter publishes on enter and clears itself on exit."""

    class Y(BaseFilter):
        pass

    with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
        with Y() as y:
            mock.assert_called_once()
            assert not y.clear_filter
        assert mock.call_count == 2
        assert y.clear_filter


def test_base_filter_eq():
    """Filters of the same class or a subclass with equal uuid compare equal both ways."""

    class Y(BaseFilter):
        pass

    class Z(Y):
        pass

    y1 = Y()
    y2 = Y()
    z = Z()
    z.uuid = y2.uuid = y1.uuid
    assert y1 == y2
    assert y2 == y1
    assert y1 == z
    assert z == y1


def test_base_filter_not_eq():
    """Filters of unrelated classes compare unequal even with the same uuid."""

    class Y(BaseFilter):
        pass

    class Z(BaseFilter):
        pass

    y = Y()
    z = Z()
    z.uuid = y.uuid
    assert y != z
    assert z != y


# --- LogEvent tests ---


def test_log_event_subclass():
    """A LogEvent subclass defaults to ERROR, empty log info and not ready to publish."""

    class Y(LogEvent):
        pass

    assert not Y.is_abstract()
    assert Y.base == "Y"
    assert Y.type is None
    assert Y.subtype is None
    y = Y(regex="regex1")
    assert y.base == "Y"
    assert y.type is None
    assert y.subtype is None
    assert y.severity == Severity.ERROR
    assert y.regex == "regex1"
    assert y.node is None
    assert y.line is None
    assert y.line_number == 0
    assert y.backtrace is None
    assert y.raw_backtrace is None
    assert not y._ready_to_publish
    assert isinstance(y, LogEvent)
    assert isinstance(y, SctEventProtocol)


def test_log_event_add_info():
    """add_info() fills node, line and timestamp from the log line and makes the event publishable."""

    class Y(LogEvent):
        pass

    y = Y(regex="regex1")
    assert y is y.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
    assert y.base == "Y"
    assert y.type is None
    assert y.subtype is None
    assert y.timestamp == -275248380.0
    assert y.severity == Severity.ERROR
    assert y.regex == "regex1"
    assert y.node == "node1"
    assert y.line == "1961-04-12T06:07:00+00:00 Poyekhalee!"
    assert y.line_number == 1
    assert y.backtrace is None
    assert y.raw_backtrace is None
    assert y._ready_to_publish


def test_log_event_clone_fresh():
    """clone() of a fresh LogEvent yields a distinct copy with the same defaults."""
    global Y  # noqa: PLW0603

    class Y(LogEvent):
        pass

    z = Y(regex="regex1")
    y = z.clone()
    assert y is not z
    assert y.base == "Y"
    assert y.type is None
    assert y.subtype is None
    assert y.severity == Severity.ERROR
    assert y.regex == "regex1"
    assert y.node is None
    assert y.line is None
    assert y.line_number == 0
    assert y.backtrace is None
    assert y.raw_backtrace is None
    assert not y._ready_to_publish
    assert isinstance(y, LogEvent)
    assert isinstance(y, SctEventProtocol)


def test_log_event_clone_with_info():
    """clone() copies log info but not readiness, and later add_info() on the clone leaves the source intact."""
    global Y  # noqa: PLW0603

    class Y(LogEvent):
        pass

    z = Y(regex="regex1", severity=Severity.NORMAL)
    z.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
    z.backtrace = "backtrace1"
    z.raw_backtrace = "raw_backtrace1"
    y = z.clone()
    assert y is not z
    assert y.base == "Y"
    assert y.type is None
    assert y.subtype is None
    assert y.timestamp == -275248380.0
    assert y.severity == Severity.NORMAL
    assert y.regex == "regex1"
    assert y.node == "node1"
    assert y.line == "1961-04-12T06:07:00+00:00 Poyekhalee!"
    assert y.line_number == 1
    assert y.backtrace == "backtrace1"
    assert y.raw_backtrace == "raw_backtrace1"
    assert not y._ready_to_publish
    assert isinstance(y, SctEventProtocol)
    assert isinstance(y, LogEvent)

    y.add_info(node="node2", line="no timestamp", line_number=42)
    assert z.event_timestamp != y.event_timestamp
    assert y.node == "node2"
    assert y.line == "no timestamp"
    assert y.line_number == 42
    assert y.backtrace == "backtrace1"
    assert y.raw_backtrace == "raw_backtrace1"
    assert y._ready_to_publish
    assert z.node == "node1"
    assert z.line == "1961-04-12T06:07:00+00:00 Poyekhalee!"
    assert z.line_number == 1
    assert z._ready_to_publish


def test_log_event_msgfmt():
    """LogEvent str() adds node/line, then raw backtrace, then backtrace in preference to it."""

    class Y(LogEvent):
        T: Type[LogEventProtocol]

    Y.add_subevent_type("T", regex="r1")

    y = Y.T()
    y.event_id = "04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e"
    assert str(y) == (
        "(Y Severity.ERROR) period_type=one-time "
        "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=0"
    )

    y.add_info(node="n1", line="l1", line_number=1)
    assert str(y) == (
        "(Y Severity.ERROR) period_type=one-time "
        "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
        "node=n1\nl1"
    )

    y.raw_backtrace = "rb1"
    assert str(y) == (
        "(Y Severity.ERROR) period_type=one-time "
        "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
        "node=n1\nl1\nrb1"
    )

    y.backtrace = "b1"
    assert str(y) == (
        "(Y Severity.ERROR) period_type=one-time "
        "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
        "node=n1\nl1\nb1"
    )


def test_subcontext_with_dict_and_event_objects(error_event):
    """__getstate__ serializes both SctEvent and dict subcontext entries to dicts, keeping events picklable."""
    disruption = DisruptionEvent(nemesis_name="TestNemesis", node="test-node")
    disruption.event_id = "disruption-event-id"

    subcontext = [
        disruption,  # SctEvent object
        {"event_id": "dict-event-id", "base": "TestBase", "nemesis_name": "DictNemesis"},  # dict object
    ]
    error_event.subcontext = subcontext

    state = error_event.__getstate__()

    assert isinstance(state["subcontext"], list)
    assert len(state["subcontext"]) == 2

    assert isinstance(state["subcontext"][0], dict)
    assert "event_id" in state["subcontext"][0]
    assert state["subcontext"][0]["event_id"] == "disruption-event-id"

    assert isinstance(state["subcontext"][1], dict)
    assert state["subcontext"][1]["event_id"] == "dict-event-id"


def test_to_json_with_dict_in_subcontext(error_event):
    """to_json() includes dict subcontext entries, so events restored from dicts still serialize."""
    error_event.subcontext = [{"event_id": "dict-event-id", "base": "TestBase", "nemesis_name": "DictNemesis"}]

    json_str = error_event.to_json()

    parsed = json.loads(json_str)
    assert "subcontext" in parsed
    assert len(parsed["subcontext"]) == 1
    assert parsed["subcontext"][0]["event_id"] == "dict-event-id"
