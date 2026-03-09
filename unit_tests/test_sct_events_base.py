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
    SctEvent._sct_event_types_registry = SctEventTypesRegistry(severities_conf=str(severities_conf))
    yield
    SctEvent._sct_event_types_registry = registry_backup


def test_sct_event_is_in_registry():
    assert "SctEvent" in SctEvent._sct_event_types_registry


def test_create_instance():
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        SctEvent()


def test_subclass_no_max_severity():
    with pytest.raises(ValueError, match="no max severity"):
        type("X", (SctEvent,), {})


def test_subclass_abstract_no_max_severity():
    class X(SctEvent, abstract=True):
        pass

    assert X.is_abstract()
    assert X.base == "X"
    assert X.type is None
    assert X.subtype is None
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        X()


def test_subclass_with_max_severity():
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
    class Y(SctEvent):
        pass

    with pytest.raises(TypeError, match="is already used"):
        type("Y", (SctEvent,), {})


def test_default_str():
    class Y(SctEvent):
        pass

    y = Y()
    y.event_id = "d81c016f-2333-4047-8c91-7cde98c38a15"
    assert str(y) == "(Y Severity.UNKNOWN) period_type=not-set event_id=d81c016f-2333-4047-8c91-7cde98c38a15"


def test_equal():
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
    class Y(SctEvent):
        pass

    class Z(SctEvent):
        pass

    y, z = Y(), Z()
    z.event_timestamp = y.event_timestamp
    assert y != z
    assert z != y


def test_equal_pickle_unpickle():
    global Y  # noqa: PLW0603

    class Y(SctEvent):
        pass

    y = Y()
    y_pickled = pickle.dumps(y)
    assert y == pickle.loads(y_pickled)


def test_to_json():
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
    """Simple `publish()' test with mocked `get_events_main_device()'."""

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
    """Simple `publish_or_dump()' test with mocked `get_events_main_device()'."""

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
    class Y(SctEvent):
        pass

    y = Y()
    y.event_timestamp = 0
    assert y.formatted_event_timestamp == "1970-01-01 00:00:00.000"
    y.event_timestamp = None
    assert y.formatted_event_timestamp == "0000-00-00 <UnknownTimestamp>"


def test_add_subevent_type_all_levels_not_abstract():
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


def test_add_subevent_type_assertions():
    @runtime_checkable
    class YT(SctEventProtocol, Protocol):
        S: Type[SctEventProtocol]

    class Y(SctEvent):
        T: Type[YT]

    with pytest.raises(AssertionError, match="valid Python identifier"):
        Y.add_subevent_type("123")
    with pytest.raises(AssertionError, match="valid Python identifier"):
        Y.add_subevent_type("T.S")
    with pytest.raises(AssertionError, match="valid Python identifier"):
        Y.add_subevent_type("class")
    with pytest.raises(AssertionError, match="already has attribute"):
        Y.add_subevent_type("severity")

    Y.add_subevent_type("T")
    with pytest.raises(AssertionError, match="already has attribute"):
        Y.add_subevent_type("T")

    Y.T.add_subevent_type("S")
    with pytest.raises(AssertionError, match="max level .* is already reached"):
        Y.T.S.add_subevent_type("next")


def test_add_subevent_type_mixin_with_init():
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
    global Y  # noqa: PLW0603

    class Y(SctEvent):
        T: Type[SctEvent]

    Y.add_subevent_type("T")

    yt = Y.T()
    yt_pickled = pickle.dumps(yt)

    assert yt == pickle.loads(yt_pickled)


# --- BaseFilter tests ---


def test_base_filter_create_instance():
    with pytest.raises(TypeError, match="may not be instantiated directly"):
        BaseFilter()

    class Y(BaseFilter):
        pass

    y = Y()
    assert y.uuid is not None
    assert not y.clear_filter
    assert y.expire_time is None


def test_base_filter_cancel():
    class Y(BaseFilter):
        pass

    with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
        y = Y()
        mock.assert_not_called()
        y.cancel_filter()
        mock.assert_called_once()
    assert y.clear_filter


def test_base_filter_context_manager():
    class Y(BaseFilter):
        pass

    with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
        with Y() as y:
            mock.assert_called_once()
            assert not y.clear_filter
        assert mock.call_count == 2
        assert y.clear_filter


def test_base_filter_eq():
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


def _create_test_event(subcontext=None):
    """Helper to create a test event with hardcoded event_id and severity."""

    class Y(SctEvent):
        pass

    event = Y()
    event.event_id = "test-event-id"
    event.severity = Severity.ERROR
    if subcontext is not None:
        event.subcontext = subcontext
    return event


def test_log_event_subclass():
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


def test_subcontext_with_dict_and_event_objects():
    """Test that __getstate__ handles both dict and SctEvent objects in subcontext."""
    disruption = DisruptionEvent(nemesis_name="TestNemesis", node="test-node")
    disruption.event_id = "disruption-event-id"

    subcontext = [
        disruption,  # SctEvent object
        {"event_id": "dict-event-id", "base": "TestBase", "nemesis_name": "DictNemesis"},  # dict object
    ]
    event = _create_test_event(subcontext=subcontext)

    state = event.__getstate__()

    assert isinstance(state["subcontext"], list)
    assert len(state["subcontext"]) == 2

    assert isinstance(state["subcontext"][0], dict)
    assert "event_id" in state["subcontext"][0]
    assert state["subcontext"][0]["event_id"] == "disruption-event-id"

    assert isinstance(state["subcontext"][1], dict)
    assert state["subcontext"][1]["event_id"] == "dict-event-id"


def test_to_json_with_dict_in_subcontext():
    """Test that to_json() works when subcontext contains dict objects."""
    subcontext = [{"event_id": "dict-event-id", "base": "TestBase", "nemesis_name": "DictNemesis"}]
    event = _create_test_event(subcontext=subcontext)

    json_str = event.to_json()

    parsed = json.loads(json_str)
    assert "subcontext" in parsed
    assert len(parsed["subcontext"]) == 1
    assert parsed["subcontext"][0]["event_id"] == "dict-event-id"
