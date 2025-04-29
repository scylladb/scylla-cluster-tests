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

import os
import pickle
import tempfile
import unittest
from typing import Optional, Type, Protocol, runtime_checkable
from unittest.mock import patch

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import \
    SctEvent, SctEventTypesRegistry, BaseFilter, LogEvent, LogEventProtocol


Y = None  # define a global name for pickle.


class TestSctEventDefaultRegistry(unittest.TestCase):
    def test_sct_event_is_in_registry(self):
        self.assertIn("SctEvent", SctEvent._sct_event_types_registry)


class SctEventTestCase(unittest.TestCase):
    severities_yaml = (b"Y: NORMAL\n"
                       b"Z: ERROR\n"
                       b"Y.T: WARNING\n"
                       b"Y.T.S: CRITICAL\n"
                       b"Z.T.S: NORMAL\n"
                       b"W.T.S: NORMAL\n")
    severities_conf: Optional[str] = None
    _registry_bu = None

    @classmethod
    def setUpClass(cls) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as fobj:
            fobj.write(cls.severities_yaml)
        cls.severities_conf = fobj.name

    @classmethod
    def tearDownClass(cls) -> None:
        os.unlink(cls.severities_conf)

    def setUp(self) -> None:
        self._registry_bu = SctEvent._sct_event_types_registry
        SctEvent._sct_event_types_registry = SctEventTypesRegistry(severities_conf=self.severities_conf)

    def tearDown(self) -> None:
        SctEvent._sct_event_types_registry = self._registry_bu


class TestSctEvent(SctEventTestCase):
    def test_create_instance(self):
        self.assertRaisesRegex(TypeError, "may not be instantiated directly", SctEvent)

    def test_subclass_no_max_severity(self):
        self.assertRaisesRegex(ValueError, "no max severity", type, "X", (SctEvent, ), {})

    def test_subclass_abstract_no_max_severity(self):
        class X(SctEvent, abstract=True):
            pass

        self.assertTrue(X.is_abstract())
        self.assertEqual(X.base, "X")
        self.assertIsNone(X.type)
        self.assertIsNone(X.subtype)
        self.assertRaisesRegex(TypeError, "may not be instantiated directly", X)

    def test_subclass_with_max_severity(self):
        class Y(SctEvent):
            pass

        self.assertFalse(Y.is_abstract())
        self.assertEqual(Y.base, "Y")
        self.assertIsNone(Y.type)
        self.assertIsNone(Y.subtype)
        y = Y()
        self.assertEqual(y.base, "Y")
        self.assertIsNone(y.type)
        self.assertIsNone(y.subtype)
        self.assertIsInstance(y, SctEventProtocol)

    def test_subclass_twice_with_same_name(self):

        class Y(SctEvent):
            pass

        self.assertRaisesRegex(TypeError, "is already used", type, "Y", (SctEvent, ), {})

    def test_default_str(self):
        class Y(SctEvent):
            pass

        y = Y()
        y.event_id = "d81c016f-2333-4047-8c91-7cde98c38a15"
        self.assertEqual(str(y),
                         "(Y Severity.UNKNOWN) period_type=not-set event_id=d81c016f-2333-4047-8c91-7cde98c38a15")

    def test_equal(self):
        class Y(SctEvent):
            pass

        class Z(Y):
            pass

        y, y_other, z = Y(), Y(), Z()
        z.event_timestamp = y_other.event_timestamp = y.event_timestamp
        y.event_id = y_other.event_id = z.event_id = "d81c016f-2333-4047-8c91-7cde98c38a15"
        self.assertEqual(y, y)
        self.assertEqual(y, y_other)
        self.assertEqual(y_other, y)
        self.assertEqual(y, z)
        self.assertEqual(z, y)

    def test_equal_not_subclass(self):
        class Y(SctEvent):
            pass

        class Z(SctEvent):
            pass

        y, z = Y(), Z()
        z.event_timestamp = y.event_timestamp
        self.assertNotEqual(y, z)
        self.assertNotEqual(z, y)

    def test_equal_pickle_unpickle(self):
        global Y  # noqa: PLW0603

        class Y(SctEvent):
            pass

        y = Y()
        y_pickled = pickle.dumps(y)
        self.assertEqual(y, pickle.loads(y_pickled))

    def test_to_json(self):
        class Y(SctEvent):
            pass

        y = Y()
        y.event_id = "fa4a84a2-968b-474c-b188-b3bac4be8527"
        self.assertEqual(
            y.to_json(),
            f'{{"base": "Y", "type": null, "subtype": null, "event_timestamp": {y.event_timestamp}, '
            f'"source_timestamp": null, '
            f'"severity": "UNKNOWN", "event_id": "fa4a84a2-968b-474c-b188-b3bac4be8527", "log_level": 30, '
            f'"subcontext": []}}'
        )

    def test_publish(self):
        """Simple `publish()' test with mocked `get_events_main_device()'."""

        class Y(SctEvent):
            pass

        y = Y()
        self.assertTrue(y._ready_to_publish)
        with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
            y.publish()
        mock.assert_called_once()
        mock().publish_event.assert_called_once()

        self.assertFalse(y._ready_to_publish)
        with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
            y.publish()
        mock.assert_not_called()

    def test_publish_or_dump(self):
        """Simple `publish_or_dump()' test with mocked `get_events_main_device()'."""

        class Y(SctEvent):
            pass

        y = Y()
        self.assertTrue(y._ready_to_publish)
        with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
            y.publish_or_dump()
        self.assertEqual(mock.call_count, 2)
        mock().is_alive.assert_called_once()
        mock().publish_event.assert_called_once()

        self.assertFalse(y._ready_to_publish)
        with patch("sdcm.sct_events.events_device.get_events_main_device") as mock:
            y.publish_or_dump()
        mock.assert_not_called()

    def test_formatted_event_timestamp(self):
        class Y(SctEvent):
            pass

        y = Y()
        y.event_timestamp = 0
        self.assertEqual(y.formatted_event_timestamp, "1970-01-01 00:00:00.000")
        y.event_timestamp = None
        self.assertEqual(y.formatted_event_timestamp, "0000-00-00 <UnknownTimestamp>")

    def test_add_subevent_type_all_levels_not_abstract(self):
        @runtime_checkable
        class YT(SctEventProtocol, Protocol):
            S: Type[SctEventProtocol]

        class Y(SctEvent):
            T: Type[YT]

        Y.add_subevent_type("T")
        self.assertFalse(Y.T.is_abstract())
        self.assertTrue(issubclass(Y.T, Y))
        self.assertEqual(Y.T.base, "Y")
        self.assertEqual(Y.T.type, "T")
        self.assertIsNone(Y.T.subtype)

        Y.T.add_subevent_type("S")
        self.assertFalse(Y.T.S.is_abstract())
        self.assertTrue(issubclass(Y.T.S, Y.T))
        self.assertTrue(issubclass(Y.T.S, Y))
        self.assertEqual(Y.T.S.base, "Y")
        self.assertEqual(Y.T.S.type, "T")
        self.assertEqual(Y.T.S.subtype, "S")

    def test_add_subevent_type_abstract_level_in_the_middle(self):
        @runtime_checkable
        class ZT(SctEventProtocol, Protocol):
            S: Type[SctEventProtocol]

        class Z(SctEvent):
            T: Type[ZT]

        self.assertRaisesRegex(ValueError, "no max severity", Z.add_subevent_type, "T")
        Z.add_subevent_type("T", abstract=True)
        self.assertTrue(Z.T.is_abstract())
        self.assertTrue(issubclass(Z.T, Z))
        self.assertEqual(Z.T.base, "Z")
        self.assertEqual(Z.T.type, "T")
        self.assertIsNone(Z.T.subtype)

        Z.T.add_subevent_type("S")
        self.assertFalse(Z.T.S.is_abstract())
        self.assertTrue(issubclass(Z.T.S, Z.T))
        self.assertTrue(issubclass(Z.T.S, Z))
        self.assertEqual(Z.T.S.base, "Z")
        self.assertEqual(Z.T.S.type, "T")
        self.assertEqual(Z.T.S.subtype, "S")

    def test_add_subevent_type_subtype_is_not_abstract_only(self):
        @runtime_checkable
        class WT(SctEventProtocol, Protocol):
            S: Type[SctEventProtocol]

        class W(SctEvent, abstract=True):
            T: Type[WT]

        self.assertRaisesRegex(ValueError, "no max severity", W.add_subevent_type, "T")
        W.add_subevent_type("T", abstract=True)
        self.assertTrue(W.T.is_abstract())
        self.assertTrue(issubclass(W.T, W))
        self.assertEqual(W.T.base, "W")
        self.assertEqual(W.T.type, "T")
        self.assertIsNone(W.T.subtype)

        W.T.add_subevent_type("S")
        self.assertFalse(W.T.S.is_abstract())
        self.assertTrue(issubclass(W.T.S, W.T))
        self.assertTrue(issubclass(W.T.S, W))
        self.assertEqual(W.T.S.base, "W")
        self.assertEqual(W.T.S.type, "T")
        self.assertEqual(W.T.S.subtype, "S")

    def test_add_subevent_type_assertions(self):
        @runtime_checkable
        class YT(SctEventProtocol, Protocol):
            S: Type[SctEventProtocol]

        class Y(SctEvent):
            T: Type[YT]

        self.assertRaisesRegex(AssertionError, "valid Python identifier", Y.add_subevent_type, "123")
        self.assertRaisesRegex(AssertionError, "valid Python identifier", Y.add_subevent_type, "T.S")
        self.assertRaisesRegex(AssertionError, "valid Python identifier", Y.add_subevent_type, "class")
        self.assertRaisesRegex(AssertionError, "already has attribute", Y.add_subevent_type, "severity")

        Y.add_subevent_type("T")
        self.assertRaisesRegex(AssertionError, "already has attribute", Y.add_subevent_type, "T")

        Y.T.add_subevent_type("S")
        self.assertRaisesRegex(AssertionError, "max level .* is already reached", Y.T.S.add_subevent_type, "next")

    def test_add_subevent_type_mixin_with_init(self):
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
        self.assertFalse(Y.T.is_abstract())
        self.assertTrue(issubclass(Y.T, Y))
        self.assertEqual(Y.T.base, "Y")
        self.assertEqual(Y.T.type, "T")
        self.assertIsNone(Y.T.subtype)
        self.assertEqual(Y.T.severity, Severity.WARNING)

        self.assertRaisesRegex(TypeError, "missing 1 required .* 'attr1'", Y.T)

        yt = Y.T(attr1="value1")
        self.assertEqual(yt.base, "Y")
        self.assertEqual(yt.type, "T")
        self.assertIsNone(yt.subtype)
        self.assertEqual(yt.severity, Severity.CRITICAL)
        self.assertEqual(yt.attr1, "value1")
        self.assertEqual(yt.attr2, "value2")

        Y.T.add_subevent_type("S", attr1="another_value1")
        self.assertFalse(Y.T.S.is_abstract())
        self.assertTrue(issubclass(Y.T.S, Y.T))
        self.assertTrue(issubclass(Y.T.S, Y))
        self.assertEqual(Y.T.S.base, "Y")
        self.assertEqual(Y.T.S.type, "T")
        self.assertEqual(Y.T.S.subtype, "S")
        self.assertEqual(Y.T.S.severity, Severity.WARNING)
        yts = Y.T.S()
        self.assertEqual(yts.base, "Y")
        self.assertEqual(yts.type, "T")
        self.assertEqual(yts.subtype, "S")
        self.assertEqual(yts.severity, Severity.CRITICAL)
        self.assertEqual(yts.attr1, "another_value1")
        self.assertEqual(yts.attr2, "value2")

    def test_add_subevent_type_mixin_without_init(self):
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
        self.assertEqual(Y.T.severity, Severity.WARNING)

        yt = Y.T(attr1="value1")
        self.assertEqual(yt.severity, Severity.ERROR)
        self.assertEqual(yt.attr1, "value1")

    def test_add_subevent_type_pickle(self):
        global Y  # noqa: PLW0603

        class Y(SctEvent):
            T: Type[SctEvent]

        Y.add_subevent_type("T")

        yt = Y.T()
        yt_pickled = pickle.dumps(yt)

        self.assertEqual(yt, pickle.loads(yt_pickled))


class TestBaseFilter(SctEventTestCase):
    def test_create_instance(self):
        self.assertRaisesRegex(TypeError, "may not be instantiated directly", BaseFilter)

        class Y(BaseFilter):
            pass

        y = Y()
        self.assertIsNotNone(y.uuid)
        self.assertFalse(y.clear_filter)
        self.assertIsNone(y.expire_time)

    def test_cancel(self):
        class Y(BaseFilter):
            pass

        with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
            y = Y()
            mock.assert_not_called()
            y.cancel_filter()
            mock.assert_called_once()
        self.assertTrue(y.clear_filter)

    def test_context_manager(self):
        class Y(BaseFilter):
            pass

        with patch("sdcm.sct_events.base.SctEvent.publish") as mock:
            with Y() as y:
                mock.assert_called_once()
                self.assertFalse(y.clear_filter)
            self.assertEqual(mock.call_count, 2)
            self.assertTrue(y.clear_filter)

    def test_eq(self):
        class Y(BaseFilter):
            pass

        class Z(Y):
            pass

        y1 = Y()
        y2 = Y()
        z = Z()
        z.uuid = y2.uuid = y1.uuid
        self.assertEqual(y1, y2)
        self.assertEqual(y2, y1)
        self.assertEqual(y1, z)
        self.assertEqual(z, y1)

    def test_not_eq(self):
        class Y(BaseFilter):
            pass

        class Z(BaseFilter):
            pass

        y = Y()
        z = Z()
        z.uuid = y.uuid
        self.assertNotEqual(y, z)
        self.assertNotEqual(z, y)


class TestLogEvent(SctEventTestCase):
    def test_log_event_subclass(self):
        class Y(LogEvent):
            pass

        self.assertFalse(Y.is_abstract())
        self.assertEqual(Y.base, "Y")
        self.assertIsNone(Y.type)
        self.assertIsNone(Y.subtype)
        y = Y(regex="regex1")
        self.assertEqual(y.base, "Y")
        self.assertIsNone(y.type)
        self.assertIsNone(y.subtype)
        self.assertEqual(y.severity, Severity.ERROR)
        self.assertEqual(y.regex, "regex1")
        self.assertIsNone(y.node)
        self.assertIsNone(y.line)
        self.assertEqual(y.line_number, 0)
        self.assertIsNone(y.backtrace)
        self.assertIsNone(y.raw_backtrace)
        self.assertFalse(y._ready_to_publish)
        self.assertIsInstance(y, LogEvent)
        self.assertIsInstance(y, SctEventProtocol)

    def test_add_info(self):
        class Y(LogEvent):
            pass

        y = Y(regex="regex1")
        self.assertIs(y, y.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1))
        self.assertEqual(y.base, "Y")
        self.assertIsNone(y.type)
        self.assertIsNone(y.subtype)
        self.assertEqual(y.timestamp, -275248380.0)
        self.assertEqual(y.severity, Severity.ERROR)
        self.assertEqual(y.regex, "regex1")
        self.assertEqual(y.node, "node1")
        self.assertEqual(y.line, "1961-04-12T06:07:00+00:00 Poyekhalee!")
        self.assertEqual(y.line_number, 1)
        self.assertIsNone(y.backtrace)
        self.assertIsNone(y.raw_backtrace)
        self.assertTrue(y._ready_to_publish)

    def test_clone_fresh(self):
        global Y  # noqa: PLW0603

        class Y(LogEvent):
            pass

        z = Y(regex="regex1")
        y = z.clone()
        self.assertIsNot(y, z)
        self.assertEqual(y.base, "Y")
        self.assertIsNone(y.type)
        self.assertIsNone(y.subtype)
        self.assertEqual(y.severity, Severity.ERROR)
        self.assertEqual(y.regex, "regex1")
        self.assertIsNone(y.node)
        self.assertIsNone(y.line)
        self.assertEqual(y.line_number, 0)
        self.assertIsNone(y.backtrace)
        self.assertIsNone(y.raw_backtrace)
        self.assertFalse(y._ready_to_publish)
        self.assertIsInstance(y, LogEvent)
        self.assertIsInstance(y, SctEventProtocol)

    def test_clone_with_info(self):
        global Y  # noqa: PLW0603

        class Y(LogEvent):
            pass

        z = Y(regex="regex1", severity=Severity.NORMAL)
        z.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
        z.backtrace = "backtrace1"
        z.raw_backtrace = "raw_backtrace1"
        y = z.clone()
        self.assertIsNot(y, z)
        self.assertEqual(y.base, "Y")
        self.assertIsNone(y.type)
        self.assertIsNone(y.subtype)
        self.assertEqual(y.timestamp, -275248380.0)
        self.assertEqual(y.severity, Severity.NORMAL)
        self.assertEqual(y.regex, "regex1")
        self.assertEqual(y.node, "node1")
        self.assertEqual(y.line, "1961-04-12T06:07:00+00:00 Poyekhalee!")
        self.assertEqual(y.line_number, 1)
        self.assertEqual(y.backtrace, "backtrace1")
        self.assertEqual(y.raw_backtrace, "raw_backtrace1")
        self.assertFalse(y._ready_to_publish)
        self.assertIsInstance(y, SctEventProtocol)
        self.assertIsInstance(y, LogEvent)

        y.add_info(node="node2", line="no timestamp", line_number=42)
        self.assertNotEqual(z.event_timestamp, y.event_timestamp)
        self.assertEqual(y.node, "node2")
        self.assertEqual(y.line, "no timestamp")
        self.assertEqual(y.line_number, 42)
        self.assertEqual(y.backtrace, "backtrace1")
        self.assertEqual(y.raw_backtrace, "raw_backtrace1")
        self.assertTrue(y._ready_to_publish)
        self.assertEqual(z.node, "node1")
        self.assertEqual(z.line, "1961-04-12T06:07:00+00:00 Poyekhalee!")
        self.assertEqual(z.line_number, 1)
        self.assertTrue(z._ready_to_publish)

    def test_msgfmt(self):
        class Y(LogEvent):
            T: Type[LogEventProtocol]

        Y.add_subevent_type("T", regex="r1")

        y = Y.T()
        y.event_id = "04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e"
        self.assertEqual(str(y), "(Y Severity.ERROR) period_type=one-time "
                                 "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=0")

        y.add_info(node="n1", line="l1", line_number=1)
        self.assertEqual(str(y), "(Y Severity.ERROR) period_type=one-time "
                                 "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
                                 "node=n1\nl1")

        y.raw_backtrace = "rb1"
        self.assertEqual(str(y), "(Y Severity.ERROR) period_type=one-time "
                                 "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
                                 "node=n1\nl1\nrb1")

        y.backtrace = "b1"
        self.assertEqual(str(y), "(Y Severity.ERROR) period_type=one-time "
                                 "event_id=04ace3fb-b9bc-4c86-bfb2-2ffae18bb72e: type=T regex=r1 line_number=1 "
                                 "node=n1\nl1\nb1")
