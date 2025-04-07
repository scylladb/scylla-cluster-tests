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

import unittest
import unittest.mock
from pathlib import Path

from sdcm.sct_events.events_processes import \
    EventsProcessesRegistry, create_default_events_process_registry, get_default_events_process_registry


class FakeProcess:
    def __init__(self, _registry=None):
        self._registry = _registry
        self.started = False

    def start(self):
        self.started = True


class TestEventsProcessesRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = EventsProcessesRegistry("some_path")

    def test_fresh(self):
        self.assertEqual(self.registry._registry_dict, {})
        self.assertEqual(self.registry.log_dir, Path("some_path"))

    def test_start_events_process(self):
        self.registry.start_events_process("test", FakeProcess)
        self.assertEqual(len(self.registry._registry_dict), 1)
        self.assertIn("test", self.registry._registry_dict)
        self.assertEqual(self.registry._registry_dict["test"]._registry, self.registry)
        self.assertTrue(self.registry._registry_dict["test"].started)

    def test_get_events_process(self):
        process = self.registry.get_events_process("test")
        self.assertIsNone(process)
        self.registry.start_events_process("test", FakeProcess)
        process = self.registry.get_events_process("test")
        self.assertEqual(process._registry, self.registry)
        self.assertTrue(process.started)

    @unittest.mock.patch("sdcm.sct_events.events_processes._EVENTS_PROCESSES", not None)
    def test_create_default_registry_exists(self):
        self.assertRaises(RuntimeError, create_default_events_process_registry, log_dir="some_path")
        self.assertEqual(get_default_events_process_registry(), not None)

    @unittest.mock.patch("sdcm.sct_events.events_processes._EVENTS_PROCESSES", None)
    def test_create_default_registry(self):
        self.assertEqual(get_default_events_process_registry(not None), not None)
        self.assertRaises(RuntimeError, get_default_events_process_registry)
        registry = create_default_events_process_registry(log_dir="some_path")
        self.assertEqual(registry.log_dir, Path("some_path"))
        self.assertEqual(registry.default, True)
        self.assertIs(get_default_events_process_registry(), registry)
