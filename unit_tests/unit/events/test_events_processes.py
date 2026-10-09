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

from pathlib import Path
from unittest.mock import patch

import pytest

from sdcm.sct_events.events_processes import (
    EventsProcessesRegistry,
    create_default_events_process_registry,
    get_default_events_process_registry,
)


class FakeProcess:
    """Minimal events process recording whether the registry started it."""

    def __init__(self, _registry=None):
        self._registry = _registry
        self.started = False

    def start(self):
        self.started = True


@pytest.fixture
def registry():
    """Provide an empty, non-default events processes registry."""
    return EventsProcessesRegistry("some_path")


def test_fresh(registry):
    """A new registry holds no processes and exposes its log dir as a Path."""
    assert registry._registry_dict == {}
    assert registry.log_dir == Path("some_path")


def test_start_events_process(registry):
    """start_events_process creates, registers and starts the process bound to this registry."""
    registry.start_events_process("test", FakeProcess)
    assert len(registry._registry_dict) == 1
    assert "test" in registry._registry_dict
    assert registry._registry_dict["test"]._registry == registry
    assert registry._registry_dict["test"].started


def test_get_events_process(registry):
    """get_events_process returns None for unknown names and the started process once registered."""
    process = registry.get_events_process("test")
    assert process is None
    registry.start_events_process("test", FakeProcess)
    process = registry.get_events_process("test")
    assert process._registry == registry
    assert process.started


@patch("sdcm.sct_events.events_processes._EVENTS_PROCESSES", not None)
def test_create_default_registry_exists():
    """Creating a second default registry raises, so one test run cannot have two default registries."""
    with pytest.raises(RuntimeError):
        create_default_events_process_registry(log_dir="some_path")
    assert get_default_events_process_registry() == (not None)


@patch("sdcm.sct_events.events_processes._EVENTS_PROCESSES", None)
def test_create_default_registry():
    """An explicit registry passes through; without one, the default is required and creation sets it."""
    assert get_default_events_process_registry(not None) == (not None)
    with pytest.raises(RuntimeError):
        get_default_events_process_registry()
    registry = create_default_events_process_registry(log_dir="some_path")
    assert registry.log_dir == Path("some_path")
    assert registry.default is True
    assert get_default_events_process_registry() is registry
