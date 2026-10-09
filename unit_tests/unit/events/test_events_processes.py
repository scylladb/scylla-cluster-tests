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

from sdcm.sct_events.event_handler import EventsHandler, start_events_handler
from sdcm.sct_events.events_analyzer import EventsAnalyzer, start_events_analyzer
from sdcm.sct_events.events_processes import (
    EVENTS_ANALYZER_ID,
    EVENTS_FILE_LOGGER_ID,
    EVENTS_GRAFANA_AGGREGATOR_ID,
    EVENTS_GRAFANA_ANNOTATOR_ID,
    EVENTS_GRAFANA_POSTMAN_ID,
    EVENTS_HANDLER_ID,
    EventsProcessesRegistry,
    create_default_events_process_registry,
    get_default_events_process_registry,
    get_events_process,
)
from sdcm.sct_events.file_logger import EventsFileLogger, start_events_logger
from sdcm.sct_events.grafana import (
    GrafanaAnnotator,
    GrafanaEventAggregator,
    GrafanaEventPostman,
    start_grafana_pipeline,
)


class FakeProcess:
    """Minimal events process recording whether the registry started it."""

    def __init__(self, _registry=None):
        self._registry = _registry
        self.started = False

    def start(self):
        self.started = True


@pytest.fixture
def processes_registry():
    """Provide an empty, non-default events processes registry."""
    return EventsProcessesRegistry("some_path")


def test_fresh(processes_registry):
    """A new registry holds no processes and exposes its log dir as a Path."""
    assert processes_registry._registry_dict == {}
    assert processes_registry.log_dir == Path("some_path")


def test_start_events_process(processes_registry):
    """start_events_process creates, registers and starts the process bound to this registry."""
    processes_registry.start_events_process("test", FakeProcess)
    assert len(processes_registry._registry_dict) == 1
    assert "test" in processes_registry._registry_dict
    assert processes_registry._registry_dict["test"]._registry == processes_registry
    assert processes_registry._registry_dict["test"].started


def test_get_events_process(processes_registry):
    """get_events_process returns None for unknown names and the started process once registered."""
    process = processes_registry.get_events_process("test")
    assert process is None
    processes_registry.start_events_process("test", FakeProcess)
    process = processes_registry.get_events_process("test")
    assert process._registry == processes_registry
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


@pytest.mark.parametrize(
    "start_function, expected_processes",
    [
        pytest.param(start_events_logger, {EVENTS_FILE_LOGGER_ID: EventsFileLogger}, id="file-logger"),
        pytest.param(
            start_grafana_pipeline,
            {
                EVENTS_GRAFANA_ANNOTATOR_ID: GrafanaAnnotator,
                EVENTS_GRAFANA_AGGREGATOR_ID: GrafanaEventAggregator,
                EVENTS_GRAFANA_POSTMAN_ID: GrafanaEventPostman,
            },
            id="grafana-pipeline",
        ),
        pytest.param(start_events_analyzer, {EVENTS_ANALYZER_ID: EventsAnalyzer}, id="events-analyzer"),
        pytest.param(start_events_handler, {EVENTS_HANDLER_ID: EventsHandler}, id="events-handler"),
    ],
)
def test_start_subscriber_registers_running_process_on_given_registry(
    main_events_context, start_events_subscriber, start_function, expected_processes
):
    """Each subscriber start function registers running processes bound to the registry it was given."""
    start_events_subscriber(start_function, next(iter(expected_processes)))

    for process_id, process_class in expected_processes.items():
        process = get_events_process(process_id, _registry=main_events_context.events_processes_registry)
        assert isinstance(process, process_class)
        assert process.is_alive()
        assert process._registry == main_events_context.events_processes_registry
        assert process._registry == main_events_context.events_main_device._registry
