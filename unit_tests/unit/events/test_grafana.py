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

import time
import unittest.mock

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.setup import EVENTS_PROCESS_STOP_TIMEOUT, EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.sct_events.grafana import (
    GrafanaAnnotator,
    GrafanaEventAggregator,
    GrafanaEventPostman,
    get_grafana_postman,
    set_grafana_url,
    start_grafana_pipeline,
    start_posting_grafana_annotations,
)
from sdcm.sct_events.events_processes import (
    EVENTS_GRAFANA_ANNOTATOR_ID,
    EVENTS_GRAFANA_AGGREGATOR_ID,
    get_events_process,
)
from sdcm.wait import wait_for


def test_grafana(main_events_context):
    start_grafana_pipeline(_registry=main_events_context.events_processes_registry)
    grafana_annotator = get_events_process(
        EVENTS_GRAFANA_ANNOTATOR_ID, _registry=main_events_context.events_processes_registry
    )
    grafana_aggregator = get_events_process(
        EVENTS_GRAFANA_AGGREGATOR_ID, _registry=main_events_context.events_processes_registry
    )
    grafana_postman = get_grafana_postman(_registry=main_events_context.events_processes_registry)

    time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

    try:
        assert isinstance(grafana_annotator, GrafanaAnnotator)
        assert grafana_annotator.is_alive()
        assert grafana_annotator._registry == main_events_context.events_main_device._registry
        assert grafana_annotator._registry == main_events_context.events_processes_registry

        assert isinstance(grafana_aggregator, GrafanaEventAggregator)
        assert grafana_aggregator.is_alive()
        assert grafana_aggregator._registry == main_events_context.events_main_device._registry
        assert grafana_aggregator._registry == main_events_context.events_processes_registry

        assert isinstance(grafana_postman, GrafanaEventPostman)
        assert grafana_postman.is_alive()
        assert grafana_postman._registry == main_events_context.events_main_device._registry
        assert grafana_postman._registry == main_events_context.events_processes_registry

        # The aggregator's default 90 s time window is kept as-is, so every event published
        # below is aggregated inside one window and only the first `max_duplicates` of them
        # are posted.  Shortening the window made the expected count depend on where the wall
        # clock happened to fall while a batch was being delivered; the rollover between
        # windows is covered by test_grafana_duplicate_in_the_next_time_window_is_posted_again instead.
        published_events = 3 * 10
        expected_annotations = grafana_aggregator.max_duplicates

        set_grafana_url("http://localhost", _registry=main_events_context.events_processes_registry)
        with unittest.mock.patch("requests.post") as mock:
            for _ in range(3):
                with main_events_context.wait_for_n_events(grafana_annotator, count=10):
                    for _ in range(10):
                        main_events_context.events_main_device.publish_event(
                            ClusterHealthValidatorEvent.NodeStatus(severity=Severity.NORMAL)
                        )

            # Nothing may be posted before the Grafana URL has been announced.
            assert mock.call_count == 0

            start_posting_grafana_annotations(_registry=main_events_context.events_processes_registry)
            wait_for(lambda: mock.call_count == expected_annotations, timeout=10, step=0.1, throw_exc=False)

            # Duplicates of the same annotation are capped at `max_duplicates` per time window.
            assert mock.call_count == expected_annotations
            assert mock.call_args.kwargs["json"]["tags"] == [
                "ClusterHealthValidatorEvent",
                "NORMAL",
                "events",
                "NodeStatus",
            ]

        # The suppressed duplicates still have to reach the aggregator, and only the first
        # `max_duplicates` of them were posted, so wait for it to drain before counting.
        wait_for(
            lambda: grafana_aggregator.events_counter == published_events,
            timeout=10,
            step=0.1,
            throw_exc=False,
        )

        assert main_events_context.events_main_device.events_counter == grafana_annotator.events_counter
        assert grafana_annotator.events_counter == grafana_aggregator.events_counter
        assert grafana_postman.events_counter <= grafana_aggregator.events_counter
    finally:
        grafana_annotator.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
        grafana_aggregator.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
        grafana_postman.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)


def test_grafana_duplicate_in_the_next_time_window_is_posted_again(main_events_context):
    """The duplicate cap is per time window, so a duplicate seen in the next one is posted again."""
    time_window = 0.2
    start_grafana_pipeline(_registry=main_events_context.events_processes_registry)
    grafana_annotator = get_events_process(
        EVENTS_GRAFANA_ANNOTATOR_ID, _registry=main_events_context.events_processes_registry
    )
    grafana_aggregator = get_events_process(
        EVENTS_GRAFANA_AGGREGATOR_ID, _registry=main_events_context.events_processes_registry
    )
    grafana_postman = get_grafana_postman(_registry=main_events_context.events_processes_registry)

    time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

    try:
        # A single event fills a window, so no assertion here depends on a batch of events
        # landing on the same side of a window boundary.
        grafana_aggregator.max_duplicates = 1
        grafana_aggregator.time_window = time_window

        set_grafana_url("http://localhost", _registry=main_events_context.events_processes_registry)
        start_posting_grafana_annotations(_registry=main_events_context.events_processes_registry)

        with unittest.mock.patch("requests.post") as mock:
            for _ in range(2):
                with main_events_context.wait_for_n_events(grafana_annotator, count=1):
                    main_events_context.events_main_device.publish_event(
                        ClusterHealthValidatorEvent.NodeStatus(severity=Severity.NORMAL)
                    )
                # Outlast the window, so the next duplicate is counted against a fresh one.
                # `wait_for_n_events` already waited out `last_event_processing_delay` on top.
                time.sleep(time_window * 5)

            wait_for(lambda: mock.call_count == 2, timeout=10, step=0.1, throw_exc=False)
            assert mock.call_count == 2, (
                "a duplicate annotation seen after the time window expired must be posted again"
            )
    finally:
        grafana_annotator.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
        grafana_aggregator.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
        grafana_postman.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)
