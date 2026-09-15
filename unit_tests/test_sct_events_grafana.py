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
import unittest
import unittest.mock

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
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

from unit_tests.lib.events_utils import EventsUtilsMixin


class TestGrafana(unittest.TestCase, EventsUtilsMixin):
    @classmethod
    def setUpClass(cls) -> None:
        cls.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.teardown_events_processes()

    def test_grafana(self):
        start_grafana_pipeline(_registry=self.events_processes_registry)
        grafana_annotator = get_events_process(EVENTS_GRAFANA_ANNOTATOR_ID, _registry=self.events_processes_registry)
        grafana_aggregator = get_events_process(EVENTS_GRAFANA_AGGREGATOR_ID, _registry=self.events_processes_registry)
        grafana_postman = get_grafana_postman(_registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            self.assertIsInstance(grafana_annotator, GrafanaAnnotator)
            self.assertTrue(grafana_annotator.is_alive())
            self.assertEqual(grafana_annotator._registry, self.events_main_device._registry)
            self.assertEqual(grafana_annotator._registry, self.events_processes_registry)

            self.assertIsInstance(grafana_aggregator, GrafanaEventAggregator)
            self.assertTrue(grafana_aggregator.is_alive())
            self.assertEqual(grafana_aggregator._registry, self.events_main_device._registry)
            self.assertEqual(grafana_aggregator._registry, self.events_processes_registry)

            self.assertIsInstance(grafana_postman, GrafanaEventPostman)
            self.assertTrue(grafana_postman.is_alive())
            self.assertEqual(grafana_postman._registry, self.events_main_device._registry)
            self.assertEqual(grafana_postman._registry, self.events_processes_registry)

            # The aggregator's default 90 s time window is kept as-is, so every event published
            # below is aggregated inside one window and only the first `max_duplicates` of them
            # are posted.  Shortening the window made the expected count depend on where the wall
            # clock happened to fall while a batch was being delivered; the rollover between
            # windows is covered by TestGrafanaAggregatorTimeWindow instead.
            published_events = 3 * 10
            expected_annotations = grafana_aggregator.max_duplicates

            set_grafana_url("http://localhost", _registry=self.events_processes_registry)
            with unittest.mock.patch("requests.post") as mock:
                for _ in range(3):
                    with self.wait_for_n_events(grafana_annotator, count=10):
                        for _ in range(10):
                            self.events_main_device.publish_event(
                                ClusterHealthValidatorEvent.NodeStatus(severity=Severity.NORMAL)
                            )
<<<<<<< HEAD:unit_tests/test_sct_events_grafana.py
                    time.sleep(1)
                self.assertEqual(mock.call_count, 0)
||||||| parent of 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py
                    time.sleep(1)
                assert mock.call_count == 0
=======

                # Nothing may be posted before the Grafana URL has been announced.
                assert mock.call_count == 0
>>>>>>> 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py

                start_posting_grafana_annotations(_registry=self.events_processes_registry)
                wait_for(lambda: mock.call_count == expected_annotations, timeout=10, step=0.1, throw_exc=False)

<<<<<<< HEAD:unit_tests/test_sct_events_grafana.py
                self.assertEqual(mock.call_count, runs * 5)
                self.assertEqual(
                    mock.call_args.kwargs["json"]["tags"],
                    ["ClusterHealthValidatorEvent", "NORMAL", "events", "NodeStatus"],
                )
||||||| parent of 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py
                assert mock.call_count == runs * 5
                assert mock.call_args.kwargs["json"]["tags"] == [
                    "ClusterHealthValidatorEvent",
                    "NORMAL",
                    "events",
                    "NodeStatus",
                ]
=======
                # Duplicates of the same annotation are capped at `max_duplicates` per time window.
                assert mock.call_count == expected_annotations
                assert mock.call_args.kwargs["json"]["tags"] == [
                    "ClusterHealthValidatorEvent",
                    "NORMAL",
                    "events",
                    "NodeStatus",
                ]
>>>>>>> 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py

<<<<<<< HEAD:unit_tests/test_sct_events_grafana.py
            self.assertEqual(self.events_main_device.events_counter, grafana_annotator.events_counter)
            self.assertEqual(grafana_annotator.events_counter, grafana_aggregator.events_counter)
            self.assertLessEqual(grafana_postman.events_counter, grafana_aggregator.events_counter)
||||||| parent of 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py
            assert self.events_main_device.events_counter == grafana_annotator.events_counter
            assert grafana_annotator.events_counter == grafana_aggregator.events_counter
            assert grafana_postman.events_counter <= grafana_aggregator.events_counter
=======
            # The suppressed duplicates still have to reach the aggregator, and only the first
            # `max_duplicates` of them were posted, so wait for it to drain before counting.
            wait_for(
                lambda: grafana_aggregator.events_counter == published_events,
                timeout=10,
                step=0.1,
                throw_exc=False,
            )

            assert self.events_main_device.events_counter == grafana_annotator.events_counter
            assert grafana_annotator.events_counter == grafana_aggregator.events_counter
            assert grafana_postman.events_counter <= grafana_aggregator.events_counter
>>>>>>> 0c18ab336 (fix(unit_tests): de-flake TestGrafana, the events publisher is the bottleneck):unit_tests/unit/test_sct_events_grafana.py
        finally:
            grafana_annotator.stop(timeout=1)
            grafana_aggregator.stop(timeout=1)
            grafana_postman.stop(timeout=1)


class TestGrafanaAggregatorTimeWindow(EventsUtilsMixin):
    """The duplicate cap is per time window, so a duplicate seen in the next one is posted again."""

    time_window = 0.2

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)

    @classmethod
    def teardown_class(cls) -> None:
        cls.teardown_events_processes()

    def test_duplicate_in_the_next_time_window_is_posted_again(self):
        start_grafana_pipeline(_registry=self.events_processes_registry)
        grafana_annotator = get_events_process(EVENTS_GRAFANA_ANNOTATOR_ID, _registry=self.events_processes_registry)
        grafana_aggregator = get_events_process(EVENTS_GRAFANA_AGGREGATOR_ID, _registry=self.events_processes_registry)
        grafana_postman = get_grafana_postman(_registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            # A single event fills a window, so no assertion here depends on a batch of events
            # landing on the same side of a window boundary.
            grafana_aggregator.max_duplicates = 1
            grafana_aggregator.time_window = self.time_window

            set_grafana_url("http://localhost", _registry=self.events_processes_registry)
            start_posting_grafana_annotations(_registry=self.events_processes_registry)

            with unittest.mock.patch("requests.post") as mock:
                for _ in range(2):
                    with self.wait_for_n_events(grafana_annotator, count=1):
                        self.events_main_device.publish_event(
                            ClusterHealthValidatorEvent.NodeStatus(severity=Severity.NORMAL)
                        )
                    # Outlast the window, so the next duplicate is counted against a fresh one.
                    # `wait_for_n_events` already waited out `last_event_processing_delay` on top.
                    time.sleep(self.time_window * 5)

                wait_for(lambda: mock.call_count == 2, timeout=10, step=0.1, throw_exc=False)
                assert mock.call_count == 2, (
                    "a duplicate annotation seen after the time window expired must be posted again"
                )
        finally:
            grafana_annotator.stop(timeout=1)
            grafana_aggregator.stop(timeout=1)
            grafana_postman.stop(timeout=1)
