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


class TestGrafana(EventsUtilsMixin):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)

    @classmethod
    def teardown_class(cls) -> None:
        cls.teardown_events_processes()

    def test_grafana(self):
        start_grafana_pipeline(_registry=self.events_processes_registry)
        grafana_annotator = get_events_process(EVENTS_GRAFANA_ANNOTATOR_ID, _registry=self.events_processes_registry)
        grafana_aggregator = get_events_process(EVENTS_GRAFANA_AGGREGATOR_ID, _registry=self.events_processes_registry)
        grafana_postman = get_grafana_postman(_registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            assert isinstance(grafana_annotator, GrafanaAnnotator)
            assert grafana_annotator.is_alive()
            assert grafana_annotator._registry == self.events_main_device._registry
            assert grafana_annotator._registry == self.events_processes_registry

            assert isinstance(grafana_aggregator, GrafanaEventAggregator)
            assert grafana_aggregator.is_alive()
            assert grafana_aggregator._registry == self.events_main_device._registry
            assert grafana_aggregator._registry == self.events_processes_registry

            assert isinstance(grafana_postman, GrafanaEventPostman)
            assert grafana_postman.is_alive()
            assert grafana_postman._registry == self.events_main_device._registry
            assert grafana_postman._registry == self.events_processes_registry

            grafana_aggregator.time_window = 1

            set_grafana_url("http://localhost", _registry=self.events_processes_registry)
            with unittest.mock.patch("requests.post") as mock:
                for runs in range(1, 4):
                    with self.wait_for_n_events(grafana_annotator, count=10, timeout=1):
                        for _ in range(10):
                            self.events_main_device.publish_event(
                                ClusterHealthValidatorEvent.NodeStatus(severity=Severity.NORMAL)
                            )
                    time.sleep(1)
                assert mock.call_count == 0

                start_posting_grafana_annotations(_registry=self.events_processes_registry)
                wait_for(lambda: mock.call_count == runs * 5, timeout=10, step=0.1, throw_exc=False)

                assert mock.call_count == runs * 5
                assert mock.call_args.kwargs["json"]["tags"] == [
                    "ClusterHealthValidatorEvent",
                    "NORMAL",
                    "events",
                    "NodeStatus",
                ]

            assert self.events_main_device.events_counter == grafana_annotator.events_counter
            assert grafana_annotator.events_counter == grafana_aggregator.events_counter
            assert grafana_postman.events_counter <= grafana_aggregator.events_counter
        finally:
            grafana_annotator.stop(timeout=1)
            grafana_aggregator.stop(timeout=1)
            grafana_postman.stop(timeout=1)
