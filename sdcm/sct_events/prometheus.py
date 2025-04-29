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

"""
This is an example of how we'll send info into Prometheus.

Currently it's not in use, since the data we want to show, doesn't fit Prometheus model,
we are using the GrafanaAnnotator
"""

import logging
import threading
from typing import Tuple, Any

from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events.events_processes import BaseEventsProcess, verbose_suppress


LOGGER = logging.getLogger(__name__)


class PrometheusDumper(BaseEventsProcess[Tuple[str, Any], None], threading.Thread):
    def run(self) -> None:
        events_gauge = \
            nemesis_metrics_obj().create_gauge("sct_events_gauge",
                                               "Gauge for SCT events",
                                               ["event_type", "type", "subtype", "severity", "node", ])

        for event_tuple in self.inbound_events():
            with verbose_suppress("PrometheusDumper failed to process %s", event_tuple):
                event_class, event = event_tuple  # try to unpack event from EventsDevice
                events_gauge.labels(event_class,
                                    getattr(event, "type", ""),
                                    getattr(event, "subtype", ""),
                                    event.severity,
                                    getattr(event, "node", "")).set(event.event_timestamp)
