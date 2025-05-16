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
import logging
import threading
from typing import NewType, Dict, Any, Tuple, Optional, Callable, cast
from functools import partial
from collections import defaultdict

import requests

from sdcm.sct_events.events_processes import \
    EVENTS_GRAFANA_ANNOTATOR_ID, EVENTS_GRAFANA_AGGREGATOR_ID, EVENTS_GRAFANA_POSTMAN_ID, \
    EventsProcessesRegistry, BaseEventsProcess, EventsProcessPipe, \
    start_events_process, get_events_process, verbose_suppress


GRAFANA_EVENT_AGGREGATOR_TIME_WINDOW: float = 90  # seconds
GRAFANA_EVENT_AGGREGATOR_MAX_DUPLICATES: int = 5
GRAFANA_EVENT_AGGREGATOR_QUEUE_WAIT_TIMEOUT: float = 1  # seconds
GRAFANA_ANNOTATIONS_API_ENDPOINT: str = "/api/annotations"
GRAFANA_ANNOTATIONS_API_AUTH: Tuple[str, str] = ("admin", "admin", )

LOGGER = logging.getLogger(__name__)


Annotation = NewType("Annotation", Dict[str, Any])
AnnotationKey = NewType("AnnotationKey", Tuple[str, ...])


class GrafanaAnnotator(EventsProcessPipe[Tuple[str, Any], Annotation]):
    def run(self) -> None:
        for event_tuple in self.inbound_events():
            with verbose_suppress("GrafanaAnnotator failed to process %s", event_tuple):
                event_class, event = event_tuple  # try to unpack event from EventsDevice
                if not event.publish_to_grafana:
                    continue
                tags = [event_class, event.severity.name, "events", ]
                if event_type := getattr(event, "type", None):
                    tags.append(event_type)
                if event_subtype := getattr(event, "subtype", None):
                    tags.append(event_subtype)
                self.outbound_queue.put(
                    Annotation({
                        "time": int(event.timestamp * 1000.0),
                        "tags": tags,
                        "isRegion": False,
                        "text": str(event),
                    })
                )


class GrafanaEventAggregator(EventsProcessPipe[Annotation, Annotation]):
    inbound_events_process = EVENTS_GRAFANA_ANNOTATOR_ID
    time_window = GRAFANA_EVENT_AGGREGATOR_TIME_WINDOW
    max_duplicates = GRAFANA_EVENT_AGGREGATOR_MAX_DUPLICATES

    def run(self) -> None:
        time_window_counters: Dict[AnnotationKey, int] = defaultdict(int)
        time_window_end = time.perf_counter()

        for annotation in self.inbound_events():
            with verbose_suppress("GrafanaEventAggregator failed to process an annotation %s", annotation):
                annotation_key = self.unique_key(annotation)
                time_diff = time.perf_counter() - time_window_end

                # The current time window expired.
                if time_diff > 0:
                    LOGGER.debug("GrafanaEventAggregator start a new time window (%s sec)", self.time_window)
                    time_window_counters.clear()

                    # It can be more than one time window expired since last event seen.
                    time_window_end += (time_diff // self.time_window + 1) * self.time_window

                time_window_counters[annotation_key] += 1
                if time_window_counters[annotation_key] > self.max_duplicates:
                    continue

                # Put the annotation to the posting queue.
                self.outbound_queue.put(annotation)

    @staticmethod
    def unique_key(annotation: Annotation) -> AnnotationKey:
        return AnnotationKey(tuple(annotation["tags"]))


class GrafanaEventPostman(BaseEventsProcess[Annotation, None], threading.Thread):
    inbound_events_process = EVENTS_GRAFANA_AGGREGATOR_ID
    api_endpoint = GRAFANA_ANNOTATIONS_API_ENDPOINT
    api_auth = GRAFANA_ANNOTATIONS_API_AUTH

    def __init__(self, _registry: EventsProcessesRegistry):
        self.url_set = threading.Event()
        self._grafana_post_urls = []
        super().__init__(_registry=_registry)

    def run(self) -> None:
        # Waiting until the monitor URL is set, and we can start using the API.
        self.url_set.wait()

        for annotation in self.inbound_events():  # events from GrafanaAggregator
            for grafana_post_url in self._grafana_post_urls:
                with verbose_suppress("GrafanaEventPostman failed to post an annotation to '%s' "
                                      "endpoint.\nAnnotation: %s", grafana_post_url, annotation):
                    requests.post(
                        grafana_post_url, json=annotation, auth=self.api_auth).raise_for_status()

    def set_grafana_url(self, grafana_base_url: str) -> None:
        if not grafana_base_url:
            LOGGER.error("Reject to set an empty Grafana URL")
            return
        self._grafana_post_urls.append(grafana_base_url + self.api_endpoint)

    def start_posting_grafana_annotations(self):
        self.url_set.set()

    def terminate(self) -> None:
        super().terminate()
        self.url_set.set()


start_grafana_annotator = partial(start_events_process, EVENTS_GRAFANA_ANNOTATOR_ID, GrafanaAnnotator)
start_grafana_aggregator = partial(start_events_process, EVENTS_GRAFANA_AGGREGATOR_ID, GrafanaEventAggregator)
start_grafana_postman = partial(start_events_process, EVENTS_GRAFANA_POSTMAN_ID, GrafanaEventPostman)
get_grafana_postman = cast(Callable[..., GrafanaEventPostman], partial(get_events_process, EVENTS_GRAFANA_POSTMAN_ID))


def start_grafana_pipeline(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    start_grafana_annotator(_registry=_registry)
    start_grafana_aggregator(_registry=_registry)
    start_grafana_postman(_registry=_registry)


def set_grafana_url(url: str, _registry: Optional[EventsProcessesRegistry] = None) -> None:
    get_grafana_postman(_registry=_registry).set_grafana_url(url)


def start_posting_grafana_annotations(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    get_grafana_postman(_registry=_registry).start_posting_grafana_annotations()


__all__ = ("start_grafana_pipeline", "set_grafana_url", "start_posting_grafana_annotations")
