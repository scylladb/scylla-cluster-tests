import threading
from collections import defaultdict
import logging

import requests

from sdcm.sct_events import EVENTS_PROCESSES

LOGGER = logging.getLogger(__name__)


class GrafanaAnnotator(threading.Thread):
    def __init__(self):
        self.stop_event = threading.Event()
        super().__init__(daemon=True)

    def run(self):
        for event_class, message_data in EVENTS_PROCESSES['MainDevice'].subscribe_events(stop_event=self.stop_event):

            event_type = getattr(message_data, 'type', None)
            tags = [event_class, message_data.severity.name, 'events']
            if event_type:
                tags += [event_type]
            annotate_data = {
                "time": int(message_data.timestamp * 1000.0),
                "tags": tags,
                "isRegion": False,
                "text": str(message_data)
            }
            EVENTS_PROCESSES['EVENTS_GRAFANA_AGGRAGATOR'].store_annotation(annotate_data)

    def terminate(self):
        self.stop_event.set()


class GrafanaEventAggragator(threading.Thread):
    """
    This is a background thread that scans grafana annotations in a `time_window` (in minutes)
    find `max_duplicated events in that time windows, and delete all of the others similar events
    """

    def __init__(self, time_window_in_secs=90, max_duplicates=5):
        self.stop_event = threading.Event()
        self.url_set = threading.Event()
        self.grafana_base_url = ''

        self.time_window_in_secs = time_window_in_secs
        self.max_duplicates = max_duplicates
        self.annotations = list()
        self.auth = ('admin', 'admin')
        super().__init__(daemon=True)

    def set_grafana_url(self, grafana_base_url):
        self.grafana_base_url = grafana_base_url
        self.url_set.set()

    def run(self):
        while self.stop_event is None or not self.stop_event.isSet():
            # waiting until the monitor url is set, and we can start using the api
            self.url_set.wait()
            if self.grafana_base_url:
                try:
                    duplication_counter = defaultdict(int)  # counter for specific message per this time window

                    # get current time window of events
                    annotations = self.annotations[:]
                    self.annotations.clear()

                    # scan for duplicates in this time window, and send out only `max_duplicates` of them
                    for annotation in annotations:
                        duplication_counter[self.unique_key(annotation)] += 1
                        if duplication_counter[self.unique_key(annotation)] <= self.max_duplicates:
                            self.post_annotation(annotation)

                except Exception as ex:  # pylint: disable=broad-except
                    LOGGER.warning(f"GrafanaEventAggragator failed [{str(ex)}]")

                # wait `time_window` time in minutes
                LOGGER.debug(f"GrafanaEventAggragator sleeping for {self.time_window_in_secs}sec")
                self.stop_event.wait(self.time_window_in_secs)

    @staticmethod
    def unique_key(annotation):
        return tuple(annotation['tags'])

    def terminate(self):
        self.url_set.set()
        self.stop_event.set()

    def store_annotation(self, annotate_data):
        self.annotations.append(annotate_data)

    def post_annotation(self, annotate_data):
        try:
            res = requests.post(self.grafana_base_url + '/api/annotations', json=annotate_data, auth=self.auth)
            res.raise_for_status()
        except requests.exceptions.RequestException as ex:
            LOGGER.warning(f"Failed to annotate an event in grafana [{str(ex)}]")
