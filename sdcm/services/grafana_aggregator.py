from collections import defaultdict
import threading
import requests

from sdcm.services.base import DetachThreadService


class GrafanaEventAggragator(DetachThreadService):
    """
    This is a background thread that scans grafana annotations in a `time_window` (in minutes)
    find `max_duplicated events in that time windows, and delete all of the others similar events
    """
    _interval = 90
    stop_priority = 60

    def __init__(self, max_duplicates=5):
        self.url_set = threading.Event()
        self.grafana_base_url = ''
        self.max_duplicates = max_duplicates
        self.annotations = list()
        self.auth = ('admin', 'admin')
        super().__init__()

    def set_grafana_url(self, grafana_base_url):
        self.grafana_base_url = grafana_base_url
        self.url_set.set()

    def _service_body(self):
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
                self._log.warning(f"GrafanaEventAggragator failed [{str(ex)}]")

    @staticmethod
    def unique_key(annotation):
        return tuple(annotation['tags'])

    def stop(self):
        self.url_set.set()
        super().stop()

    def store_annotation(self, annotate_data):
        self.annotations.append(annotate_data)

    def post_annotation(self, annotate_data):
        try:
            res = requests.post(self.grafana_base_url + '/api/annotations', json=annotate_data, auth=self.auth)
            res.raise_for_status()
        except requests.exceptions.RequestException as ex:
            self._log.warning(f"Failed to annotate an event in grafana [{str(ex)}]")
