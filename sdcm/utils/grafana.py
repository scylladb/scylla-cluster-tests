import datetime
import threading
from collections import defaultdict
from string import digits
import logging

import requests

LOGGER = logging.getLogger(__name__)


def normalize_text(text):
    """
    clearing digit from text, so it would be usable as a key to find duplicate events
    :param text:
    :return: text without digit
    """
    res = text.translate(str.maketrans('', '', digits))
    return res


class GrafanaEventAggragator(threading.Thread):
    """
    This is a background thread that scans grafana annotations in a `time_window` (in minutes)
    find `max_duplicated events in that time windows, and delete all of the others similar events
    """

    def __init__(self, params):
        self.stop_event = threading.Event()
        self.url_set = threading.Event()
        self.grafana_base_url = ''
        # TODO: add those to sct_events.py
        if params is None:
            params = dict()
        self.time_window = params.get('event_aggregator_time_window', 5)
        self.max_duplicates = params.get('event_aggregator_max_duplicates', 5)
        self.auth = ('admin', 'admin')
        super(GrafanaEventAggragator, self).__init__()

    def set_grafana_url(self, grafana_base_url):
        self.grafana_base_url = grafana_base_url
        self.url_set.set()

    def run(self):
        current_time = datetime.datetime.now()

        while self.stop_event is None or not self.stop_event.isSet():
            # waiting until the monitor url is set, and we can start using the api
            self.url_set.wait()
            if self.grafana_base_url:
                try:
                    duplication_counter = defaultdict(int)  # counter for specific message per this time window
                    # get a time window of events
                    annotations = self.get_grafana_annotations_by_time(current_time - datetime.timedelta(minutes=self.time_window),
                                                                       current_time)
                    current_time = current_time + datetime.timedelta(minutes=self.time_window)

                    # scan for duplicates and mark get ids for deletion
                    id_for_deletions = []
                    for annotation in annotations:
                        duplication_counter[self.unique_key(annotation)] += 1
                        if duplication_counter[self.unique_key(annotation)] > self.max_duplicates:
                            id_for_deletions.append(annotation['id'])

                    # delete extra annotation by id
                    LOGGER.debug(f"ids for deletions = {id_for_deletions}")
                    self.delete_grafana_annotations(id_for_deletions)

                except Exception as ex:  # pylint: disable=broad-except
                    LOGGER.warning("GrafanaEventAggragator failed [%s]", str(ex))

                # wait `time_window` time in minutes
                LOGGER.debug(f"GrafanaEventAggragator sleeping for {self.time_window}min")
                self.stop_event.wait(60 * self.time_window)

    @staticmethod
    def unique_key(annotation):
        return tuple(annotation['tags']), normalize_text(annotation['text'])

    def terminate(self):
        self.url_set.set()
        self.stop_event.set()

    def get_grafana_annotations_by_time(self, start, end):
        annotations_url = "{0.grafana_base_url}/api/annotations"
        try:
            res = requests.get(annotations_url.format(self), params={'from': int(start.timestamp() * 1000), 'to': int(end.timestamp() * 1000)},
                               headers={'Content-Type': 'application/json'}, auth=self.auth)

            res.raise_for_status()
            if res.json():
                return res.json()
            else:
                LOGGER.warning('empty data')
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("unable to get grafana annotations [%s]", str(ex))
            raise
        return dict()

    def delete_grafana_annotations(self, ids):
        annotations_url = "{0.grafana_base_url}/api/annotations/{id}"
        for _id in ids:
            try:
                requests.get(annotations_url.format(self, id=_id),
                             headers={'Content-Type': 'application/json'},
                             auth=self.auth)
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.warning("unable to delete grafana annotations [%s]", str(ex))
