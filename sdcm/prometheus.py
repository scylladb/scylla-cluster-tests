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
import datetime
import threading
from typing import Optional
from http.server import HTTPServer
from socketserver import ThreadingMixIn

import requests
import prometheus_client

from sdcm.sct_events.base import EventPeriod
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry
from sdcm.sct_events.monitors import PrometheusAlertManagerEvent
from sdcm.utils.decorators import retrying, log_run_info
from sdcm.utils.net import get_my_ip

START = 'start'
STOP = 'stop'


LOGGER = logging.getLogger(__name__)
NM_OBJ = {}


class _ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    """Thread per request HTTP server."""


def start_http_server(port, addr='', registry=prometheus_client.REGISTRY):
    """Starts an HTTP server for prometheus metrics as a daemon thread"""
    custom_metrics_handler = prometheus_client.MetricsHandler.factory(registry)
    httpd = _ThreadingSimpleServer((addr, port), custom_metrics_handler)
    http_thread = threading.Thread(target=httpd.serve_forever, name='HttpServerThread', daemon=True)
    http_thread.start()
    return httpd


def start_metrics_server():
    """
    https://github.com/prometheus/prometheus/wiki/Default-port-allocations
    Occupied port 9389 for SCT
    """
    try:
        LOGGER.debug('Try to start prometheus API server')
        httpd = start_http_server(0)
        port = httpd.server_port
        ip = get_my_ip()
        LOGGER.info('prometheus API server running on port: %s', port)
        return '{}:{}'.format(ip, port)
    except Exception as ex:  # noqa: BLE001
        LOGGER.error('Cannot start local http metrics server: %s', ex)

    return None


def nemesis_metrics_obj(metric_name_suffix=''):
    global NM_OBJ  # noqa: PLW0602
    if not NM_OBJ.get(metric_name_suffix):
        NM_OBJ[metric_name_suffix] = NemesisMetrics(metric_name_suffix)
    return NM_OBJ[metric_name_suffix]


class NemesisMetrics:
    # NOTE: make sure that the following attr is used in the grafana dashboard
    #       and as an expression like the following:
    #       {__name__=~'nemesis(.*)(?:gauge)(.*)'}
    NAME_PREFIX = 'nemesis'

    def __init__(self, metric_name_suffix=''):
        name = self.NAME_PREFIX
        if metric_name_suffix:
            name += f"_{metric_name_suffix.replace('-', '_')}"
        self._disrupt_counter = self.create_counter(
            f"{name}_counter",
            'Counter for nemesis disruption methods',
            ['method', 'event'])
        self._disrupt_gauge = self.create_gauge(
            f"{name}_gauge",
            'Gauge for nemesis disruption methods',
            ['method'])

    @staticmethod
    def create_counter(name, desc, param_list):
        try:
            return prometheus_client.Counter(name, desc, param_list)
        except Exception as ex:  # noqa: BLE001
            LOGGER.error('Cannot create metrics counter: %s', ex)
        return None

    @staticmethod
    def create_gauge(name, desc, param_list):
        try:
            return prometheus_client.Gauge(name, desc, param_list)
        except Exception as ex:  # noqa: BLE001
            LOGGER.error('Cannot create metrics gauge: %s', ex)
        return None

    def event_start(self, disrupt):
        try:
            self._disrupt_counter.labels(disrupt, START).inc()
            self._disrupt_gauge.labels(disrupt).inc()
        except Exception as ex:
            LOGGER.exception('Cannot start metrics event: %s', ex)

    def event_stop(self, disrupt):
        try:
            self._disrupt_counter.labels(disrupt, STOP).inc()
            self._disrupt_gauge.labels(disrupt).dec()
        except Exception as ex:
            LOGGER.exception('Cannot stop metrics event: %s', ex)


class PrometheusAlertManagerListener(threading.Thread):

    def __init__(self, ip, port=9093, interval=10, stop_flag: threading.Event = None):
        super().__init__(name=self.__class__.__name__, daemon=True)
        self._alert_manager_url = f"http://{ip}:{port}/api/v2"
        self._stop_flag = stop_flag if stop_flag else threading.Event()
        self._interval = interval
        self._timeout = 600
        self.event_registry = ContinuousEventsRegistry()

    @property
    def is_alert_manager_up(self):
        try:
            return requests.get(f"{self._alert_manager_url}/status", timeout=3).json()['cluster']['status'] == 'ready'
        except Exception:  # noqa: BLE001
            return False

    @log_run_info
    def wait_till_alert_manager_up(self):
        end_time = time.time() + self._timeout
        while time.time() < end_time and not self._stop_flag.is_set():
            if self.is_alert_manager_up:
                return
            time.sleep(30)
        if self._stop_flag.is_set():
            LOGGER.warning("Prometheus Alert Manager was asked to stop.")
        else:
            raise TimeoutError(f"Prometheus Alert Manager({self._alert_manager_url}) "
                               f"did not get up for {self._timeout}s")

    @log_run_info
    def stop(self):
        self._stop_flag.set()

    @retrying(n=10)
    def _get_alerts(self, active=False):
        if active:
            response = requests.get(f"{self._alert_manager_url}/alerts?active={int(active)}", timeout=3)
        else:
            response = requests.get(f"{self._alert_manager_url}/alerts", timeout=3)
        if response.status_code == 200:
            return response.json()
        return None

    def _publish_new_alerts(self, alerts: dict):
        for alert in alerts.values():
            PrometheusAlertManagerEvent(raw_alert=alert).begin_event()

    def _publish_end_of_alerts(self, alerts: dict):
        all_alerts = self._get_alerts()
        updated_dict = {}
        if all_alerts:
            for alert in all_alerts:
                fingerprint = alert.get('fingerprint', None)
                if not fingerprint:
                    continue
                updated_dict[fingerprint] = alert
        for alert in alerts.values():
            if not alert.get('endsAt', None):
                alert['endsAt'] = time.strftime("%Y-%m-%dT%H:%M:%S.0Z", time.gmtime())
            alert = updated_dict.get(alert['fingerprint'], alert)  # noqa: PLW2901
            labels = alert.get("labels") or {}
            alert_name = labels.get("alertname", "")
            node = labels.get("instance", "N/A")

            continuous_hash = PrometheusAlertManagerEvent.get_continuous_hash_from_dict({
                'node': node,
                'starts_at': alert.get("startsAt"),
                'alert_name': alert_name
            })

            if begin_event := self.event_registry.find_continuous_events_by_hash(continuous_hash):
                begin_event[-1].end_event()
                continue

            new_event = PrometheusAlertManagerEvent(raw_alert=alert)
            new_event.period_type = EventPeriod.INFORMATIONAL.value
            new_event.end_event()

    def run(self):
        self.wait_till_alert_manager_up()
        existed = {}
        while not self._stop_flag.is_set():
            start_time = time.time()
            just_left = existed.copy()
            existing = {}
            new_ones = {}
            alerts = self._get_alerts(active=True)
            if alerts is not None:
                for alert in alerts:
                    fingerprint = alert.get('fingerprint', None)
                    if not fingerprint:
                        continue
                    state = alert.get('status', {}).get('state', '')
                    if state == 'suppressed':
                        continue
                    existing[fingerprint] = alert
                    if fingerprint in just_left:
                        del just_left[fingerprint]
                        continue
                    new_ones[fingerprint] = alert
                existed = existing
            self._publish_new_alerts(new_ones)
            self._publish_end_of_alerts(just_left)
            delta = int((start_time + self._interval) - time.time())
            if delta > 0:
                time.sleep(int(delta))

    def silence(self,
                alert_name: str,
                duration: Optional[int] = None,
                start: Optional[datetime.datetime] = None,
                end: Optional[datetime.datetime] = None) -> str:
        """
        Silence an alert for a duration of time

        :param alert_name: name of the alert as it configured in prometheus
        :param duration: duration time in seconds, if None, start and end must be defined
        :param start: if None, would be default to current utc time
        :param end: if None, will be calculated by duration
        :return: silenceID
        """

        assert duration or (start and end), "should define duration or (start and end)"
        if not start:
            start = datetime.datetime.utcnow()
        if not end:
            end = start + datetime.timedelta(seconds=duration)
        silence_data = {
            "matchers": [
                {
                    "name": "alertname",
                    "value": alert_name,
                    "isRegex": True
                }
            ],
            "startsAt": start.isoformat("T") + "Z",
            "endsAt": end.isoformat("T") + "Z",
            "createdBy": "SCT",
            "comment": "Silence by SCT code",
            "status": {
                "state": "active"
            }
        }
        res = requests.post(f"{self._alert_manager_url}/silences", timeout=3, json=silence_data)
        res.raise_for_status()
        return res.json()['silenceID']

    def delete_silence(self, silence_id: str) -> None:
        """
        delete a alert silence

        :param silence_id: silence id returned from `silence()` api call
        :return:
        """
        res = requests.delete(f"{self._alert_manager_url}/silence/{silence_id}", timeout=3)
        res.raise_for_status()


class AlertSilencer:

    def __init__(self,
                 alert_manager: PrometheusAlertManagerListener,
                 alert_name: str,
                 duration: Optional[int] = None,
                 start: Optional[datetime.datetime] = None,
                 end: Optional[datetime.datetime] = None):
        self.alert_manager = alert_manager
        self.alert_name = alert_name
        self.duration = duration or 86400  # 24h
        self.start = start
        self.end = end
        self.silence_id = None

    def __enter__(self):
        self.silence_id = self.alert_manager.silence(self.alert_name, self.duration, self.start, self.end)

    def __exit__(self, *args):
        self.alert_manager.delete_silence(self.silence_id)
