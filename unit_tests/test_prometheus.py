import os
import time
import unittest
import tempfile
import shutil
import json
import requests

from sdcm.sct_events import stop_events_device, start_events_device
from sdcm.prometheus import PrometheusAlertManagerListener, PrometheusAlertManagerEvent


class PrometheusAlertManagerListenerArtificialTest(PrometheusAlertManagerListener):
    interval = 0

    def __init__(self, artificial_alerts: list):
        super().__init__('', interval=0)
        self._artificial_alerts = artificial_alerts
        self._iter = -1
        self._results = {'_publish_end_of_alerts': [], '_publish_new_alerts': []}

    def _get_alerts(self, active=True, max_attempts=10):
        self._iter += 1
        if self._iter < len(self._artificial_alerts):
            return self._artificial_alerts[0:self._iter]
        if self._iter > len(self._artificial_alerts) * 2:
            self._stop_flag.set()
            return []
        return self._artificial_alerts[self._iter - len(self._artificial_alerts):len(self._artificial_alerts)]

    def _publish_end_of_alerts(self, alerts: dict):
        self._results['_publish_end_of_alerts'].append(alerts)

    def _publish_new_alerts(self, alerts: dict):
        self._results['_publish_new_alerts'].append(alerts)

    def get_result(self):
        return self._results

    def start(self):
        super().start()
        self._thread.join()

    def wait_till_alert_manager_up(self, timeout=60):
        pass


class PrometheusAlertManagerListenerRealTest(PrometheusAlertManagerListener):
    """
    TBD: To be fixed to work with dockerized prometheus
    TBD: Alert manager accept alerts only for known instances
    """
    # pylint: disable=too-many-instance-attributes

    def __init__(self,  # pylint: disable=too-many-arguments
                 ip, port=9093, artificial_alerts: list = None, events_wait_timeout=60, tmp_dir='/tmp/logdir'):
        self._artificial_alerts: list = artificial_alerts
        self._alert_manager_url = f"http://{ip}:{port}/api/v2"
        self._alerts_endpoint = f"{self._alert_manager_url}/alerts"
        self._tmp_dir = tmp_dir
        self._events_wait_timeout = events_wait_timeout
        self._current_alert_to_post: int = -1
        self._event_log_file = None
        self._result_data = None
        super().__init__(ip, port, interval=0)

    def start(self):
        shutil.rmtree(self._tmp_dir)
        start_events_device('/tmp/logdir')
        self._event_log_file = None
        while self._event_log_file is None:
            try:
                self._event_log_file = open(f'{self._tmp_dir}/events_log/raw_events.log')
            except:  # pylint: disable=bare-except
                pass
        self._result_data = {
            'end_events': [],
            'start_events': []
        }
        super().start()

    def _post_alert(self, alert):
        response = requests.post(self._alerts_endpoint, json=[alert], timeout=3)
        if response.status_code != 200:
            raise RuntimeError(f"Can't post alerts to '{self._alert_manager_url}/alerts' "
                               f"status={response.status_code} response={response.content}")
        return True

    def post_next_alert(self):
        self._current_alert_to_post += 1
        if self._current_alert_to_post < len(self._artificial_alerts):
            return self._post_alert(self._artificial_alerts[self._current_alert_to_post])
        if self._current_alert_to_post > len(self._artificial_alerts) * 2:
            self._stop_flag.set()
            return False
        self._stop_flag.set()
        return False

    def wait_alert_event(self, event_type: str, timeout=None):
        if timeout is None:
            timeout = self._events_wait_timeout
        end_time = time.time() + timeout
        current_alert = PrometheusAlertManagerEvent(event_type=event_type,
                                                    raw_alert=self._artificial_alerts[self._current_alert_to_post])
        while True:
            if time.time() > end_time:
                raise TimeoutError(
                    f"Waiting for alert event #{self._current_alert_to_post} reached timeout: {str(current_alert)}")
            line = self._event_log_file.readline()
            if not line:
                continue
            event = PrometheusAlertManagerEvent()
            if not event._load_from_event_str(line):  # pylint: disable=protected-access
                continue
            if current_alert == event:
                return True

    def post_alerts_and_read_events(self, timeout=60):
        alert = self.post_next_alert()
        end_time = time.time() + timeout
        while alert is not False:
            self.wait_alert_event(event_type='start')
            alert = self.post_next_alert()
            if time.time() > end_time:
                raise TimeoutError("post_alerts_and_read_events reached timeout")

    def stop(self, timeout=None):
        super().stop(timeout)
        stop_events_device()


class PrometheusAlertManagerTest(unittest.TestCase):
    temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        # stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def test_alert_manager_listener_artificial_run(self):
        with open(os.path.join(os.path.dirname(__file__),
                               'test_data/test_prometheus/test_alert_manager_listener_artificial_run.yaml')) as file:
            test_data = json.load(file)
        listener = PrometheusAlertManagerListenerArtificialTest(artificial_alerts=test_data['post'])
        listener.start()
        result = listener.get_result()
        self.assertEqual(result, test_data['expected'])

    @unittest.skip("TBD: dockerized prometheuse to be added")
    def test_alert_manager_listener_real_run(self):  # pylint: disable=no-self-use
        with open(os.path.join(os.path.dirname(__file__),
                               'test_data/test_prometheus/test_alert_manager_listener_real_run.json')) as file:
            i = PrometheusAlertManagerListenerRealTest(
                '52.50.35.156',
                artificial_alerts=json.load(file),
                tmp_dir='/tmp/logdir',
                events_wait_timeout=30)
        i.start()
        i.post_alerts_and_read_events()
        i.stop()
