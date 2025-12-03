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

import os
import json
import unittest

from sdcm.prometheus import PrometheusAlertManagerListener


class PrometheusAlertManagerListenerArtificialTest(PrometheusAlertManagerListener):
    interval = 0

    def __init__(self, artificial_alerts: list):
        super().__init__("", interval=0)
        self._artificial_alerts = artificial_alerts
        self._iter = -1
        self._results = {"_publish_end_of_alerts": [], "_publish_new_alerts": []}

    def _get_alerts(self, active=True):
        self._iter += 1
        if self._iter < len(self._artificial_alerts):
            return self._artificial_alerts[0 : self._iter]
        if self._iter > len(self._artificial_alerts) * 2:
            self._stop_flag.set()
            return []
        return self._artificial_alerts[self._iter - len(self._artificial_alerts) : len(self._artificial_alerts)]

    def _publish_end_of_alerts(self, alerts: dict):
        self._results["_publish_end_of_alerts"].append(alerts)

    def _publish_new_alerts(self, alerts: dict):
        self._results["_publish_new_alerts"].append(alerts)

    def get_result(self):
        return self._results

    def wait_till_alert_manager_up(self):
        pass


class PrometheusAlertManagerTest(unittest.TestCase):
    def test_alert_manager_listener_artificial_run(self):
        with open(
            os.path.join(
                os.path.dirname(__file__), "test_data/test_prometheus/test_alert_manager_listener_artificial_run.yaml"
            ),
            encoding="utf-8",
        ) as file:
            test_data = json.load(file)
        listener = PrometheusAlertManagerListenerArtificialTest(artificial_alerts=test_data["post"])
        listener.start()
        result = listener.get_result()
        self.assertEqual(result, test_data["expected"])
