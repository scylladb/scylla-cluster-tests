#!/usr/bin/env python

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
# Copyright (c) 2018 ScyllaDB


import time
import os

from sdcm.tester import ClusterTester
from sdcm.utils.common import format_timestamp


class GeminiTest(ClusterTester):

    """
    Test Scylla with gemini - tool for testing data integrity
    https://github.com/scylladb/gemini
    """
    gemini_results = {
        "cmd": "N/A",
        "status": "Not Running",
        "results": [],
        'errors': {}
    }

    def test_random_load(self):
        """
        Run gemini tool
        """
        cmd = self.params.get('gemini_cmd')

        self.log.debug('Start gemini benchmark')
        test_queue = self.run_gemini(cmd=cmd)

        self.gemini_results = self.verify_gemini_results(queue=test_queue)

        if self.gemini_results['status'] == 'FAILED':
            self.fail(self.gemini_results['results'])

    def test_load_random_with_nemesis(self):

        cmd = self.params.get('gemini_cmd')

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)

        self.log.debug('Start gemini benchmark')
        test_queue = self.run_gemini(cmd=cmd)

        # sleep before run nemesis test_duration * .25
        sleep_before_start = float(self.params.get('test_duration', 5)) * 60 * .25
        self.log.info('Sleep interval {}'.format(sleep_before_start))
        time.sleep(sleep_before_start)

        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        self.gemini_results = self.verify_gemini_results(queue=test_queue)

        self.db_cluster.stop_nemesis(timeout=1600)

        if self.gemini_results['status'] == 'FAILED':
            self.fail(self.gemini_results['results'])

    def get_email_data(self):
        scylla_version = self.db_cluster.nodes[0].scylla_version
        oracle_db_version = self.cs_db_cluster.nodes[0].scylla_version if self.cs_db_cluster else ""
        start_time = format_timestamp(self.start_time)
        critical = self.get_critical_events()

        return {
            "subject": 'Gemini - test results: {}'.format(start_time),
            "gemini_cmd": self.gemini_results['cmd'],
            "gemini_version": self.loaders.gemini_version,
            "scylla_version": scylla_version,
            "scylla_ami_id": self.params.get('ami_id_db_scylla'),
            "scylla_instance_type": self.params.get('instance_type_db'),
            "number_of_db_nodes": self.params.get('n_db_nodes'),
            "number_of_oracle_nodes": self.params.get('n_test_oracle_db_nodes', 1),
            "oracle_db_version": oracle_db_version,
            "oracle_ami_id": self.params.get('ami_id_db_oracle'),
            "oracle_instance_type": self.params.get('instance_type_db_oracle'),
            "results": self.gemini_results['results'],
            "status": self.gemini_results['status'],
            'test_name': self.id(),
            'test_id': self.test_id,
            'start_time': start_time,
            'end_time': format_timestamp(time.time()),
            'build_url': os.environ.get('BUILD_URL', None),
            'nemesis_name': self.params.get('nemesis_class_name'),
            'nemesis_details': self.get_nemesises_stats(),
            'test_status': ("FAILED", critical) if critical else ("No critical errors in critical.log", None),
        }
