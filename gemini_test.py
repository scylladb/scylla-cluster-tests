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

import datetime
import time
import os
from sdcm.tester import ClusterTester
from sdcm.results_analyze import BaseResultsAnalyzer


class GeminiTest(ClusterTester):

    """
    Test Scylla with gemini - tool for testing data integrity
    https://github.com/scylladb/gemini
    """

    def test_random_load(self):
        """
        Run gemini tool
        """
        prepared_results = self._prepare_test_results()

        cmd = self.params.get('gemini_cmd')
        prepared_results['gemini_cmd'] = cmd

        self.log.debug('Start gemini benchmark')
        prepared_results['start_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        test_queue = self.run_gemini(cmd=cmd)
        result = self.get_gemini_results(queue=test_queue)

        prepared_results.update(result)

        prepared_results['end_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._send_email(prepared_results)

        if result['status'] == 'FAILED':
            self.fail(result['results'])

    def test_random_load_with_nemesis(self):
        prepared_results = self._prepare_test_results()

        cmd = self.params.get('gemini_cmd')
        prepared_results['gemini_cmd'] = cmd

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
        prepared_results['nemesis_name'] = self.params.get('nemesis_class_name')

        self.log.debug('Start gemini benchmark')
        prepared_results['start_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        test_queue = self.run_gemini(cmd=cmd)

        # sleep before run nemesis test_duration * .15
        sleep_before_start = float(self.params.get('test_duration', 5)) * 60 * .15
        self.log.info('Sleep interval {}'.format(sleep_before_start))
        time.sleep(sleep_before_start)

        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        result = self.get_gemini_results(queue=test_queue)

        self.db_cluster.stop_nemesis(timeout=1600)
        nemesises = self.get_doc_data(key='nemesis')

        prepared_results.update(result)
        prepared_results['nemesis_details'] = nemesises
        prepared_results['end_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self._send_email(prepared_results)

        if result['status'] == 'FAILED':
            self.fail(result['results'])

    def _prepare_test_results(self):
        return {
            "test_name": self.id(),
            "build_url": os.getenv('BUILD_URL', "#"),
            "start_time": "",
            "end_time": "",
            "gemini_cmd": "",
            "gemini_version": self.loaders.gemini_version,
            "scylla_version": self.db_cluster.nodes[0].scylla_version,
            "scylla_ami_id": self.params.get('ami_id_db_scylla'),
            "scylla_instance_type": self.params.get('instance_type_db'),
            "number_of_db_nodes": self.params.get('n_db_nodes'),
            "number_of_oracle_nodes": self.params.get('n_test_oracle_db_nodes', 1),
            "oracle_db_version": self.cs_db_cluster.nodes[0].scylla_version,
            "oracle_ami_id": self.params.get('ami_id_db_oracle'),
            "oracle_instance_type": self.params.get('instance_type_db_oracle'),
            "nemesis_name": '-',
            "nemesis_details": {},
            "results": [],
            'status': None
        }

    def _send_email(self, results):
        email_recipients = self.params.get('email_recipients', default=None)
        self.log.info('Send email with results to {}'.format(email_recipients))
        em = BaseResultsAnalyzer(es_index=self._test_index, es_doc_type=self._es_doc_type,
                                 email_template_fp="results_gemini.html",
                                 send_email=self.params.get('send_email', default=True),
                                 email_recipients=email_recipients)

        html = em.render_to_html(results)

        em.send_email(subject='Gemini - test results: {}'.format(results['start_time']), content=html)
