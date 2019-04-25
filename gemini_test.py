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
from sdcm.tester import ClusterTester
from sdcm.results_analyze import BaseResultsAnalyzer


class GeminiTest(ClusterTester):

    """
    Test Scylla with gemini - tool for testing data integrity
    https://github.com/scylladb/gemini

    :avocado: enable
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

    def _prepare_test_results(self):
        return {
            "test_name": self.avocado_params.id.name.split('.')[0],
            "start_time": "",
            "end_time": "",
            "gemini_cmd": "",
            "scylla_version": self.db_cluster.nodes[0].scylla_version,
            "scylla_ami_id": self.params.get('ami_id_db_scylla'),
            "scylla_instance_type": self.params.get('instance_type_db'),
            "oracle_db_version": self.cs_db_cluster.nodes[0].scylla_version,
            "oracle_ami_id": self.params.get('ami_id_db_oracle'),
            "oracle_instance_type": self.params.get('instance_type_db_oracle'),
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
