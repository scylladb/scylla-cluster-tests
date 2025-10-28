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
# Copyright (c) 2023 ScyllaDB
import json
import os

from sdcm.argus_results import send_perf_simple_query_result_to_argus
from sdcm.send_email import read_email_data_from_file
from sdcm.tester import ClusterTester
from sdcm.utils.microbenchmarking.perf_simple_query_reporter import PerfSimpleQueryAnalyzer


class PerfSimpleQueryTest(ClusterTester):

    def test_perf_simple_query(self):
        perf_simple_query_extra_command = self.params.get('perf_simple_query_extra_command') or ""
        result = self.db_cluster.nodes[0].remoter.run(
            f"scylla perf-simple-query --json-result=perf-simple-query-result.txt --smp 1 -m 1G {perf_simple_query_extra_command}")
        if result.ok:
            output = self.db_cluster.nodes[0].remoter.run("cat perf-simple-query-result.txt").stdout
            results = json.loads(output)
            self.create_test_stats(
                specific_tested_stats={"perf_simple_query_result": results},
                doc_id_with_timestamp=True)
            if self.create_stats:
                is_gce = self.params.get('cluster_backend') == 'gce'
                regression_result = PerfSimpleQueryAnalyzer(self._test_index).check_regression(
                    self._test_id, is_gce=is_gce,
                    extra_jobs_to_compare=self.params.get('perf_extra_jobs_to_compare'))
                
                # If check_regression failed, create a basic email_data.json
                if not regression_result:
                    self._create_fallback_email_data()

            error_thresholds = self.params.get("latency_decorator_error_thresholds")
            send_perf_simple_query_result_to_argus(self.test_config.argus_client(), results, error_thresholds)

    def _create_fallback_email_data(self):
        """Create basic email data when check_regression() fails"""
        self.log.warning("check_regression() failed, creating fallback email data")
        
        email_data_file = os.path.join(self.logdir, "email_data.json")
        fallback_data = {
            "reporter": "PerfSimpleQuery",
            "subject": f"perf_simple_query Test - Regression check failed for test {self._test_id}",
            "testrun_id": self._test_id,
            "test_stats": "N/A",
            "scylla_date_results_table": "N/A",
            "job_url": os.environ.get('BUILD_URL', 'N/A'),
            "test_version": "N/A",
            "collect_last_scylla_date_count": "N/A",
            "deviation_diff": "N/A",
            "is_deviation_within_limits": "N/A"
        }
        
        with open(email_data_file, 'w', encoding="utf-8") as f:
            json.dump(fallback_data, f)
        
        self.log.info("Fallback email data saved to: %s", email_data_file)
    
    def get_email_data(self):
        """Read email data from the file created by check_regression()"""
        self.log.info("Prepare data for email")
        
        # Try to read the email_data.json file created by check_regression()
        email_data_file = os.path.join(self.logdir, "email_data.json")
        email_data = read_email_data_from_file(email_data_file)
        
        if email_data:
            self.log.info("Email data loaded from file: %s", email_data_file)
            return email_data
        else:
            # If no email data was created (e.g., check_regression failed),
            # return empty dict so the base class can add grafana_screenshots
            self.log.warning("No email data found in %s, email will contain minimal information", email_data_file)
            return {}

    def update_test_with_errors(self):
        self.log.info("update_test_with_errors: Suppress writing errors to ES")
