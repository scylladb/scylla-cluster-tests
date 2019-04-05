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


from avocado import main
from sdcm.tester import ClusterTester


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
        cmd = self.params.get('gemini_cmd')

        self.log.debug('Start gemini benchmark')
        test_queue = self.run_gemini(cmd=cmd)
        results = self.get_gemini_results(queue=test_queue)
        self.log.debug(results)
        assert results, "Gemini results are not found."
        failed = False
        for res in results:
            for err_type in ['write_errors', 'read_errors', 'errors']:
                if res[err_type]:
                    self.log.error("Gemini {} errors: {}".format(err_type, res[err_type]))
                    failed = True

        if failed:
            self.fail('Test failed due to found errors')
        else:
            self.log.info('Test passed')
