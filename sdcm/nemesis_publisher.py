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

import logging
from datetime import datetime

from elasticsearch import Elasticsearch

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class NemesisElasticSearchPublisher:
    index_name = 'nemesis_data'
    es: Elasticsearch

    def __init__(self, tester):
        self.tester = tester

    def create_es_connection(self):
        ks = KeyStore()
        es_conf = ks.get_elasticsearch_credentials()
        self.es = Elasticsearch(hosts=[es_conf["es_url"]], verify_certs=False,
                                http_auth=(es_conf["es_user"], es_conf["es_password"]))

    def publish(self, disrupt_name, status=True, data=None):
        test_data = self.tester.get_stats()
        assert test_data, "without _stats data we can't publish nemesis ES"

        new_nemesis_data = dict(
            test_id=test_data['test_details']['test_id'],
            job_name=test_data['test_details']['job_name'],
            test_name=test_data['test_details']['test_name'],
            scylla_version=test_data['versions']['scylla-server']['version'],
            scylla_git_sha=test_data['versions']['scylla-server']['commit_id'],
        )
        if status:
            new_nemesis_data.update(
                nemesis_name=disrupt_name,
                nemesis_duration=data['duration'],
                start_time=datetime.utcfromtimestamp(data['start']),
                end_time=datetime.utcfromtimestamp(data['end']),
                target_node=data['node'],
                outcome="passed"
            )
            if 'skip_reason' in data:
                new_nemesis_data['outcome'] = 'skipped'
                new_nemesis_data['skip_reason'] = data['skip_reason']

        else:
            new_nemesis_data.update(dict(
                nemesis_name=disrupt_name,
                nemesis_duration=data['duration'],
                start_time=datetime.utcfromtimestamp(data['start']),
                end_time=datetime.utcfromtimestamp(data['end']),
                target_node=data['node'],
                outcome="failure",
                failure_message=data['error']
            ))

        res = self.es.index(index=self.index_name, doc_type='nemesis', body=new_nemesis_data)
        LOGGER.debug(res)
