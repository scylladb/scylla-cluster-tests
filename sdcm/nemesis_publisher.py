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
from functools import cached_property
import sys

from elasticsearch import Elasticsearch

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class NemesisElasticSearchPublisher:
    index_name = 'nemesis_data'
    es: Elasticsearch
    error_message_size_limit_mb = 100

    def __init__(self, tester):
        self.tester = tester

    def create_es_connection(self):
        ks = KeyStore()
        es_conf = ks.get_elasticsearch_token()
        self.es = Elasticsearch(**es_conf)

    @cached_property
    def stats(self):
        _stats = {}
        _stats['versions'] = self.tester.get_scylla_versions()
        _stats['test_details'] = self.tester.get_test_details()
        _stats['test_details']['test_name'] = self.tester.id()
        return _stats

    def publish(self, disrupt_name, status=True, data=None):
        test_data = self.stats
        assert test_data, "without self.stats data we can't publish nemesis ES"
        if 'scylla-server' in test_data['versions']:
            version = test_data['versions']['scylla-server'].get('version', '')
            commit_id = test_data['versions']['scylla-server'].get('commit_id', '')
            commit_date = test_data['versions']['scylla-server'].get('date', '')
            build_id = test_data['versions']['scylla-server'].get('build_id', '')
        elif 'version' in test_data['versions']:
            version = test_data['versions'].get('version', '')
            commit_id = test_data['versions'].get('commit_id', '')
            commit_date = test_data['versions'].get('date', '')
            build_id = test_data['versions'].get('build_id', '')
        else:
            version = ''
            commit_id = ''
            commit_date = ''
            build_id = ''
        scylla_version = '.'.join([version, commit_date, commit_id])
        new_nemesis_data = dict(
            test_id=test_data['test_details']['test_id'],
            job_name=test_data['test_details']['job_name'],
            test_name=test_data['test_details']['test_name'],
            scylla_version=version,
            scylla_git_sha=commit_id,
            scylla_commit_date=commit_date,
            full_scylla_version=scylla_version,
            build_id=build_id,
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
            error_message_size_mb = sys.getsizeof(data["error"]) / 1024**2
            diff = error_message_size_mb / self.error_message_size_limit_mb
            if diff > 1.0:
                # NOTE: useful for cases when loader nodes fail to connect to
                # terminated K8S host machines. It may provide very huge output up to 1Gb.
                LOGGER.warning("Got too big error message running '%s' nemesis: %sMb.\n"
                               "The limit is '%sMb'. Trimming the error message...",
                               disrupt_name, error_message_size_mb, self.error_message_size_limit_mb)
                # NOTE: we are satisfied making rough rounding here
                data["error"] = data["error"][:int(len(data["error"]) / diff)]
            new_nemesis_data.update(dict(
                nemesis_name=disrupt_name,
                nemesis_duration=data['duration'],
                start_time=datetime.utcfromtimestamp(data['start']),
                end_time=datetime.utcfromtimestamp(data['end']),
                target_node=data['node'],
                outcome="failure",
                failure_message=data['error']
            ))

        res = self.es.index(index=self.index_name, body=new_nemesis_data)
        LOGGER.debug(res)
