#!/usr/bin/env python
import sys
import os.path
import datetime
import logging
import logging.config

import click
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from sdcm.keystore import KeyStore

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


LOGGER = logging.getLogger(__name__)


@click.command(help="Migrate nemesis data from old indexes into the new one")
@click.option('--dry-run/--no-dry-run', default=False)
@click.option('--new-index', default='nemesis_data', help='name of the target index')
@click.option('--days', default=10, type=int, help="how many days backwards to migrate")
@click.argument('old_index_name', type=str, default='longevitytest')
def migrate(old_index_name, dry_run, new_index, days):  # pylint: disable=too-many-locals

    logging.basicConfig(level=logging.DEBUG)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': 'INFO',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # Default is stderr
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': 'WARNING',
                'propagate': False
            },
            '__main__': {  # if __name__ == '__main__'
                'handlers': ['default'],
                'level': 'DEBUG',
                'propagate': False
            },
        }
    }
    )
    ks = KeyStore()
    es_conf = ks.get_elasticsearch_token()
    elastic_search = Elasticsearch(**es_conf)

    if not elastic_search.indices.exists(index=new_index):
        elastic_search.indices.create(index=new_index)

    def post_to_new(doc):
        if dry_run:
            return
        elastic_search.index(index=new_index, body=doc)

    res = scan(elastic_search, index=old_index_name, query={"query": {"range": {
        "test_details.start_time": {
            "gte": (datetime.datetime.utcnow() - datetime.timedelta(days=days)).timestamp(),
            "lte": datetime.datetime.utcnow().timestamp(),
            "boost": 2.0
        }
    }}}, size=300, scroll='3h')

    for num, hit in enumerate(res):
        nemesis_list = hit["_source"]["nemesis"]
        test_data = hit["_source"]
        LOGGER.info("%s: %s", num, test_data['test_details']['test_id'])
        if 'scylla-server' not in test_data['versions']:
            LOGGER.debug("%s: No version for %s - %s",
                         num, test_data['test_details']['test_id'], test_data['test_details']['job_name'])
            if not test_data['test_details']['job_name']:
                LOGGER.debug(test_data)
            continue

        for nemesis_class, data in nemesis_list.items():
            for failure in data['failures']:
                new_nemesis_data = dict(
                    test_id=test_data['test_details']['test_id'],
                    job_name=test_data['test_details']['job_name'],
                    test_name=test_data['test_details']['test_name'],
                    scylla_version=test_data['versions']['scylla-server']['version'],
                    scylla_git_sha=test_data['versions']['scylla-server']['commit_id'],
                )

                new_nemesis_data.update(dict(
                    nemesis_name=nemesis_class,
                    nemesis_duration=failure['duration'],
                    start_time=datetime.datetime.utcfromtimestamp(failure['start']),
                    end_time=datetime.datetime.utcfromtimestamp(failure['end']),
                    target_node=failure['node'],
                    outcome="failure",
                    failure_message=failure['error']
                ))
                post_to_new(new_nemesis_data)

            for run in data['runs']:
                new_nemesis_data = dict(
                    test_id=test_data['test_details']['test_id'],
                    job_name=test_data['test_details']['job_name'],
                    test_name=test_data['test_details']['test_name'],
                    scylla_version=test_data['versions']['scylla-server']['version'],
                    scylla_git_sha=test_data['versions']['scylla-server']['commit_id'],
                )
                new_nemesis_data.update(dict(
                    nemesis_name=nemesis_class,
                    nemesis_duration=run['duration'],
                    start_time=datetime.datetime.utcfromtimestamp(run['start']),
                    end_time=datetime.datetime.utcfromtimestamp(run['end']),
                    target_node=run['node'],
                    outcome="passed"
                ))
                if run.get('type', '') == 'skipped':
                    new_nemesis_data['outcome'] = 'skipped'
                    new_nemesis_data['skip_reason'] = run['skip_reason']
                post_to_new(new_nemesis_data)


if __name__ == '__main__':
    migrate()  # pylint: disable=no-value-for-parameter
