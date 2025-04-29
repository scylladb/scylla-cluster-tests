#!/usr/bin/env python
import sys
import os.path
import logging
import logging.config

import click
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from sdcm.keystore import KeyStore

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


LOGGER = logging.getLogger(__name__)


def configure_logging():
    logging.basicConfig(level=logging.DEBUG)
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                },
            },
            "handlers": {
                "default": {
                    "level": "INFO",
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",  # Default is stderr
                },
            },
            "loggers": {
                "": {  # root logger
                    "handlers": ["default"],
                    "level": "WARNING",
                    "propagate": False,
                },
                "__main__": {  # if __name__ == '__main__'
                    "handlers": ["default"],
                    "level": "DEBUG",
                    "propagate": False,
                },
            },
        }
    )


@click.command(help="edit specific values in ES")
@click.argument("index_name", default="performanceregressiontest", type=str)
@click.argument("test_id", type=str)
@click.argument("new_job_name", type=str)
def update_value(index_name, test_id, new_job_name):
    """
    helper script to move results in index to a different job
    as needed

    for example, we had a bad run that we want to exclude and move to different job-name:

        ./utils/update_field.py performanceregressiontest 456cb6da-e315-4f38-bde5-28eba9e386b9 \
            scylla-master/scylla-master-perf-regression-latency-650gb-grow-shrink

    """
    configure_logging()

    ks = KeyStore()
    es_conf = ks.get_elasticsearch_token()
    elastic_search = Elasticsearch(**es_conf)

    res = scan(
        elastic_search,
        index=index_name,
        query={
            "query": {"query_string": {"query": f'test_details.test_id: "{test_id}"'}}
        },
        size=300,
        scroll="3h",
    )

    for num, hit in enumerate(res):
        test_data = hit["_source"]
        LOGGER.info(
            "%s: %s - %s",
            num,
            test_data["test_details"]["test_id"],
            test_data["test_details"]["job_name"],
        )
        test_data["test_details"]["job_name"] = new_job_name
        elastic_search.update(
            index=index_name, id=hit["_id"], doc=test_data
        )
        click.secho(f"updated {test_data['test_details']['test_id']}", fg="green")


if __name__ == "__main__":
    update_value()
