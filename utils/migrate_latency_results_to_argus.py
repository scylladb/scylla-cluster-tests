#!/usr/bin/env python
import sys
import os.path
import datetime
import logging.config
from collections import OrderedDict

import argus
from argus.client.sct.client import ArgusSCTClient
from elasticsearch import Elasticsearch

from sdcm.argus_results import send_result_to_argus
from sdcm.keystore import KeyStore

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


LOGGER = logging.getLogger(__name__)


def migrate(creds, job_name, days=7, index_name="performancestatsv2", dry_run=True):  # pylint: disable=too-many-locals
    ks = KeyStore()
    es_conf = ks.get_elasticsearch_token()
    elastic_search = Elasticsearch(**es_conf)

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "test_details.start_time": {
                                "gte": (datetime.datetime.utcnow() - datetime.timedelta(days=days)).timestamp(),
                                "lte": datetime.datetime.utcnow().timestamp(),
                                "boost": 2.0
                            }
                        }
                    },
                    {
                        "term": {
                            "test_details.job_name.keyword": job_name
                        }
                    }
                ]
            }
        },
        "size": 10000
    }

    res = elastic_search.search(index=index_name, body=query)
    for hit in res['hits']['hits']:
        document_id = hit['_id']
        print(f"Document ID: {document_id}")
        workload = hit['_source']['test_details']['sub_type']
        client = ArgusSCTClient(document_id, auth_token=creds["token"], base_url=creds["baseUrl"])
        try:
            latency_during_ops = hit['_source']['latency_during_ops']
        except KeyError:
            print(f"Document {document_id} does not have latency_during_ops key")
            continue
        del latency_during_ops["summary"]  # we don't store summary in Argus

        def sort_func(item):
            try:
                hdr_summary = item[1]['cycles'][0]['hdr_summary']
            except KeyError:  # Steady state is first
                return 0
            try:
                return hdr_summary.get('WRITE', hdr_summary.get('READ'))['start_time']
            except KeyError as exc:
                print(f"error while geting start time in {hdr_summary}", exc)
                return 1

        ordered_latency_during_ops = OrderedDict(sorted(latency_during_ops.items(), key=sort_func))
        for operation in ordered_latency_during_ops:
            if operation == "Steady State":
                result = latency_during_ops[operation]
                description = "Latencies without any operation running"
                if dry_run:
                    print(f"Would send {operation} - {workload} - latencies - cycle 0 to Argus")
                    continue
                try:
                    start_time = result["hdr_summary"].get("READ", result["hdr_summary"].get("WRITE"))[
                        "start_time"] / 1000
                    send_result_to_argus(argus_client=client, workload=workload, name=operation,
                                         description=description, cycle=0, result=result, start_time=start_time)
                except argus.client.base.ArgusClientError:
                    print(
                        f"Failed to send {operation} - {workload} - latencies to Argus: {hit['_source']['test_details']['job_url']}")
                continue
            for idx, cycle in enumerate(latency_during_ops[operation]["cycles"], start=1):
                start_time = cycle["hdr_summary"].get("READ", cycle["hdr_summary"].get("WRITE"))["start_time"]
                start_time = start_time if start_time < 1000000000000 else start_time / 1000
                if dry_run:
                    print(f"Would send {operation} - {workload} - latencies - cycle {idx} to Argus")
                    continue
                try:
                    send_result_to_argus(
                        argus_client=client,
                        workload=workload,
                        name=operation,
                        description=latency_during_ops[operation]["legend"] or "",
                        cycle=idx,
                        result=cycle,
                        start_time=start_time
                    )
                except RuntimeError:
                    # happens when no EventsDevice is running and trying to raise SCT event
                    pass
                except argus.client.base.ArgusClientError:
                    print(
                        f"Failed to send {operation} - {workload} - latencies - cycle {idx} to Argus: {hit['_source']['test_details']['job_url']}")


if __name__ == '__main__':
    """Migrates latency decorator results from ES to Argus"""
    migrate_to_prod = False  # set to True to migrate to production Argus
    dry_run = True  # set to True to simulate the migration without actually sending the data to Argus
    days_back = 365  # set to the number of days back you want to migrate
    # can take job name by clicking a specific run in Argus and copying the job name (without the run number)
    job_name = "scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis"
    if not migrate_to_prod:
        creds = {
            "token": "<token for local environment>",
            "baseUrl": "http://localhost:5000"
        }
    else:
        key_store = KeyStore()
        creds = key_store.get_argus_rest_credentials()

    for index in ["latency-during-ops-write", "latency-during-ops-read", "latency-during-ops-mixed"]:
        migrate(creds=creds, days=days_back,
                index_name=index,
                job_name=job_name,
                dry_run=dry_run)
