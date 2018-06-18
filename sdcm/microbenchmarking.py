#!/usr/bin/env python2
import os
import sys
import logging
import datetime
import json
import argparse
import socket

from collections import defaultdict
from es import ES
from results_analyze import BaseResultsAnalyzer

logger = logging.getLogger("microbenchmarking")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ES_INDEX = 'test13_microbenchmarking'
HOSTNAME = socket.gethostname()


class MicroBenchmarkingResultsAnalyzer(BaseResultsAnalyzer):
    def __init__(self):
        super(MicroBenchmarkingResultsAnalyzer, self).__init__(
            es_index=ES_INDEX,
            send_mail=True,
            email_template_fp="results_microbenchmark.html",
            query_limit=10000
        )

    def check_regression(self, current_results):
        filter_path = (
            "hits.hits._id",  # '2018-04-02_18:36:47'
            "hits.hits._type",  # large-partition-skips_64-32.1
            "hits.hits._source.hostname",  # 'godzilla.cloudius-systems.com'
            "hits.hits._source.test_group_properties.name",  # large-partition-skips
            "hits.hits._source.results.parameters",  # 'read,skip,test_run_count': u'64,32,1',
            "hits.hits._source.results.stats.aio",
            "hits.hits._source.results.stats.cpu",
            "hits.hits._source.results.stats.time (s)",
            "hits.hits._source.results.stats.frag/s",
            "hits.hits._source.versions",
        )

        tests_filtered = self._es.search(index=self._index, filter_path=filter_path, size=self._limit)
        assert tests_filtered, "No results from DB"
        results = tests_filtered["hits"]["hits"]
        sorted_by_type = defaultdict(list)
        for res in results:
            if res["_source"]["hostname"] != HOSTNAME:
                continue
            sorted_by_type[res["_type"]].append(res)

        report_results = defaultdict(dict)
        # report_results = {
        #     "large-partition-skips_1-0.1": {
        #           "aio":{
        #               "min"
        #               "max":
        #               "cur":
        #               "last":
        #               "diff":
        #           }
        # }
        allowed_stats = ("Current", "Last", "Diff last [%]", "Best", "Diff best [%]")

        def set_results_by_param(param):
            list_of_results_from_db.sort(key=lambda x: datetime.datetime.strptime(x["_id"], "%Y-%m-%d_%H:%M:%S"))

            def get_param_val(x):
                return float(x["_source"]["results"]["stats"][param])

            def get_commit_id(x):
                return x["_source"]['versions']['scylla-server']['commit_id']

            last_val = get_param_val(list_of_results_from_db[-1])
            last_commit = get_commit_id(list_of_results_from_db[-1])
            cur_val = float(current_result["results"]["stats"][param])
            min_result = min(list_of_results_from_db, key=get_param_val)
            min_result_val = get_param_val(min_result)
            min_result_commit = get_commit_id(min_result)
            diff_last = ((cur_val - last_val)/max(cur_val, last_val))*100 if max(cur_val, last_val) else 0
            diff_best = ((cur_val - min_result_val)/max(cur_val, min_result_val))*100 if max(cur_val, min_result_val) else 0
            stats = {
                "Current": cur_val,
                "Last": (last_val, last_commit),
                "Best": (min_result_val, min_result_commit),
                "Diff last [%]": diff_last,  # diff in percents
                "Diff best [%]": diff_best,
            }
            report_results[test_type][param] = stats

        for test_type, current_result in current_results.iteritems():
            list_of_results_from_db = sorted_by_type[test_type]
            if not list_of_results_from_db:
                logger.warning("No results for '%s' in DB. Skipping", test_type)
                continue
            set_results_by_param("aio")
            set_results_by_param("frag/s")
            set_results_by_param("cpu")
            set_results_by_param("time (s)")

        version_info = current_results[current_results.keys()[0]]['versions']['scylla-server']
        subject = "Microbenchmarks - Performance Regression"
        for_render = {
            "subject": subject,
            "results": report_results,
            "stats_names": allowed_stats,
            "kibana_url": self._conf.get('kibana_url'),
        }
        for_render.update(dict(test_version=version_info))
        html = self.render_to_html(for_render, save_to_file="/tmp/perf.html")
        self.send_email(subject, html)


def get_results(results_path, update_db):
    test_id = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    bad_chars = " "
    os.chdir(os.path.join(results_path, "perf_fast_forward_output"))
    db = ES()
    results = {}
    for dirname in os.listdir(os.getcwd()):
        logger.info(dirname)
        for filename in os.listdir(dirname):
            new_filename = "".join(c for c in filename if c not in bad_chars)
            test_type = dirname + "_" + os.path.splitext(new_filename)[0]
            logger.info(filename)
            logger.info(test_type)
            with open(os.path.join(dirname, filename), 'r') as f:
                datastore = json.load(f)
            datastore.update({'hostname': HOSTNAME})
            if update_db:
                db.create(index=ES_INDEX, doc_type=test_type, doc_id=test_id, body=datastore)
            results[test_type] = datastore
    return results


def main(update_db, results_path="."):
    results = get_results(results_path=results_path, update_db=update_db)
    mbra = MicroBenchmarkingResultsAnalyzer()
    mbra.check_regression(results)


def parse_args():
    parser = argparse.ArgumentParser(description="Microbencmarking stats uploader and analyzer")
    parser.add_argument("--update-db", action="store_true", default=False,
                        help="Upload current microbenchmarking stats to ElasticSearch")
    parser.add_argument("--results-path", action="store", default=".",
                        help="Path where to search for test results")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(update_db=args.update_db, results_path=args.results_path)
