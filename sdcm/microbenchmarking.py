#!/usr/bin/env python3

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

import os
import sys
import logging
import datetime
import json
import argparse
import socket
import tempfile
from collections import defaultdict
import contextlib
import warnings

# disable InsecureRequestWarning
import urllib3

# HACK: since cryptography==37.0.0 CryptographyDeprecationWarning is being raised
# this is until https://github.com/paramiko/paramiko/issues/2038 would be solved
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings(action='ignore', category=CryptographyDeprecationWarning)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sdcm.results_analyze import BaseResultsAnalyzer  # pylint: disable=wrong-import-position
from sdcm.utils.log import setup_stdout_logger  # pylint: disable=wrong-import-position

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
setup_stdout_logger()

LOGGER = logging.getLogger("microbenchmarking")
LOGGER.setLevel(logging.DEBUG)

MICROBENCHMARK_INDEX_NAME = 'microbenchmarkingv3'


@contextlib.contextmanager
def chdir(dirname=None):
    curdir = os.getcwd()
    try:
        if dirname is not None:
            os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)


class LargeNumberOfDatasetsException(Exception):
    def __init__(self, msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = msg

    def __str__(self):
        return "MBM: {0.message}".format(self)


class EmptyResultFolder(Exception):
    def __init__(self, msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = msg

    def __str__(self):
        return "MBM: {0.message}".format(self)


class MicroBenchmarkingResultsAnalyzer(BaseResultsAnalyzer):  # pylint: disable=too-many-instance-attributes
    allowed_stats = ('Current', 'Stats', 'Last, commit, date', 'Diff last [%]', 'Best, commit, date', 'Diff best [%]')
    higher_better = ('frag/s',)
    lower_better = ('avg aio',)
    submetrics = {'frag/s': ['mad f/s', 'max f/s', 'min f/s']}

    def __init__(self, email_recipients, db_version=None, es_index=None):
        if not es_index:
            es_index = MICROBENCHMARK_INDEX_NAME
        super().__init__(es_index=es_index, es_doc_type="microbenchmark", email_recipients=email_recipients,
                         email_template_fp="results_microbenchmark.html", query_limit=10000, logger=LOGGER)
        self.hostname = socket.gethostname()
        self._run_date_pattern = "%Y-%m-%d_%H:%M:%S"
        self.test_run_date = datetime.datetime.now().strftime(self._run_date_pattern)
        self.db_version = db_version
        self.build_url = os.getenv('BUILD_URL', "")
        self.cur_version_info = None
        self.metrics = self.higher_better + self.lower_better

    def _get_prior_tests(self, filter_path, additional_filter=''):
        query = f"hostname:'{self.hostname}' AND versions.scylla-server.version:{self.db_version[:3]}*"
        if additional_filter:
            query += " AND " + additional_filter
        print(query)
        output = self._es.search(  # pylint: disable=unexpected-keyword-arg; pylint doesn't understand Elasticsearch code
            index=self._es_index,
            q=query,
            size=self._limit,
            filter_path=filter_path,
        )
        return output

    def check_regression(self, current_results):  # pylint: disable=arguments-differ
        # pylint: disable=too-many-locals, too-many-statements

        if not current_results:
            return {}

        start_date = datetime.datetime.strptime("2019-01-01", "%Y-%m-%d")
        filter_path = (
            "hits.hits._id",  # '2018-04-02_18:36:47_large-partition-skips_[64-32.1)'
            "hits.hits._source.test_args",  # [64-32.1)
            "hits.hits.test_group_properties.name",  # large-partition-skips
            "hits.hits._source.hostname",  # 'monster'
            "hits.hits._source.test_run_date",
            "hits.hits._source.test_group_properties.name",  # large-partition-skips
            "hits.hits._source.results.stats.aio",
            "hits.hits._source.results.stats.avg aio",
            "hits.hits._source.results.stats.cpu",
            "hits.hits._source.results.stats.time (s)",
            "hits.hits._source.results.stats.frag/s",
            "hits.hits._source.versions",
            "hits.hits._source.excluded"
        )

        self.db_version = self.cur_version_info["version"]
        tests_filtered = self._get_prior_tests(
            filter_path,
            additional_filter='((-_exists_:excluded) OR (excluded:false))'
        )
        assert tests_filtered, "No results from DB"

        results = []
        for doc in tests_filtered['hits']['hits']:
            doc_date = datetime.datetime.strptime(
                doc['_source']['versions']['scylla-server']['run_date_time'], "%Y-%m-%d %H:%M:%S")
            if doc_date > start_date:
                results.append(doc)

        sorted_by_type = defaultdict(list)
        for res in results:
            test_type = "%s_%s" % (res["_source"]["test_group_properties"]["name"],
                                   res["_source"]["test_args"])
            sorted_by_type[test_type].append(res)

        report_results = defaultdict(dict)
        # report_results = {
        #     "large-partition-skips_1-0.1": {
        #           "aio":{
        #               "Current":
        #               "Last":
        #               "Diff last [%]":
        #               "Best":
        #               "Diff best [%]":
        #           },
        #           "frag/s":{
        #               "Current":
        #               "Stats": { submetrica: }
        #               "Last":
        #               "Diff last [%]":
        #               "Best":
        #               "Diff best [%]":
        #           },
        # }

        def set_results_for(current_result, metrica):
            list_of_results_from_db.sort(key=lambda x: datetime.datetime.strptime(x["_source"]["test_run_date"],
                                                                                  self._run_date_pattern))

            def get_metrica_val(val):
                metrica_val = val["_source"]["results"]["stats"].get(metrica, None)
                return float(metrica_val) if metrica_val else None

            def get_commit_id(val):
                return val["_source"]['versions']['scylla-server']['commit_id']

            def get_commit_date(val):
                return datetime.datetime.strptime(val["_source"]['versions']['scylla-server']['date'],
                                                  "%Y%m%d").date()

            def get_best_result_for_metrica():
                # build new list with results where analyzing metrica is not None
                # metrica s with result 0, will be included

                list_for_searching = [el for el in list_of_results_from_db if get_metrica_val(el) is not None]

                # if list is empty ( which could be happened for new metric),
                # then return first element in list, because result will be None
                if not list_for_searching:
                    return list_of_results_from_db[0]
                if metrica in self.higher_better:
                    return max(list_for_searching, key=get_metrica_val)
                elif metrica in self.lower_better:
                    return min(list_for_searching, key=get_metrica_val)
                else:
                    return list_of_results_from_db[0]

            def count_diff(cur_val, dif_val):
                try:
                    cur_val = float(cur_val) if cur_val else None
                except ValueError:
                    cur_val = None

                if not cur_val:
                    return None

                if dif_val is None:
                    return None

                ret_dif = ((cur_val - dif_val) / dif_val) * 100 if dif_val > 0 else cur_val * 100

                if metrica in self.higher_better:
                    ret_dif = -ret_dif

                ret_dif = -ret_dif if ret_dif != 0 else 0

                return ret_dif

            def get_diffs(cur_val, best_result_val, last_val):
                # if last result doesn't contain the metric
                # assign 0 to last value to count formula of changes

                diff_best = count_diff(cur_val, best_result_val)
                diff_last = count_diff(cur_val, last_val)

                return (diff_last, diff_best)

            if len(list_of_results_from_db) > 1 and get_commit_id(list_of_results_from_db[-1]) == self.cur_version_info["commit_id"]:
                last_idx = -2
            else:  # when current results are on disk but db is not updated
                last_idx = -1

            cur_val = current_result["results"]["stats"].get(metrica, None)
            if cur_val:
                cur_val = float(cur_val)

            last_val = get_metrica_val(list_of_results_from_db[last_idx])
            last_commit = get_commit_id(list_of_results_from_db[last_idx])
            last_commit_date = get_commit_date(list_of_results_from_db[last_idx])

            best_result = get_best_result_for_metrica()
            best_result_val = get_metrica_val(best_result)
            best_result_commit = get_commit_id(best_result)
            best_commit_date = get_commit_date(best_result)

            diff_last, diff_best = get_diffs(cur_val, best_result_val, last_val)

            stats = {
                "Current": cur_val,
                "Last, commit, date": (last_val, last_commit, last_commit_date),
                "Best, commit, date": (best_result_val, best_result_commit, best_commit_date),
                "Diff last [%]": diff_last,  # diff in percents
                "Diff best [%]": diff_best,
                "has_regression": False,
                "has_improvement": False,

            }

            if ((diff_last and diff_last < -5) or (diff_best and diff_best < -5)):
                report_results[test_type]["has_diff"] = True
                stats["has_regression"] = True

            if ((diff_last and diff_last > 50) or (diff_best and diff_best > 50)):
                report_results[test_type]['has_improve'] = True
                stats['has_improvement'] = True

            report_results[test_type]["dataset_name"] = current_result['dataset_name']
            report_results[test_type][metrica] = stats

        def set_results_for_sub(current_result, metrica):
            report_results[test_type][metrica].update({'Stats': {}})
            for submetrica in self.submetrics.get(metrica):
                submetrica_cur_val = float(current_result["results"]["stats"][submetrica])
                report_results[test_type][metrica]['Stats'].update({submetrica: submetrica_cur_val})

        for test_type, current_result in current_results.items():
            list_of_results_from_db = sorted_by_type[test_type]
            if not list_of_results_from_db:
                self.log.warning("No results for '%s' in DB. Skipping", test_type)
                continue
            for metrica in self.metrics:
                self.log.info("Analyzing %s:%s", test_type, metrica)
                set_results_for(current_result, metrica)
                if metrica in list(self.submetrics.keys()):
                    set_results_for_sub(current_result, metrica)

        return report_results

    def send_html_report(self, report_results, html_report_path=None):

        subject = "Microbenchmarks - Performance Regression - %s" % self.test_run_date
        dashboard_path = "app/kibana#/dashboard/aee9b370-09db-11e9-a976-2fe0f5890cd0?_g=(filters%3A!())"

        for_render = {
            "subject": subject,
            "testrun_id": self.test_run_date,
            "results": report_results,
            "stats_names": self.allowed_stats,
            "metrics": self.metrics,
            "kibana_url": self.gen_kibana_dashboard_url(dashboard_path),
            "job_url": self.build_url,
            "full_report": True,
            "hostname": self.hostname,
            "test_version": self.cur_version_info
        }

        if html_report_path:
            html_file_path = html_report_path
        else:
            html_fd, html_file_path = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")
            os.close(html_fd)
        self.render_to_html(for_render, html_file_path=html_file_path)
        for_render["full_report"] = False
        summary_html = self.render_to_html(for_render)
        return self._send_report(subject, summary_html, files=(html_file_path,))

    def _send_report(self, subject, summary_html, files):
        return self.send_email(subject, summary_html, files=files)

    def get_results(self, results_path, update_db):
        # pylint: disable=too-many-locals

        bad_chars = " "
        with chdir(os.path.join(results_path, "perf_fast_forward_output")):
            results = {}
            for (fullpath, subdirs, files) in os.walk(os.getcwd()):
                self.log.info(fullpath)
                if (os.path.dirname(fullpath).endswith('perf_fast_forward_output') and
                        len(subdirs) > 1):
                    raise LargeNumberOfDatasetsException('Test set {} has more than one datasets: {}'.format(
                        os.path.basename(fullpath),
                        subdirs))

                if not subdirs:
                    dataset_name = os.path.basename(fullpath)
                    self.log.info('Dataset name: {}'.format(dataset_name))
                    dirname = os.path.basename(os.path.dirname(fullpath))
                    self.log.info("Test set: {}".format(dirname))
                    for filename in files:
                        if filename.startswith('.'):
                            continue
                        new_filename = "".join(c for c in filename if c not in bad_chars)
                        test_args = os.path.splitext(new_filename)[0]
                        test_type = dirname + "_" + test_args
                        json_path = os.path.join(dirname, dataset_name, filename)
                        with open(json_path, encoding="utf-8") as json_file:
                            self.log.info("Reading: %s", json_path)
                            datastore = json.load(json_file)
                        datastore.update({'hostname': self.hostname,
                                          'test_args': test_args,
                                          'test_run_date': self.test_run_date,
                                          'dataset_name': dataset_name,
                                          'excluded': False
                                          })
                        if update_db:
                            self._es.create_doc(index=self._es_index, doc_type=self._es_doc_type,
                                                doc_id="%s_%s" % (self.test_run_date, test_type), body=datastore)
                        results[test_type] = datastore
            if not results:
                raise EmptyResultFolder("perf_fast_forward_output folder is empty")

            self.cur_version_info = results[list(results.keys())[0]]['versions']['scylla-server']
            return results

    def exclude_test_run(self, testrun_id=''):
        """Exclude test results by testrun id

        Filter test result by hostname, scylla version and test_run_date filed
        and mark the all found result with flag exluded: True

        Keyword Arguments:
            testrun_id {str} -- testrun id as value of field test_run_date (default: {''})
        """
        if not testrun_id or not self.db_version:
            self.log.info("Nothing to exclude")
            return

        self.log.info('Exclude testrun {} from results'.format(testrun_id))
        filter_path = (
            "hits.hits._id",  # '2018-04-02_18:36:47_large-partition-skips_[64-32.1)'
            "hits.hits._source.hostname",  # 'monster'
            "hits.hits._source.test_run_date",
        )

        testrun_results = self._get_prior_tests(filter_path, f'test_run_date:\"{testrun_id}\"')

        if not testrun_results:
            self.log.info("Nothing to exclude")
            return

        for res in testrun_results['hits']['hits']:
            self.log.info(res['_id'])
            self.log.info(res['_source']['test_run_date'])
            self._es.update_doc(index=self._es_index,
                                doc_type=self._es_doc_type,
                                doc_id=res['_id'],
                                body={'excluded': True})

    def exclude_by_test_id(self, test_id=''):
        """Exclude test result by id

        Filter test result by id (ex. 2018-10-29_18:58:51_large-partition-single-key-slice_begin_incl_0-500000_end_incl.1)
        and mark the test result with flag excluded: True
        Keyword Arguments:
            test_id {str} -- test id from field _id (default: {''})
        """
        if not test_id or not self.db_version:
            self.log.info("Nothing to exclude")
            return

        self.log.info('Exclude test id {} from results'.format(test_id))
        doc = self._es.get_doc(index=self._es_index, doc_id=test_id)
        if doc:
            self._es.update_doc(index=self._es_index,
                                doc_type=self._es_doc_type,
                                doc_id=doc['_id'],
                                body={'excluded': True})
        else:
            self.log.info("Nothing to exclude")
            return

    def exclude_before_date(self, date=''):
        """Exclude all test results before date

        Query test result by hostname and scylla version,
        convert string to date object,
        filter all test result with versions.scylla-server.run_date_time before
        date, and mark them with flag excluded: True

        Keyword Arguments:
            date {str} -- date in format YYYY-MM-DD or YYYY-MM-DD hh:mm:ss (default: {''})
        """
        if not date and not self.db_version:
            self.log.info("Nothing to exclude")
            return
        format_pattern = "%Y-%m-%d %H:%M:%S"

        try:
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            try:
                date = datetime.datetime.strptime(date, format_pattern)
            except ValueError:
                self.log.error("Wrong format of parameter --before-date. Should be \"YYYY-MM-DD\" or \"YYYY-MM-DD hh:mm:ss\"")
                return

        filter_path = (
            "hits.hits._id",
            "hits.hits._source.hostname",
            "hits.hits._source.versions.scylla-server.run_date_time"
        )
        self.log.info('Exclude tests before date {}'.format(date))
        results = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit,  # pylint: disable=unexpected-keyword-arg
                                  q="hostname:'%s' AND versions.scylla-server.version:%s*" %
                                  (self.hostname, self.db_version[:3]))
        if not results:
            self.log.info('Nothing to exclude')
            return

        before_date_results = []
        for doc in results['hits']['hits']:
            doc_date = datetime.datetime.strptime(
                doc['_source']['versions']['scylla-server']['run_date_time'], format_pattern)
            if doc_date < date:
                before_date_results.append(doc)

        for res in before_date_results:
            self._es.update_doc(index=self._es_index,
                                doc_type=self._es_doc_type,
                                doc_id=res['_id'],
                                body={'excluded': True})

    def exclude_testrun_by_commit_id(self, commit_id=None):
        if not commit_id and not self.db_version:
            self.log.info('Nothing to exclude')
            return

        filter_path = (
            "hits.hits._id",
            "hits.hits._source.hostname",
            "hits.hits._source.versions.scylla-server.commit_id",
            "hits.hits._source.test_run_date"
        )

        self.log.info('Exclude tests by commit id #{}'.format(commit_id))

        results = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit,  # pylint: disable=unexpected-keyword-arg
                                  q="hostname:'{}' \
                                  AND versions.scylla-server.version:{}*\
                                  AND versions.scylla-server.commit_id:'{}'".format(self.hostname, self.db_version[:3], commit_id))
        if not results:
            self.log.info('There is no testrun results for commit id #{}'.format(commit_id))
            return

        for doc in results['hits']['hits']:
            self.log.info("Exclude test: %s\nCommit: #%s\nRun Date time: %s\n",
                          doc['_id'],
                          doc['_source']['versions']['scylla-server']['commit_id'],
                          doc['_source']['test_run_date'])
            self._es.update_doc(index=self._es_index,
                                doc_type=self._es_doc_type,
                                doc_id=doc['_id'],
                                body={'excluded': True})


def main(args):
    if args.mode == 'exclude':
        mbra = MicroBenchmarkingResultsAnalyzer(
            email_recipients=None, db_version=args.db_version, es_index=args.es_index)
        if args.testrun_id:
            mbra.exclude_test_run(args.testrun_id)
        if args.test_id:
            mbra.exclude_by_test_id(args.test_id)
        if args.before_date:
            mbra.exclude_before_date(args.before_date)
        if args.commit_id:
            mbra.exclude_testrun_by_commit_id(args.commit_id)
    if args.mode == 'check':
        mbra = MicroBenchmarkingResultsAnalyzer(
            email_recipients=args.email_recipients.split(","), es_index=args.es_index)
        results = mbra.get_results(results_path=args.results_path, update_db=args.update_db)
        if results:
            if args.hostname:
                mbra.hostname = args.hostname
            report_results = mbra.check_regression(results)
            mbra.send_html_report(report_results, html_report_path=args.report_path)
        else:
            LOGGER.warning('Perf_fast_forward testrun is failed or not build results in json format')
            sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Microbencmarking stats utility \
        for upload and analyze or exclude test result from future analyze")

    subparser = parser.add_subparsers(dest='mode',
                                      title='Microbencmarking utility modes',
                                      description='Microbencmarking could be run in two modes definded by subcommand',
                                      metavar='Modes',
                                      help='To see subcommand help use  microbenchmarking.py subcommand -h')
    exclude = subparser.add_parser('exclude', help='Exclude results by testrun, testid or date')
    exclude_group = exclude.add_mutually_exclusive_group(required=True)
    exclude_group.add_argument('--testrun-id', action='store', default='',
                               help='Exclude test results for testrun id')
    exclude_group.add_argument('--test-id', action='store', default='',
                               help='Exclude test result by id')
    exclude_group.add_argument('--before-date', action='store', default='',
                               help='Exclude all test results before date of run stored in field versions.scylla-server.run_date_time.\
                               Value in format YYYY-MM-DD or YYYY-MM-DD hh:mm:ss')
    exclude_group.add_argument('--commit-id', action='store', default='',
                               help='Exclude test run for specific commit id')
    exclude.add_argument('--db-version', action='store', default='',
                         help='Exclude test results for scylla version',
                         required=True)
    exclude.add_argument('--es-index', action="store", default='',
                         help="ES Index for excluding results")

    check = subparser.add_parser('check', help='Upload and analyze test result')
    check.add_argument("--update-db", action="store_true", default=False,
                       help="Upload current microbenchmarking stats to ElasticSearch")
    check.add_argument("--results-path", action="store", default=".",
                       help="Path where to search for test results")
    check.add_argument("--email-recipients", action="store", default="bentsi@scylladb.com",
                       help="Comma separated email addresses list that will get the report")
    check.add_argument("--report-path", action="store", default="",
                       help="Save HTML generated results report to the file path before sending by email")
    check.add_argument("--hostname", action="store", default="",
                       help="Run check regression for host with hostname")
    check.add_argument("--es-index", action="store", default="",
                       help="ES index name where previous results are stored")

    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    main(ARGS)
