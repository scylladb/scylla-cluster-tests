#!/usr/bin/env python2
import os
import sys
import logging
import datetime
import json
import argparse
import socket
import tempfile
from collections import defaultdict
from results_analyze import BaseResultsAnalyzer
# disable InsecureRequestWarning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("microbenchmarking")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class MicroBenchmarkingResultsAnalyzer(BaseResultsAnalyzer):
    def __init__(self, email_recipients, db_version=None):
        super(MicroBenchmarkingResultsAnalyzer, self).__init__(
            es_index="microbenchmarking",
            es_doc_type="microbenchmark",
            send_email=True,
            email_recipients=email_recipients,
            email_template_fp="results_microbenchmark.html",
            query_limit=10000,
            logger=logger
        )
        self.hostname = socket.gethostname()
        self._run_date_pattern = "%Y-%m-%d_%H:%M:%S"
        self.test_run_date = datetime.datetime.now().strftime(self._run_date_pattern)
        self.db_version = db_version

    def check_regression(self, current_results, html_report_path):
        filter_path = (
            "hits.hits._id",  # '2018-04-02_18:36:47_large-partition-skips_[64-32.1)'
            "hits.hits._source.test_args",  # [64-32.1)
            "hits.hits.test_group_properties.name",  # large-partition-skips
            "hits.hits._source.hostname",  # 'godzilla.cloudius-systems.com'
            "hits.hits._source.test_run_date",
            "hits.hits._source.test_group_properties.name",  # large-partition-skips
            "hits.hits._source.results.stats.aio",
            "hits.hits._source.results.stats.cpu",
            "hits.hits._source.results.stats.time (s)",
            "hits.hits._source.results.stats.frag/s",
            "hits.hits._source.versions",
            "hits.hits._source.excluded"
        )

        cur_version_info = current_results[current_results.keys()[0]]['versions']['scylla-server']
        self.db_version = cur_version_info["version"]
        tests_filtered = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit,
                                         q="hostname:'%s' \
                                            AND versions.scylla-server.version:%s* \
                                            AND ((-_exists_:excluded) OR (excluded:false))" % (self.hostname,
                                                                                               self.db_version[:3]))
        assert tests_filtered, "No results from DB"
        results = tests_filtered["hits"]["hits"]
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
        allowed_stats = ('Current', 'Stats', 'Last, commit, date', 'Diff last [%]', 'Best, commit, date', 'Diff best [%]')
        higher_better = ('frag/s',)
        lower_better = ('aio',)
        submetrics = {'frag/s': ['mad f/s', 'max f/s', 'min f/s']}
        metrics = higher_better + lower_better

        def set_results_for(metrica):
            list_of_results_from_db.sort(key=lambda x: datetime.datetime.strptime(x["_source"]["test_run_date"],
                                                                                  self._run_date_pattern))

            def get_metrica_val(x):
                return float(x["_source"]["results"]["stats"][metrica])

            def get_commit_id(x):
                return x["_source"]['versions']['scylla-server']['commit_id']

            def get_commit_date(x):
                return datetime.datetime.strptime(x["_source"]['versions']['scylla-server']['date'],
                                                  "%Y%m%d").date()

            def get_best_result_for_metrica():

                if metrica in higher_better:
                    best_result = max(list_of_results_from_db, key=get_metrica_val)
                elif metrica in lower_better:
                    best_result = min(list_of_results_from_db, key=get_metrica_val)

                return best_result

            def get_diffs():
                diff_best = ((cur_val - best_result_val) / best_result_val) * 100 if best_result_val > 0 else cur_val * 100
                diff_last = ((cur_val - last_val) / last_val) * 100 if last_val > 0 else cur_val * 100

                if metrica in higher_better:
                    diff_best = -diff_best
                    diff_last = -diff_last

                diff_last = -diff_last if diff_last != 0 else 0
                diff_best = -diff_best if diff_best != 0 else 0
                return (diff_last, diff_best)

            if len(list_of_results_from_db) > 1 and get_commit_id(list_of_results_from_db[-1]) == cur_version_info["commit_id"]:
                last_idx = -2
            else:  # when current results are on disk but db is not updated
                last_idx = -1
            cur_val = float(current_result["results"]["stats"][metrica])

            last_val = get_metrica_val(list_of_results_from_db[last_idx])
            last_commit = get_commit_id(list_of_results_from_db[last_idx])
            last_commit_date = get_commit_date(list_of_results_from_db[last_idx])

            best_result = get_best_result_for_metrica()
            best_result_val = get_metrica_val(best_result)
            best_result_commit = get_commit_id(best_result)
            best_commit_date = get_commit_date(best_result)

            diff_last, diff_best = get_diffs()

            stats = {
                "Current": cur_val,
                "Last, commit, date": (last_val, last_commit, last_commit_date),
                "Best, commit, date": (best_result_val, best_result_commit, best_commit_date),
                "Diff last [%]": diff_last,  # diff in percents
                "Diff best [%]": diff_best,
                "has_regression": False,
                "has_improvement": False,

            }

            if (diff_last < -5 or diff_best < -5):
                report_results[test_type]["has_diff"] = True
                stats["has_regression"] = True

            if (diff_last > 50 or diff_best > 50):
                report_results[test_type]['has_improve'] = True
                stats['has_improvement'] = True

            report_results[test_type]["dataset_name"] = current_result['dataset_name']
            report_results[test_type][metrica] = stats

        def set_results_for_sub(metrica):
            report_results[test_type][metrica].update({'Stats': {}})
            for submetrica in submetrics.get(metrica):
                submetrica_cur_val = float(current_result["results"]["stats"][submetrica])
                report_results[test_type][metrica]['Stats'].update({submetrica: submetrica_cur_val})

        for test_type, current_result in current_results.iteritems():
            list_of_results_from_db = sorted_by_type[test_type]
            if not list_of_results_from_db:
                self.log.warning("No results for '%s' in DB. Skipping", test_type)
                continue
            for metrica in metrics:
                self.log.info("Analyzing {test_type}:{metrica}".format(**locals()))
                set_results_for(metrica)
                if metrica in submetrics.keys():
                    set_results_for_sub(metrica)

        subject = "Microbenchmarks - Performance Regression - %s" % self.test_run_date
        dashboard_path = "app/kibana#/dashboard/aee9b370-09db-11e9-a976-2fe0f5890cd0?_g=(filters%3A!())"
        for_render = {
            "subject": subject,
            "testrun_id": self.test_run_date,
            "results": report_results,
            "stats_names": allowed_stats,
            "metrics": metrics,
            "kibana_url": self.gen_kibana_dashboard_url(dashboard_path),
            "full_report": True,
            "hostname": self.hostname,
        }
        for_render.update(dict(test_version=cur_version_info))
        if html_report_path:
            html_file_path = html_report_path
        else:
            html_file_path = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        self.render_to_html(for_render, html_file_path=html_file_path)
        for_render["full_report"] = False
        summary_html = self.render_to_html(for_render)
        self.send_email(subject, summary_html, files=(html_file_path,))

    def get_results(self, results_path, update_db):
        bad_chars = " "
        os.chdir(os.path.join(results_path, "perf_fast_forward_output"))
        results = {}
        for (fullpath, subdirs, files) in os.walk(os.getcwd()):
            self.log.info(fullpath)
            if (os.path.dirname(fullpath).endswith('perf_fast_forward_output') and
                    len(subdirs) > 1):
                raise Exception('Test set {} has more than one datasets: {}'.format(
                    os.path.basename(fullpath),
                    subdirs))

            if not subdirs:
                dataset_name = os.path.basename(fullpath)
                self.log.info('Dataset name: {}'.format(dataset_name))
                dirname = os.path.basename(os.path.dirname(fullpath))
                self.log.info("Test set: {}".format(dirname))
                for filename in files:
                    new_filename = "".join(c for c in filename if c not in bad_chars)
                    test_args = os.path.splitext(new_filename)[0]
                    test_type = dirname + "_" + test_args
                    json_path = os.path.join(dirname, dataset_name, filename)
                    with open(json_path, 'r') as f:
                        self.log.info("Reading: %s", json_path)
                        datastore = json.load(f)
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
            "hits.hits._source.hostname",  # 'godzilla.cloudius-systems.com'
            "hits.hits._source.test_run_date",
        )
        testrun_results = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit,
                                          q="hostname:'%s' AND versions.scylla-server.version:%s* AND test_run_date:\"%s\"" % (self.hostname,
                                                                                                                               self.db_version[:3],
                                                                                                                               testrun_id))
        if not testrun_results:
            self.log.info("Nothing to exclude")
            return

        for res in testrun_results['hits']['hits']:
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
        results = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit,
                                  q="hostname:'%s' AND versions.scylla-server.version:%s*" %
                                  (self.hostname, self.db_version[:3]))
        if not results:
            self.log.info('Nothing to exclude')
            return

        before_date_results = []
        for doc in results['hits']['hits']:
            doc_date = datetime.datetime.strptime(doc['_source']['versions']['scylla-server']['run_date_time'], format_pattern)
            if doc_date < date:
                before_date_results.append(doc)

        for res in before_date_results:
            self._es.update_doc(index=self._es_index,
                                doc_type=self._es_doc_type,
                                doc_id=res['_id'],
                                body={'excluded': True})


def main(args):
    if args.mode == 'exclude':
        mbra = MicroBenchmarkingResultsAnalyzer(email_recipients=None, db_version=args.db_version)
        if args.testrun_id:
            mbra.exclude_test_run(args.testrun_id)
        if args.test_id:
            mbra.exclude_by_test_id(args.test_id)
        if args.before_date:
            mbra.exclude_before_date(args.before_date)
    if args.mode == 'check':
        mbra = MicroBenchmarkingResultsAnalyzer(email_recipients=args.email_recipients.split(","))
        results = mbra.get_results(results_path=args.results_path, update_db=args.update_db)
        if results:
            mbra.check_regression(results, html_report_path=args.report_path)
        else:
            logger.warning('Perf_fast_forward testrun is failed or not build results in json format')
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
    exclude.add_argument('--db-version', action='store', default='',
                         help='Exclude test results for scylla version',
                         required=True)

    check = subparser.add_parser('check', help='Upload and analyze test result')
    check.add_argument("--update-db", action="store_true", default=False,
                       help="Upload current microbenchmarking stats to ElasticSearch")
    check.add_argument("--results-path", action="store", default=".",
                       help="Path where to search for test results")
    check.add_argument("--email-recipients", action="store", default="bentsi@scylladb.com",
                       help="Comma separated email addresses list that will get the report")
    check.add_argument("--report-path", action="store", default="",
                       help="Save HTML generated results report to the file path before sending by email")

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args)
