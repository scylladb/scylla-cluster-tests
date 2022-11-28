import os
import logging
import math
import pprint
from datetime import datetime

import jinja2
from sortedcontainers import SortedDict

from sdcm.db_stats import TestStatsMixin
from sdcm.es import ES
from sdcm.send_email import Email


LOGGER = logging.getLogger(__name__)
PP = pprint.PrettyPrinter(indent=2)


class QueryFilter():
    """
    Definition of query filtering parameters
    """
    SETUP_PARAMS = ['n_db_nodes', 'n_loaders', 'n_monitor_nodes']
    SETUP_INSTANCE_PARAMS = ['instance_type_db', 'instance_type_loader', 'instance_type_monitor']

    def __init__(self, test_doc, is_gce=False, use_wide_query=False):
        self.test_doc = test_doc
        self.test_name = test_doc["_source"]["test_details"]["test_name"]
        self.is_gce = is_gce
        self.date_re = '/.*/'
        self.use_wide_query = use_wide_query

    def setup_instance_params(self):
        return ['gce_' + param for param in self.SETUP_INSTANCE_PARAMS] if self.is_gce else self.SETUP_INSTANCE_PARAMS

    def filter_setup_details(self):
        setup_details = ''
        for param in self.SETUP_PARAMS + self.setup_instance_params():
            if setup_details:
                setup_details += ' AND '
            setup_details += 'setup_details.{}: {}'.format(param, self.test_doc['_source']['setup_details'][param])
        return setup_details

    def filter_test_details(self):
        test_details = 'test_details.job_name:\"{}\" '.format(
            self.test_doc['_source']['test_details']['job_name'].split('/')[0])
        test_details += self.test_cmd_details()
        test_details += ' AND test_details.time_completed: {}'.format(self.date_re)
        test_details += ' AND test_details.test_name: {}'.format(self.test_name.replace(":", r"\:"))
        return test_details

    def test_cmd_details(self):
        raise NotImplementedError('Derived classes must implement this method.')

    def filter_by_dashboard_query(self):
        if "throughput" in self.test_doc['_source']['test_details']['job_name']:
            test_type = r'results.stats.op\ rate:*'
        elif "latency" in self.test_doc['_source']['test_details']['job_name']:
            test_type = r'results.stats.latency\ 99th\ percentile:*'
        else:
            test_type = ""
        return test_type

    def build_query(self):
        query = [self.filter_test_details()]

        if not self.use_wide_query:
            query.append(self.filter_setup_details())
        else:
            query.append(self.filter_by_dashboard_query())

        return " AND ".join([q for q in query if q])

    def __call__(self, *args, **kwargs):
        try:
            return self.build_query()
        except KeyError:
            LOGGER.exception('Expected parameters for filtering are not found , test {}'.format(self.test_doc['_id']))
        return None


class PerformanceQueryFilter(QueryFilter):

    ALLOWED_PERFORMANCE_JOBS = [
        {
            "job_folder": "Perf-Regression",
            "job_prefix": ["scylla-release-perf-regression"]
        },
        {
            "job_folder": "scylla-master",
            "job_prefix": ["scylla-master-perf-regression"]
        },

    ]

    def filter_test_details(self):
        test_details = self.build_filter_job_name()
        if not self.use_wide_query:
            test_details += self.test_cmd_details()
            test_details += ' AND test_details.time_completed: {}'.format(self.date_re)
        test_details += ' AND {}'.format(self.build_filter_test_name())
        return test_details

    def build_filter_job_name(self):
        """Prepare filter to search by job name

        Select performance tests if job name is located in folder
        MAIN_JOB_FOLDERS, of job name starts from MAIN_JOB_FOLDER item( this
        is mostly doing for collecting previous tests)

        For other job filter documents only with exact same job name
        :returns: [description]
        :rtype: {[type]}
        """
        job_name = self.test_doc['_source']['test_details']['job_name'].split("/")

        def get_query_filter_by_job_folder(job_item):
            filter_query = ""
            if job_name[0] and job_name[0] in job_item['job_folder']:
                base_job_name = job_name[1]
                if self.use_wide_query:
                    filter_query = r'test_details.job_name.keyword: {}\/{} '.format(job_name[0],
                                                                                    base_job_name)
                else:
                    filter_query = r'(test_details.job_name.keyword: {}\/{} OR'.format(job_name[0],
                                                                                       base_job_name)
                    filter_query += r' test_details.job_name.keyword: {}) '.format(base_job_name)
            return filter_query

        def get_query_filter_by_job_prefix(job_item):
            filter_query = ""
            for job_prefix in job_item['job_prefix']:
                if not job_name[0].startswith(job_prefix):
                    continue
                base_job_name = job_name[0]
                if self.use_wide_query:
                    filter_query = r'test_details.job_name.keyword: {}\/{} '.format(job_item['job_folder'],
                                                                                    base_job_name)
                else:
                    filter_query = r'(test_details.job_name.keyword: {}\/{} OR'.format(job_item['job_folder'],
                                                                                       base_job_name)
                    filter_query += r' test_details.job_name.keyword: {}) '.format(base_job_name)
                break
            return filter_query

        job_filter_query = ""
        for job_item in self.ALLOWED_PERFORMANCE_JOBS:
            job_filter_query = get_query_filter_by_job_folder(job_item)
            if not job_filter_query:
                job_filter_query = get_query_filter_by_job_prefix(job_item)
            if job_filter_query:
                break

        if not job_filter_query:
            job_filter_query = r'test_details.job_name.keyword: {} '.format(
                self.test_doc['_source']['test_details']['job_name'])

        return job_filter_query

    def build_filter_test_name(self):
        new_test_name = ""
        avocado_test_name = ""
        if ".py:" in self.test_name:
            new_test_name = self.test_name.replace(".py:", ".")
            avocado_test_name = self.test_name.replace(":", r"\:")
        else:
            avocado_test_name = self.test_name.replace(".", r".py\:", 1)
            new_test_name = self.test_name
        if self.use_wide_query:
            return " test_details.test_name: {}".format(new_test_name)
        else:
            return " (test_details.test_name: {} OR test_details.test_name: {})".format(new_test_name, avocado_test_name)

    def test_cmd_details(self):
        raise NotImplementedError('Derived classes must implement this method.')


class QueryFilterCS(QueryFilter):

    _CMD = ('cassandra-stress', )
    _PRELOAD_CMD = ('preload-cassandra-stress', )
    _PARAMS = ('command', 'cl', 'rate threads', 'schema', 'mode', 'pop', 'duration')
    _PROFILE_PARAMS = ('command', 'profile', 'ops', 'rate threads', 'duration')

    def test_details_params(self):
        return self._CMD + self._PRELOAD_CMD if \
            self.test_doc['_source']['test_details'].get(self._PRELOAD_CMD[0]) else self._CMD

    def cs_params(self):
        return self._PROFILE_PARAMS if self.test_name.endswith('profiles') else self._PARAMS

    def test_cmd_details(self):
        test_details = ""
        for cassandra_stress in self.test_details_params():
            for param in self.cs_params():
                if param == 'rate threads':
                    test_details += r' AND test_details.{}.rate\ threads: {}'.format(
                        cassandra_stress, self.test_doc['_source']['test_details'][cassandra_stress][param])
                elif param == 'duration' and cassandra_stress.startswith('preload'):
                    continue
                else:
                    if param not in self.test_doc['_source']['test_details'][cassandra_stress]:
                        continue
                    param_val = self.test_doc['_source']['test_details'][cassandra_stress][param]
                    if param in ['profile', 'ops']:
                        param_val = "\"{}\"".format(param_val)
                    test_details += ' AND test_details.{}.{}: {}'.format(cassandra_stress, param, param_val)
        return test_details


class PerformanceFilterCS(QueryFilterCS, PerformanceQueryFilter):
    pass


class QueryFilterScyllaBench(QueryFilter):

    _CMD = ('scylla-bench', )
    _PARAMS = ('mode', 'workload', 'partition-count', 'clustering-row-count', 'concurrency', 'connection-count',
               'replication-factor', 'duration')

    def test_cmd_details(self):
        test_details = ['AND test_details.{}.{}: {}'.format(
            cmd, param, self.test_doc['_source']['test_details'][cmd][param])
            for param in self._PARAMS for cmd in self._CMD]

        return ' '.join(test_details)


class PerformanceFilterScyllaBench(QueryFilterScyllaBench, PerformanceQueryFilter):
    pass


class BaseResultsAnalyzer:  # pylint: disable=too-many-instance-attributes
    def __init__(self, es_index, es_doc_type, send_email=False, email_recipients=(),  # pylint: disable=too-many-arguments
                 email_template_fp="", query_limit=1000, logger=None):
        self._es = ES()
        self._conf = self._es._conf  # pylint: disable=protected-access
        self._es_index = es_index
        self._es_doc_type = es_doc_type
        self._limit = query_limit
        self._send_email = send_email
        self._email_recipients = email_recipients
        self._email_template_fp = email_template_fp
        self.log = logger if logger else LOGGER

    def get_all(self):
        """
        Get all the test results in json format
        """
        return self._es.search(index=self._es_index, size=self._limit)  # pylint: disable=unexpected-keyword-arg

    def get_test_by_id(self, test_id):
        """
        Get test results by test id
        :param test_id: test id created by performance test
        :return: test results in json format
        """
        if not self._es.exists(index=self._es_index, doc_type=self._es_doc_type, id=test_id):
            self.log.error('Test results not found: {}'.format(test_id))
            return None
        return self._es.get(index=self._es_index, doc_type=self._es_doc_type, id=test_id)

    def _test_version(self, test_doc):
        if test_doc['_source'].get('versions'):
            for value in ('scylla-server', 'scylla-enterprise-server'):
                key = test_doc['_source']['versions'].get(value)
                if key:
                    return key

        self.log.error('Scylla version is not found for test %s', test_doc['_id'])
        return None

    def render_to_html(self, results, html_file_path="", template=None):
        """
        Render analysis results to html template
        :param results: results dictionary
        :param html_file_path: Boolean, whether to save html file on disk
        :param template: If not None, define the template to use, default to `self._email_template_fp`
        :return: html string
        """
        email_template_fp = template if template else self._email_template_fp
        self.log.info("Rendering results to html using '%s' template...", email_template_fp)
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
        print(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'])
        template = env.get_template(email_template_fp)
        html = template.render(results)
        self.log.info("Results has been rendered to html")
        if html_file_path:
            with open(html_file_path, "w") as html_file:
                html_file.write(html)
            self.log.info("HTML report saved to '%s'.", html_file_path)
        return html

    def send_email(self, subject, content, html=True, files=()):
        if self._send_email and self._email_recipients:
            self.log.debug('Send email to {}'.format(self._email_recipients))
            email = Email()
            email.send(subject, content, html=html, recipients=self._email_recipients, files=files)
        else:
            self.log.warning("Won't send email (send_email: %s, recipients: %s)",
                             self._send_email, self._email_recipients)

    def gen_kibana_dashboard_url(self, dashboard_path=""):
        return "%s/%s" % (self._conf.get('kibana_url'), dashboard_path)


class SpecifiedStatsPerformanceAnalyzer(BaseResultsAnalyzer):
    """
    Get specified performance test results from elasticsearch DB and analyze it to find a regression
    """

    def __init__(self, es_index, es_doc_type, send_email, email_recipients, logger=None):   # pylint: disable=too-many-arguments
        super(SpecifiedStatsPerformanceAnalyzer, self).__init__(
            es_index=es_index,
            es_doc_type=es_doc_type,
            send_email=send_email,
            email_recipients=email_recipients,
            email_template_fp="",
            logger=logger
        )

    def _test_stats(self, test_doc):
        # check if stats exists
        if 'results' not in test_doc['_source']:
            self.log.error('Cannot find the field: results for test id: {}!'.format(test_doc['_id']))
            return None
        return test_doc['_source']['results']

    def check_regression(self, test_id, dict_specific_tested_stats):  # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        """
        Get test results by id, filter similar results and calculate DB values for each version,
        then compare with max-allowed in the tested version (and report all the found versions).
        :param test_id: test id created by performance test
        :param is_gce: is gce instance
        """
        allowed_deviation = 1.20
        # get test res
        doc = self.get_test_by_id(test_id)
        if not doc:
            self.log.error('Cannot find test by id: {}!'.format(test_id))
            return False

        test_stats = self._test_stats(doc)
        if not test_stats:
            self.log.debug("Could not find test statistics, regression check is skipped")
            return False
        es_base_path = 'hits.hits'
        es_source_path = es_base_path + '._source'
        filter_path = ['.'.join([es_base_path, '_id']),
                       '.'.join([es_source_path, 'results', 'throughput']),
                       '.'.join([es_source_path, 'versions'])]

        for stat in dict_specific_tested_stats.keys():  # Add all requested specific-stats to be retrieved from ES DB.
            stat_path = '.'.join([es_source_path, stat])
            filter_path.append(stat_path)

        tests_filtered = self._es.search(index=self._es_index, filter_path=filter_path, size=self._limit)
        self.log.debug("Filtered tests found are: {}".format(tests_filtered))

        if not tests_filtered:
            self.log.info('Cannot find tests with the same parameters as {}'.format(test_id))
            return False
        cur_test_version = None
        tested_params = dict_specific_tested_stats.keys()
        group_by_version = dict()
        # repair_runtime result example:
        # { u'_id': u'20190303-105120-405065',
        #   u'_index': u'performanceregressionrowlevelrepairtest',
        #   u'_source': { u'coredumps': { },
        #                 u'errors': { },
        #                 u'nemesis': { },
        #                 u'repair_runtime': 11.847206830978394,
        #                 u'results': { u'latency_read_99': { },
        #                               u'latency_write_99': { },
        #                               u'repair_runtime': -1,
        #                               u'throughput': { }},
        #
        # # Find the average results for each version per tested param (stats)
        for tag_row in tests_filtered['hits']['hits']:
            if '_source' not in tag_row:  # non-valid record?
                self.log.error('Skip non-valid test: %s', tag_row['_id'])
                continue
            if not tag_row['_source']['versions'] or 'scylla-server' not in tag_row['_source']['versions']:
                continue
            version_info = tag_row['_source']['versions']['scylla-server']
            version = version_info['version']
            self.log.debug("version_info={} version={}".format(version_info, version))

            if tag_row['_id'] == test_id:  # save the current test values
                cur_test_version = version
                continue

            if not version:
                continue

            # group_by_version example:
            #   { '2.3.0':
            #       { 'repair_runtime': [12.345, 13.031]},
            #     '3.1.0': { 'repair_runtime': [5.341]}
            #   }
            if version not in group_by_version:
                group_by_version[version] = dict()
                for param in tested_params:
                    if param in tag_row['_source']:
                        group_by_version[version][param] = [tag_row['_source'][param]]
            else:
                for param in tested_params:
                    if param in tag_row['_source']:
                        if param not in group_by_version[version]:
                            group_by_version[version][param] = [tag_row['_source'][param]]
                        else:
                            group_by_version[version][param].append(tag_row['_source'][param])

            self.log.debug("group_by_version={}".format(group_by_version))

        if not cur_test_version:
            raise ValueError("Could not retrieve current test details from database")
        for param in tested_params:
            if param in group_by_version[cur_test_version]:
                cur_test_param_result = dict_specific_tested_stats[param]
                list_param_stats = group_by_version[cur_test_version][param]
                param_avg = sum(list_param_stats) / float(len(list_param_stats))
                deviation_limit = param_avg * allowed_deviation
                self.log.info(
                    "Performance result for: {} is: {}. (average statistics deviation limit is: {}".format(param,
                                                                                                           cur_test_param_result,
                                                                                                           deviation_limit))
                for version in group_by_version:
                    if param in group_by_version[version]:
                        list_param_results = group_by_version[version][param]
                        version_avg = sum(list_param_results) / float(len(list_param_results))
                        self.log.info("Performance average of {} results for: {} on version: {} is: {}".format(
                            len(list_param_results), param, version, version_avg))
                assert float(
                    cur_test_param_result) < deviation_limit, "Current test performance for: {} exceeds allowed deviation ({})".format(
                    param, deviation_limit)
        return True


class PerformanceResultsAnalyzer(BaseResultsAnalyzer):
    """
    Get performance test results from elasticsearch DB and analyze it to find a regression
    """

    PARAMS = TestStatsMixin.STRESS_STATS

    def __init__(self, es_index, es_doc_type, send_email, email_recipients, logger=None):  # pylint: disable=too-many-arguments
        super(PerformanceResultsAnalyzer, self).__init__(
            es_index=es_index,
            es_doc_type=es_doc_type,
            send_email=send_email,
            email_recipients=email_recipients,
            email_template_fp="results_performance.html",
            logger=logger
        )

    @staticmethod
    def _remove_non_stat_keys(stats):
        for non_stat_key in ['loader_idx', 'cpu_idx', 'keyspace_idx']:
            if non_stat_key in stats:
                del stats[non_stat_key]
        return stats

    def _test_stats(self, test_doc):
        # check if stats exists
        if 'results' not in test_doc['_source'] or 'stats_average' not in test_doc['_source']['results'] or \
                'stats_total' not in test_doc['_source']['results']:
            self.log.error('Cannot find one of the fields: results, results.stats_average, '
                           'results.stats_total for test id: {}!'.format(test_doc['_id']))
            return None
        stats_average = self._remove_non_stat_keys(test_doc['_source']['results']['stats_average'])
        stats_total = test_doc['_source']['results']['stats_total']
        if not stats_average or not stats_total or any([stats_average[k] == '' for k in self.PARAMS]):
            self.log.error('Cannot find average/total results for test: {}!'.format(test_doc['_id']))
            return None
        # replace average by total value for op rate
        stats_average['op rate'] = stats_total['op rate']
        return stats_average

    def _test_version(self, test_doc):
        if test_doc['_source'].get('versions'):
            for value in ('scylla-server', 'scylla-enterprise-server'):
                key = test_doc['_source']['versions'].get(value)
                if key:
                    return key

        self.log.error('Scylla version is not found for test %s', test_doc['_id'])
        return None

    @staticmethod
    def _get_grafana_snapshot(test_doc):
        grafana_snapshots = test_doc['_source']['test_details'].get('grafana_snapshots')
        if grafana_snapshots and isinstance(grafana_snapshots, list):
            return grafana_snapshots
        elif grafana_snapshots and isinstance(grafana_snapshots, str):
            return [grafana_snapshots]
        else:
            return []

    @staticmethod
    def _get_grafana_screenshot(test_doc):
        grafana_screenshots = test_doc['_source']['test_details'].get('grafana_screenshot')
        if not grafana_screenshots:
            grafana_screenshots = test_doc['_source']['test_details'].get('grafana_screenshots')

        if grafana_screenshots and isinstance(grafana_screenshots, list):
            return grafana_screenshots
        elif grafana_screenshots and isinstance(grafana_screenshots, str):
            return [grafana_screenshots]
        else:
            return []

    @staticmethod
    def _get_setup_details(test_doc, is_gce):
        setup_details = {'cluster_backend': test_doc['_source']['setup_details'].get('cluster_backend')}
        if "aws" in setup_details['cluster_backend']:
            setup_details['ami_id_db_scylla'] = test_doc['_source']['setup_details']['ami_id_db_scylla']
        for setup_param in QueryFilter(test_doc, is_gce).setup_instance_params():
            setup_details.update(
                [(setup_param.replace('gce_', ''), test_doc['_source']['setup_details'].get(setup_param))])
        return setup_details

    def _get_best_value(self, key, val1, val2):
        if key == self.PARAMS[0]:  # op rate
            return val1 if val1 > val2 else val2
        return val1 if val2 == 0 or val1 < val2 else val2  # latency

    @staticmethod
    def _query_filter(test_doc, is_gce, use_wide_query=False):
        return PerformanceFilterScyllaBench(test_doc, is_gce)() if test_doc['_source']['test_details'].get('scylla-bench')\
            else PerformanceFilterCS(test_doc, is_gce, use_wide_query)()

    def cmp(self, src, dst, version_dst, best_test_id):
        """
        Compare current test results with the best results
        :param src: current test results
        :param dst: previous best test results
        :param version_dst: scylla server version to compare with
        :param best_test_id: the best results test id(for each parameter)
        :return: dictionary with compare calculation results
        """
        cmp_res = dict(version_dst=version_dst, res=dict())
        for param in self.PARAMS:
            param_key_name = param.replace(' ', '_')
            status = 'Progress'
            try:
                delta = src[param] - dst[param]
                change_perc = int(math.fabs(delta) * 100 / dst[param])
                best_id = best_test_id[param]
                if (param.startswith('latency') and delta > 0) or (param == 'op rate' and delta < 0):
                    status = 'Regression'
                if change_perc == 0:
                    status = "Difference"
                cmp_res['res'][param_key_name] = dict(percent='{}%'.format(change_perc),
                                                      val=src[param],
                                                      best_val=dst[param],
                                                      best_id=best_id,
                                                      status=status)
            except TypeError:
                self.log.exception('Failed to compare {} results: {} vs {}, version {}'.format(
                    param, src[param], dst[param], version_dst))
        return cmp_res

    def check_regression(self, test_id, is_gce=False, use_wide_query=False):
        """
        Get test results by id, filter similar results and calculate max values for each version,
        then compare with max in the test version and all the found versions.
        Save the analysis in log and send by email.
        :param test_id: test id created by performance test
        :param is_gce: is gce instance
        :return: True/False
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements

        # get test res
        doc = self.get_test_by_id(test_id)
        if not doc:
            self.log.error('Cannot find test by id: {}!'.format(test_id))
            return False
        self.log.debug(PP.pformat(doc))

        test_stats = self._test_stats(doc)
        if not test_stats:
            return False

        # filter tests
        query = self._query_filter(doc, is_gce, use_wide_query)
        if not query:
            return False
        self.log.debug("Query to ES: %s", query)
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.results.stats_total',
                       'hits.hits._source.results.throughput',
                       'hits.hits._source.versions']
        tests_filtered = self._es.search(index=self._es_index, q=query, filter_path=filter_path,  # pylint: disable=unexpected-keyword-arg
                                         size=self._limit)

        if not tests_filtered:
            self.log.info('Cannot find tests with the same parameters as {}'.format(test_id))
            return False
        # get the best res for all versions of this job
        group_by_version = dict()
        # Example:
        # group_by_version = {
        #     "2.3.rc1": {
        #         "tests": {  # SortedDict(),
        #             "20180726": {
        #                 "latency 99th percentile": 10.3,
        #                 "op rate": 15034.3
        #                 #...
        #             }
        #         },
        #
        #         "stats_best": {
        #             "op rate": 0,
        #             "latency mean": 0,
        #         },
        #         "best_test_id": {
        #             "op rate": {"commit": 9b4a0a287", "date": "2020.02.02"},
        #             "latency mean": {"commit": 9b4a0a287", "date": "2020.02.02"},
        #
        #         }
        #     }
        # }
        # Find best results for each version
        for row in tests_filtered['hits']['hits']:
            if row['_id'] == test_id:  # filter the current test
                continue
            if '_source' not in row:  # non-valid record?
                self.log.error('Skip non-valid test: %s', row['_id'])
                continue
            if not row['_source']['versions'] or 'scylla-server' not in row['_source']['versions']:
                continue
            version_info = self._test_version(row)
            version = version_info.get('version')
            self.log.debug("version_info={} version={}".format(version_info, version))
            if not version:
                continue
            curr_test_stats = self._test_stats(row)
            if not curr_test_stats:
                continue

            formated_version_date = datetime.strptime(version_info['date'], "%Y%m%d").strftime("%Y-%m-%d")
            version_info_data = {"commit": version_info['commit_id'], "date": formated_version_date}

            if version not in group_by_version:
                group_by_version[version] = dict(tests=SortedDict(), stats_best=dict(), best_test_id=dict())
                group_by_version[version]['stats_best'] = {k: 0 for k in self.PARAMS}
                group_by_version[version]['best_test_id'] = {
                    k: version_info_data for k in self.PARAMS}
            group_by_version[version]['tests'][version_info['date']] = {
                "test_stats": curr_test_stats,
                "version": {k: version_info_data for k in self.PARAMS}
            }
            old_best = group_by_version[version]['stats_best']
            group_by_version[version]['stats_best'] =\
                {k: self._get_best_value(k, curr_test_stats[k], old_best[k])
                 for k in self.PARAMS if k in curr_test_stats and k in old_best}
            # replace best test id if best value changed
            for k in self.PARAMS:
                if k in curr_test_stats and k in old_best and\
                        group_by_version[version]['stats_best'][k] == curr_test_stats[k]:
                    group_by_version[version]['best_test_id'][k] = version_info_data
        res_list = list()
        # compare with the best in the test version and all the previous versions
        test_version_info = self._test_version(doc)
        test_version = test_version_info['version']

        for version in group_by_version:
            if version == test_version and not group_by_version[test_version]['tests']:
                self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(test_stats,
                               group_by_version[version]['stats_best'],
                               version,
                               group_by_version[version]['best_test_id'])
            _, latest_version_test = group_by_version[version]["tests"].peekitem(index=-1)
            latest_res = self.cmp(test_stats,
                                  latest_version_test["test_stats"],
                                  version,
                                  latest_version_test["version"])
            res_list.append({"best": cmp_res, "last": latest_res})
        if not res_list:
            self.log.info('No test results to compare with')
            return False
        # send results by email
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        cassandra_stress = doc['_source']['test_details'].get('cassandra-stress')
        dashboard_path = "app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"
        results = dict(test_name=full_test_name,
                       test_start_time=str(test_start_time),
                       test_version=test_version_info,
                       res_list=res_list,
                       setup_details=self._get_setup_details(doc, is_gce),
                       prometheus_stats={stat: doc["_source"]["results"].get(
                           stat, {}) for stat in TestStatsMixin.PROMETHEUS_STATS},
                       prometheus_stats_units=TestStatsMixin.PROMETHEUS_STATS_UNITS,
                       grafana_snapshots=self._get_grafana_snapshot(doc),
                       grafana_screenshots=self._get_grafana_screenshot(doc),
                       cs_raw_cmd=cassandra_stress.get('raw_cmd', "") if cassandra_stress else "",
                       job_url=doc['_source']['test_details'].get('job_url', ""),
                       dashboard_master=self.gen_kibana_dashboard_url(dashboard_path),
                       )
        self.log.debug('Regression analysis:')
        self.log.debug(PP.pformat(results))
        test_name = full_test_name.split('.')[-1]  # Example: longevity_test.py:LongevityTest.test_custom_time
        subject = 'Performance Regression Compare Results - {} - {}'.format(test_name, test_version)
        html = self.render_to_html(results)
        self.send_email(subject, html)

        return True

    def check_regression_with_subtest_baseline(self, test_id, base_test_id, subtest_baseline, is_gce=False):
        """
        Get test results by id, filter similar results and calculate max values for each version,
        then compare with max in the test version and all the found versions.
        Save the analysis in log and send by email.
        :param test_id: test id created by performance test
        :param base_test_id: current test id
        :param subtest_baseline: which of the subtest is the baseline
        :param is_gce: is gce instance
        :return: True/False
        """
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements

        doc = self.get_test_by_id(test_id)
        if not doc:
            self.log.error('Cannot find test by id: {}!'.format(test_id))
            return False
        self.log.debug(PP.pformat(doc))

        test_stats = self._test_stats(doc)
        if not test_stats:
            return False

        # filter tests
        query = self._query_filter(doc, is_gce)
        self.log.debug(query)
        if not query:
            return False
        self.log.debug("Query to ES: %s", query)
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.results.stats_total',
                       'hits.hits._source.results.throughput',
                       'hits.hits._source.results',
                       'hits.hits._source.versions',
                       'hits.hits._source.test_details']
        tests_filtered = self._es.search(index=self._es_index, q=query, filter_path=filter_path,
                                         # pylint: disable=unexpected-keyword-arg
                                         size=self._limit)

        if not tests_filtered:
            self.log.info('Cannot find tests with the same parameters as {}'.format(test_id))
            return False
        # get the best res for all versions of this job
        group_by_version_sub_type = dict()
        # Example:
        # group_by_type = {
        #     "version": {
        #           "cdc_disabled|cdc_enabled|cdc_pre_image": {
        #               "tests": {  # SortedDict(),
        #                   "20180726": {
        #                       "latency 99th percentile": 10.3,
        #                       "op rate": 15034.3
        #                       #...
        #                   }
        #               },
        #
        #               "stats_best": {
        #                   "op rate": 0,
        #                   "latency mean": 0,
        #               },
        #               "best_test_id": {
        #                   "op rate": "9b4a0a287",
        #                   "latency mean": "9b4a0a287",
        #
        #               }
        #           }
        #      }
        # }
        # Find best results for each version

        current_tests = dict()
        grafana_snapshots = dict()
        grafana_screenshots = dict()
        for row in tests_filtered['hits']['hits']:
            if '_source' not in row:  # non-valid record?
                self.log.error('Skip non-valid test: %s', row['_id'])
                continue
            version_info = self._test_version(row)
            version = version_info['version']
            if not version:
                continue
            if "results" not in row["_source"]:
                continue
            sub_type = row["_source"]['test_details']['sub_type']
            curr_test_stats = self._test_stats(row)
            if not curr_test_stats:
                continue
            if base_test_id in row["_id"]:
                if sub_type not in current_tests and curr_test_stats:
                    current_tests[sub_type] = dict()
                    current_tests[sub_type]['stats'] = curr_test_stats
                    current_tests[sub_type]['version'] = version_info
                    current_tests[sub_type]['best_test_id'] = {
                        k: f"Commit: {version_info['commit_id']}, Date: {version_info['date']}" for k in self.PARAMS}
                    current_tests[sub_type]['results'] = row['_source']['results']

                    grafana_snapshots[sub_type] = self._get_grafana_snapshot(row)
                    grafana_screenshots[sub_type] = self._get_grafana_screenshot(row)

                    continue

            if version not in group_by_version_sub_type:
                group_by_version_sub_type[version] = dict()

            if sub_type not in group_by_version_sub_type:
                group_by_version_sub_type[version][sub_type] = dict(tests=SortedDict(), stats_best=dict(),
                                                                    best_test_id=dict())
                group_by_version_sub_type[version][sub_type]['stats_best'] = {k: 0 for k in self.PARAMS}
                group_by_version_sub_type[version][sub_type]['best_test_id'] = {
                    k: f"Commit: {version_info['commit_id']}, Date: {version_info['date']}" for k in self.PARAMS}
            group_by_version_sub_type[version][sub_type]['tests'][version_info['date']] = curr_test_stats
            old_best = group_by_version_sub_type[version][sub_type]['stats_best']
            group_by_version_sub_type[version][sub_type]['stats_best'] = \
                {k: self._get_best_value(k, curr_test_stats[k], old_best[k])
                 for k in self.PARAMS if k in curr_test_stats and k in old_best}
            # replace best test id if best value changed
            for k in self.PARAMS:
                if k in curr_test_stats and k in old_best and \
                        group_by_version_sub_type[version][sub_type]['stats_best'][k] == curr_test_stats[k]:
                    group_by_version_sub_type[version][sub_type]['best_test_id'][
                        k] = f"Commit: {version_info['commit_id']}, Date: {version_info['date']}"

        current_res_list = list()
        versions_res_list = list()

        test_version_info = self._test_version(doc)
        test_version = test_version_info['version']
        base_line = current_tests.get(subtest_baseline)
        for sub_type in current_tests:
            if not current_tests[sub_type] or sub_type == subtest_baseline:
                self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(current_tests[sub_type]['stats'],
                               base_line['stats'],
                               sub_type,
                               current_tests[sub_type]['best_test_id'])
            current_res_list.append(cmp_res)

        if not current_res_list:
            self.log.info('No test results to compare with')
            return False
        current_res_list = sorted(current_res_list, key=lambda x: x['version_dst'])
        current_prometheus_stats = SortedDict()
        for sub_type in current_tests:
            current_prometheus_stats[sub_type] = {stat: current_tests[sub_type]["results"].get(
                stat, {}) for stat in TestStatsMixin.PROMETHEUS_STATS}

        for version in group_by_version_sub_type:
            cmp_res = dict()
            for sub_type in group_by_version_sub_type[version]:
                if not group_by_version_sub_type[version][sub_type]['tests']:
                    self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                    continue
                if sub_type not in current_tests:
                    continue
                cmp_res[sub_type] = self.cmp(current_tests[sub_type]['stats'],
                                             group_by_version_sub_type[version][sub_type]['stats_best'],
                                             version,
                                             group_by_version_sub_type[version][sub_type]['best_test_id'])
            versions_res_list.append({version: cmp_res})

        # send results by email
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        cassandra_stress = doc['_source']['test_details'].get('cassandra-stress')
        ycsb = doc['_source']['test_details'].get('ycsb')
        dashboard_path = "app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"
        results = dict(test_name=full_test_name,
                       test_start_time=str(test_start_time),
                       test_version=test_version_info,
                       base_line=base_line,
                       res_list=current_res_list,
                       ver_res_list=versions_res_list,
                       setup_details=self._get_setup_details(doc, is_gce),
                       prometheus_stats=current_prometheus_stats,
                       prometheus_stats_units=TestStatsMixin.PROMETHEUS_STATS_UNITS,
                       grafana_snapshots=grafana_snapshots,
                       grafana_screenshots=grafana_screenshots,
                       cs_raw_cmd=cassandra_stress.get('raw_cmd', "") if cassandra_stress else "",
                       ycsb_raw_cmd=ycsb.get('raw_cmd', "") if ycsb else "",
                       job_url=doc['_source']['test_details'].get('job_url', ""),
                       dashboard_master=self.gen_kibana_dashboard_url(dashboard_path),
                       baseline_type=subtest_baseline,
                       )
        self.log.debug('Regression analysis:')
        self.log.debug(PP.pformat(results))
        test_name = full_test_name.split('.', 1)[1]  # Example: longevity_test.py:LongevityTest.test_custom_time
        subject = 'Performance Regression Compare Results - {} - {}'.format(test_name, test_version)
        if ycsb:
            subject = '(Alternator) Performance Regression - {} - {}'.format(test_name, test_version)
        html = self.render_to_html(results, template='results_performance_baseline.html')
        self.send_email(subject, html)

        return True
