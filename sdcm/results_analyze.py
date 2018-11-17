import os
import logging
import math
import jinja2
import pprint
from es import ES
from send_email import Email
from datetime import datetime
from db_stats import TestStatsMixin


log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


class QueryFilter(object):
    """
    Definition of query filtering parameters
    """
    SETUP_PARAMS = ['n_db_nodes', 'n_loaders', 'n_monitor_nodes']
    SETUP_INSTANCE_PARAMS = ['instance_type_db', 'instance_type_loader', 'instance_type_monitor']

    def __init__(self, test_doc, is_gce=False):
        self.test_doc = test_doc
        self.test_name = test_doc["_source"]["test_details"]["test_name"]
        self.is_gce = is_gce
        self.date_re = '/2018-*/'

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
        test_details = 'test_details.job_name:{} '.format(
            self.test_doc['_source']['test_details']['job_name'].split('/')[0])
        test_details += self.test_cmd_details()
        test_details += ' AND test_details.time_completed: {}'.format(self.date_re)
        test_details += ' AND test_details.test_name: {}'.format(self.test_name.replace(":", "\:"))
        return test_details

    def test_cmd_details(self):
        raise NotImplementedError('Derived classes must implement this method.')

    def __call__(self, *args, **kwargs):
        try:
            return '{} AND {}'.format(self.filter_test_details(), self.filter_setup_details())
        except KeyError:
            log.exception('Expected parameters for filtering are not found , test {}'.format(self.test_doc['_id']))
        return None


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
        for cs in self.test_details_params():
            for param in self.cs_params():
                if param == 'rate threads':
                    test_details += ' AND test_details.{}.rate\ threads: {}'.format(
                        cs, self.test_doc['_source']['test_details'][cs][param])
                elif param == 'duration' and cs.startswith('preload'):
                    continue
                else:
                    param_val = self.test_doc['_source']['test_details'][cs][param]
                    if param in ['profile', 'ops']:
                        param_val = "\"{}\"".format(param_val)
                    test_details += ' AND test_details.{}.{}: {}'.format(cs, param, param_val)
        return test_details


class QueryFilterScyllaBench(QueryFilter):

    _CMD = ('scylla-bench', )
    _PARAMS = ('mode', 'workload', 'partition-count', 'clustering-row-count', 'concurrency', 'connection-count',
               'replication-factor', 'duration')

    def test_cmd_details(self):
        test_details = ['AND test_details.{}.{}: {}'.format(
            cmd, param, self.test_doc['_source']['test_details'][cmd][param])
            for param in self._PARAMS for cmd in self._CMD]

        return ' '.join(test_details)


class BaseResultsAnalyzer(object):
    def __init__(self, es_index, es_doc_type, send_email=False, email_recipients=(),
                 email_template_fp="", query_limit=1000, logger=None):
        self._es = ES()
        self._conf = self._es._conf
        self._es_index = es_index
        self._es_doc_type = es_doc_type
        self._limit = query_limit
        self._send_email = send_email
        self._email_recipients = email_recipients
        self._email_template_fp = email_template_fp
        self.log = logger if logger else log

    def get_all(self):
        """
        Get all the test results in json format
        """
        return self._es.search(index=self._es_index, size=self._limit)

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
            for v in ('scylla-server', 'scylla-enterprise-server'):
                k = test_doc['_source']['versions'].get(v)
                if k:
                    return k

        self.log.error('Scylla version is not found for test %s', test_doc['_id'])
        return None

    def render_to_html(self, results, html_file_path=""):
        """
        Render analysis results to html template
        :param results: results dictionary
        :param html_file_path: Boolean, whether to save html file on disk
        :return: html string
        """
        self.log.info("Rendering results to html using '%s' template...", self._email_template_fp)
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True)
        template = env.get_template(self._email_template_fp)
        html = template.render(results)
        if html_file_path:
            with open(html_file_path, "w") as f:
                f.write(html)
            self.log.info("HTML report saved to '%s'.", html_file_path)
        return html

    def send_email(self, subject, content, html=True, files=()):
        if self._send_email and self._email_recipients:
            self.log.debug('Send email to {}'.format(self._email_recipients))
            em = Email(self._conf['email']['server'],
                       self._conf['email']['sender'],
                       self._email_recipients)
            em.send(subject, content, html, files=files)
        else:
            self.log.warning("Won't send email (send_email: %s, recipients: %s)",
                             self._send_email, self._email_recipients)

    def check_regression(self):
        return NotImplementedError("check_regression should be implemented!")


class PerformanceResultsAnalyzer(BaseResultsAnalyzer):
    """
    Get performance test results from elasticsearch DB and analyze it to find a regression
    """

    PARAMS = ['op rate', 'latency mean', 'latency 99th percentile']

    def __init__(self, es_index, es_doc_type, send_email, email_recipients, logger=None):
        super(PerformanceResultsAnalyzer, self).__init__(
            es_index=es_index,
            es_doc_type=es_doc_type,
            send_email=send_email,
            email_recipients=email_recipients,
            email_template_fp="results_performance.html",
            logger=logger
        )

    def _remove_non_stat_keys(self, stats):
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
        if not stats_average or not stats_total:
            self.log.error('Cannot find average/total results for test: {}!'.format(test_doc['_id']))
            return None
        # replace average by total value for op rate
        stats_average['op rate'] = stats_total['op rate']
        return stats_average

    def _test_version(self, test_doc):
        if test_doc['_source'].get('versions'):
            for v in ('scylla-server', 'scylla-enterprise-server'):
                k = test_doc['_source']['versions'].get(v)
                if k:
                    return k

        self.log.error('Scylla version is not found for test %s', test_doc['_id'])
        return None

    def _get_grafana_snapshot(self, test_doc):
        return test_doc['_source']['test_details'].get('grafana_snapshot')

    def _get_setup_details(self, test_doc, is_gce):
        setup_details = {'cluster_backend': test_doc['_source']['setup_details'].get('cluster_backend')}
        if "aws" in setup_details['cluster_backend']:
            setup_details['ami_id_db_scylla'] = test_doc['_source']['setup_details']['ami_id_db_scylla']
        for sp in QueryFilter(test_doc, is_gce).setup_instance_params():
            setup_details.update([(sp.replace('gce_', ''), test_doc['_source']['setup_details'].get(sp))])
        return setup_details

    def _get_best_value(self, key, val1, val2):
        if key == self.PARAMS[0]:  # op rate
            return val1 if val1 > val2 else val2
        return val1 if val2 == 0 or val1 < val2 else val2  # latency

    def _query_filter(self, test_doc, is_gce):
        return QueryFilterScyllaBench(test_doc, is_gce)() if test_doc['_source']['test_details'].get('scylla-bench')\
            else QueryFilterCS(test_doc, is_gce)()

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

    def check_regression(self, test_id, is_gce=False):
        """
        Get test results by id, filter similar results and calculate max values for each version,
        then compare with max in the test version and all the found versions.
        Save the analysis in log and send by email.
        :param test_id: test id created by performance test
        :param is_gce: is gce instance
        :return: True/False
        """
        # get test res
        from sortedcontainers import SortedDict
        doc = self.get_test_by_id(test_id)
        if not doc:
            self.log.error('Cannot find test by id: {}!'.format(test_id))
            return False
        self.log.debug(pp.pformat(doc))

        test_stats = self._test_stats(doc)
        if not test_stats:
            return False

        # filter tests
        query = self._query_filter(doc, is_gce)
        if not query:
            return False
        self.log.debug("Query to ES: %s", query)
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.results.stats_total',
                       'hits.hits._source.results.throughput',
                       'hits.hits._source.versions']
        tests_filtered = self._es.search(index=self._es_index, q=query, filter_path=filter_path, size=self._limit)

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
        #             "op rate": "9b4a0a287",
        #             "latency mean": "9b4a0a287",
        #
        #         }
        #     }
        # }
        # Find best results for each version
        for tr in tests_filtered['hits']['hits']:
            if tr['_id'] == test_id:  # filter the current test
                continue
            if '_source' not in tr:  # non-valid record?
                self.log.error('Skip non-valid test: %s', tr['_id'])
                continue
            version_info = self._test_version(tr)
            version = version_info['version']
            if not version:
                continue
            curr_test_stats = self._test_stats(tr)
            if not curr_test_stats:
                continue
            if version not in group_by_version:
                group_by_version[version] = dict(tests=SortedDict(), stats_best=dict(), best_test_id=dict())
                group_by_version[version]['stats_best'] = {k: 0 for k in self.PARAMS}
                group_by_version[version]['best_test_id'] = {k: version_info["commit_id"] for k in self.PARAMS}
            group_by_version[version]['tests'][version_info['date']] = curr_test_stats
            old_best = group_by_version[version]['stats_best']
            group_by_version[version]['stats_best'] =\
                {k: self._get_best_value(k, curr_test_stats[k], old_best[k])
                 for k in self.PARAMS if k in curr_test_stats and k in old_best}
            # replace best test id if best value changed
            for k in self.PARAMS:
                if k in curr_test_stats and k in old_best and\
                        group_by_version[version]['stats_best'][k] == curr_test_stats[k]:
                            group_by_version[version]['best_test_id'][k] = version_info["commit_id"]

        res_list = list()
        # compare with the best in the test version and all the previous versions
        test_version_info = self._test_version(doc)
        test_version = test_version_info['version']
        for version in group_by_version.keys():
            if version == test_version and not len(group_by_version[test_version]['tests']):
                self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(test_stats,
                               group_by_version[version]['stats_best'],
                               version,
                               group_by_version[version]['best_test_id'])
            res_list.append(cmp_res)
        if not res_list:
            self.log.info('No test results to compare with')
            return False

        # send results by email
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        cassandra_stress = doc['_source']['test_details'].get('cassandra-stress')
        results = dict(test_name=full_test_name,
                       test_start_time=str(test_start_time),
                       test_version=test_version_info,
                       res_list=res_list,
                       setup_details=self._get_setup_details(doc, is_gce),
                       prometheus_stats={stat: doc["_source"]["results"].get(stat, {}) for stat in TestStatsMixin.PROMETHEUS_STATS},
                       prometheus_stats_units=TestStatsMixin.PROMETHEUS_STATS_UNITS,
                       grafana_snapshot=self._get_grafana_snapshot(doc),
                       cs_raw_cmd=cassandra_stress.get('raw_cmd', "") if cassandra_stress else "",
                       job_url=doc['_source']['test_details'].get('job_url', "")
                       )
        self.log.debug('Regression analysis:')
        self.log.debug(pp.pformat(results))

        kibana_url = self._conf.get('kibana_url')
        results.update({'dashboard_master': kibana_url})
        test_name = full_test_name.split('.')[-1]  # Example: longevity_test.py:LongevityTest.test_custom_time
        subject = 'Performance Regression Compare Results - {} - {}'.format(test_name, test_version)
        html = self.render_to_html(results)
        self.send_email(subject, html)

        return True
