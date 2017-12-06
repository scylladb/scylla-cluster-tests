import os
import logging
import math
import yaml
import elasticsearch
from sortedcontainers import SortedDict
import jinja2
import pprint
from send_email import Email

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


class ResultsAnalyzer(object):
    """
    Get performance test results from elasticsearch DB and analyze it to find a regression
    """

    PARAMS = ['op rate', 'latency mean', 'latency 99th percentile']

    def __init__(self, *args, **kwargs):
        self._conf = self._get_conf(os.path.abspath(__file__).replace('.py', '.yaml').rstrip('c'))
        self._url = self._conf.get('es_url')
        self._index = kwargs.get('index', self._conf.get('es_index'))
        self._es = elasticsearch.Elasticsearch([self._url])
        self._limit = 1000

    def _get_conf(self, conf_file):
        with open(conf_file) as cf:
            return yaml.load(cf)

    def _remove_non_stat_keys(self, stats):
        for non_stat_key in ['loader_idx', 'cpu_idx', 'keyspace_idx']:
            if non_stat_key in stats:
                del stats[non_stat_key]
        return stats

    def get_all(self):
        """
        Get all the test results in json format
        """
        return self._es.search(index=self._index, size=self._limit)

    def get_test_by_id(self, test_id):
        """
        Get test results by test id
        :param test_id: test id created by performance test
        :return: test results in json format
        """
        if not self._es.exists(index=self._index, doc_type='_all', id=test_id):
            logger.error('Test results not found: {}'.format(test_id))
            return None
        return self._es.get(index=self._index, doc_type='_all', id=test_id)

    def cmp(self, src, dst, version_src, version_dst, test_type):
        """
        Compare two results
        :param src / dst: results
        :param version_src / version_dst: scylla server version
        :param test_type: test case name
        :return: dictionary with compare calculation results
        """
        cmp_res = dict(version_src=version_src, version_dst=version_dst, test_type=test_type, res=dict())
        for param in self.PARAMS:
            param_key_name = param.replace(' ', '_')
            status = 'OK'
            try:
                delta = src[param] - dst[param]
                change_perc = int(math.fabs(delta) * 100 / dst[param])
                delta_type = '> by' if delta > 0 else '< by'
                if (param.startswith('latency') and delta > 0) or (param == 'op rate' and delta < 0):
                    status = 'Regression'
                cmp_res['res'][param_key_name] = dict(delta_type=delta_type,
                                                      percent='{}%'.format(change_perc),
                                                      comment='({} - {} = {})'.format(src[param], dst[param], delta),
                                                      status=status)
            except TypeError:
                logger.exception('Failed to compare {} results: {} vs {}, test_type {} versions {}, {}'.format(
                    param, src[param], dst[param], test_type, version_src, version_dst))
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
        doc = self.get_test_by_id(test_id)
        if not doc:
            logger.error('Cannot find test by id: {}!'.format(test_id))
            return False
        logger.info(pp.pformat(doc))

        # check if stats exists
        test_stats = self._remove_non_stat_keys(doc['_source']['results']['stats_average'])
        if not test_stats:
            logger.error('Cannot find results for test: {}!'.format(test_id))
            return False

        # filter tests
        test_type = doc['_type']
        try:
            setup_details = ''
            setup_params = ('n_db_nodes', 'n_loaders', 'n_monitor_nodes')
            if is_gce:
                setup_params += ('gce_instance_type_db', 'gce_instance_type_loader', 'gce_instance_type_monitor')
            else:
                setup_params += ('instance_type_db', 'instance_type_loader', 'instance_type_monitor')
            for param in setup_params:
                if setup_details:
                    setup_details += ' AND '
                setup_details += 'setup_details.{}: {}'.format(param, doc['_source']['setup_details'][param])

            test_details = 'test_details.job_name:{}'.format(doc['_source']['test_details']['job_name'].split('/')[0])
            test_details_params = ('cassandra-stress', )
            if test_type.endswith('read') or test_type.endswith('mixed'):
                test_details_params += ('preload-cassandra-stress', )
            cs_params = ('command', 'cl', 'rate threads', 'schema', 'mode', 'pop')
            if test_type.endswith('profiles'):
                cs_params = ('command', 'profile', 'ops', 'rate threads')
            for cs in test_details_params:
                for param in cs_params:
                    if param == 'rate threads':
                        test_details += ' AND test_details.{}.rate\ threads: {}'.format(
                            cs, doc['_source']['test_details'][cs][param])
                    else:
                        param_val = doc['_source']['test_details'][cs][param]
                        if param in ['profile', 'ops']:
                            param_val = "\"{}\"".format(param_val)
                        test_details += ' AND test_details.{}.{}: {}'.format(cs, param, param_val)

            query = '{} AND {}'.format(test_details, setup_details)
        except KeyError:
            logger.exception('Expected parameters for filtering are not found , test {} {}'.format(test_type, test_id))
            return False

        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.versions.scylla-server']

        tests_filtered = self._es.search(index=self._index, doc_type=test_type, q=query, filter_path=filter_path,
                                         size=self._limit)
        if not tests_filtered:
            logger.info('Cannot find tests with the same parameters as {}'.format(test_id))
            return False

        # get max res for all versions of this job
        group_by_version = dict()
        for tr in tests_filtered['hits']['hits']:
            if tr['_id'] == test_id:  # filter the current test
                continue
            version = tr['_source']['versions']['scylla-server']['version']
            version_date = tr['_source']['versions']['scylla-server']['date']
            if 'results' not in tr['_source'] or 'stats_average' not in tr['_source']['results']:
                logger.error('No test results found, test_id: %s', tr['_id'])
                continue
            stats = self._remove_non_stat_keys(tr['_source']['results']['stats_average'])
            if not stats:
                continue
            if version not in group_by_version:
                group_by_version[version] = dict(tests=SortedDict(), stats_max=dict())
                group_by_version[version]['stats_max'] = {k: 0 for k in self.PARAMS}
            group_by_version[version]['tests'][version_date] = stats
            old_max = group_by_version[version]['stats_max']
            group_by_version[version]['stats_max'] =\
                {k: stats[k] if stats[k] > old_max[k] else old_max[k] for k in self.PARAMS if k in stats and k in old_max}

        res_list = list()
        # compare with max in the test version and all the previous versions
        test_version = doc['_source']['versions']['scylla-server']['version']
        for version in group_by_version.keys():
            if version == test_version and not len(group_by_version[test_version]['tests']):
                logger.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(test_stats,
                               group_by_version[version]['stats_max'],
                               test_version, version, test_type)
            res_list.append(cmp_res)
        if not res_list:
            logger.info('No test results to compare with')
            return False
        logger.info('Regression analysis:')
        logger.info(pp.pformat(res_list))
        self.send_email('Performance regression compare results', res_list)
        return True

    @staticmethod
    def render_to_html(res):
        """
        Render analysis results to html template
        :param res: results dictionary
        :return: html string
        """
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True)
        template = env.get_template('results.html')
        html = template.render(dict(res_list=res))
        return html

    def send_email(self, subject, content, html=True):
        if self._conf['email']['send'] and self._conf['email']['recipients']:
            content = self.render_to_html(content)
            em = Email(self._conf['email']['server'],
                             self._conf['email']['sender'],
                             self._conf['email']['recipients'])
            em.send(subject, content, html)
