import os
import logging
import math
import yaml
import elasticsearch
from sortedcontainers import SortedDict
import jinja2
import smtplib
import email.message as email_message
import pprint

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


class ResultsAnalyzer(object):

    PARAMS = ['op rate', 'latency mean', 'latency 99th percentile']

    def __init__(self, *args, **kwargs):
        self._conf = self._get_conf(os.path.abspath(__file__).replace('.py', '.yaml'))
        self._url = self._conf.get('es_url')
        self._index = self._conf.get('es_index')
        self._es = elasticsearch.Elasticsearch([self._url])
        self._limit = 1000

    def _get_conf(self, conf_file):
        with open(conf_file) as cf:
            return yaml.load(cf)

    def get_all(self):
        return self._es.search(index=self._index, size=self._limit)

    def get_test_by_id(self, test_id):
        if not self._es.exists(index=self._index, doc_type='_all', id=test_id):
            logger.error('Test results not found: {}'.format(test_id))
            return None
        return self._es.get(index=self._index, doc_type='_all', id=test_id)

    def cmp(self, src, dst, version_src, version_dst, test_type):
        cmp_res = dict(version_src=version_src, version_dst=version_dst, test_type=test_type, res=dict())
        for param in self.PARAMS:
            param_key_name = param.replace(' ', '_')
            text = 'OK'
            try:
                delta = src[param] - dst[param]
                change_perc = int(math.fabs(delta) * 100 / dst[param])
                delta_type = '>' if delta > 0 else '<'
                if (param.startswith('latency') and delta > 0) or (param == 'op rate' and delta < 0):
                    text = 'Regression'
                cmp_res['res'][param_key_name] = '{} by {}% ({} - {} = {}) {}'.format(
                    delta_type, change_perc, src[param], dst[param], delta, text)
            except TypeError:
                logger.exception('Failed to compare {} results: {} vs {}, test_type {} versions {}, {}'.format(
                    param, src[param], dst[param], test_type, version_src, version_dst))
        return cmp_res

    def check_regression(self, test_id):
        # get test res
        doc = self.get_test_by_id(test_id)
        if not doc:
            logger.error('Cannot find test by id: {}!'.format(test_id))
            return False
        logger.info(pp.pformat(doc))

        # filter tests
        test_type = doc['_type']
        try:
            setup_details = ''
            for param in ('instance_type_db', 'instance_type_loader', 'instance_type_monitor',
                          'n_db_nodes', 'n_loaders', 'n_monitor_nodes'):
                if setup_details:
                    setup_details += ' AND '
                setup_details += 'setup_details.{}: {}'.format(param, doc['_source']['setup_details'][param])

            test_details = 'test_details.job_name:{}'.format(doc['_source']['test_details']['job_name'].split('/')[0])
            test_details_params = ('cassandra-stress', )
            if not test_type.endswith('write'):
                test_details_params += ('preload0-cassandra-stress', )
            for cs in test_details_params:
                for param in ('command', 'cl', 'rate threads', 'schema', 'mode', 'pop'):
                    if param == 'rate threads':
                        test_details += ' AND test_details.{}.rate\ threads: {}'.format(
                            cs, doc['_source']['test_details'][cs][param])
                    else:
                        test_details += ' AND test_details.{}.{}: {}'.format(
                            cs, param, doc['_source']['test_details'][cs][param])

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
            stats = tr['_source']['results']['stats_average']
            if version not in group_by_version:
                group_by_version[version] = dict(tests=SortedDict(), stats_max=dict())
                group_by_version[version]['stats_max'] = {k: 0 for k in self.PARAMS}
            group_by_version[version]['tests'][version_date] = stats
            old_max = group_by_version[version]['stats_max']
            group_by_version[version]['stats_max'] =\
                {k: stats[k] if stats[k] > old_max[k] else old_max[k] for k in self.PARAMS}

        res_list = list()
        # compare with max in the test version and all the previous versions
        test_version = doc['_source']['versions']['scylla-server']['version']
        for version in group_by_version.keys():
            if version == test_version and not len(group_by_version[test_version]['tests']):
                logger.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(doc['_source']['results']['stats_average'],
                               group_by_version[version]['stats_max'],
                               test_version, version, test_type)
            res_list.append(cmp_res)
        logger.info('Regression analysis:')
        logger.info(pp.pformat(res_list))
        self.send_email('Performance regression compare results', res_list)
        return True

    @staticmethod
    def render_to_html(res):
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True)
        template = env.get_template('results.html')
        html = template.render(dict(res_list=res))
        return html

    def send_email(self, subject, content, html=True):
        if self._conf['email']['send'] and self._conf['email']['recipients']:
            try:
                msg = email_message.Message()
                msg['subject'] = subject
                msg['from'] = self._conf['email']['sender']
                msg['to'] = ','.join(self._conf['email']['recipients'])
                if html:
                    content = self.render_to_html(content)
                    msg.add_header('Content-Type', 'text/html')
                msg.set_payload(content)

                ms = smtplib.SMTP(self._conf['email']['server'])
                ms.sendmail(self._conf['email']['sender'], self._conf['email']['recipients'],
                            msg.as_string())
                ms.quit()
            except Exception:
                logger.exception('Failed sending email')
