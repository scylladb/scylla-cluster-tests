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
# Copyright (c) 2021 ScyllaDB

import json
import os
import math
import pprint
import logging
import collections
import re

from datetime import datetime, timedelta
from typing import Any
from sortedcontainers import SortedDict

import jinja2

from sdcm.es import ES
from sdcm.test_config import TestConfig
from sdcm.db_stats import TestStatsMixin
from sdcm.send_email import Email, BaseEmailReporter
from sdcm.sct_events import Severity
from sdcm.utils.common import format_timestamp
from sdcm.utils.es_queries import QueryFilter, PerformanceFilterYCSB, PerformanceFilterScyllaBench, \
    PerformanceFilterCS, CDCQueryFilterCS, LatencyWithNemesisQueryFilter
from test_lib.utils import MagicList, get_data_by_path
from .test import TestResultClass


LOGGER = logging.getLogger(__name__)
PP = pprint.PrettyPrinter(indent=2)


class BaseResultsAnalyzer:
    PARAMS = TestStatsMixin.STRESS_STATS

    def __init__(self, es_index, email_recipients=(), email_template_fp="", query_limit=1000, logger=None,
                 events=None):
        self._es = ES()
        self._conf = self._es.conf
        self._es_index = es_index
        self._limit = query_limit
        self._email_recipients = email_recipients
        self._email_template_fp = email_template_fp
        self.log = logger if logger else LOGGER
        self._events = events

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
        if not self._es.exists(index=self._es_index, id=test_id):
            self.log.error('Test results not found: {}'.format(test_id))
            return None
        return self._es.get(index=self._es_index, id=test_id)

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
        if setup_details['cluster_backend'] == "aws":
            setup_details['ami_id_db_scylla'] = test_doc['_source']['setup_details']['ami_id_db_scylla']
            setup_details['region_name'] = test_doc['_source']['setup_details']['region_name']
        for setup_param in QueryFilter(test_doc, is_gce).setup_instance_parameters():
            setup_details.update(
                [(setup_param.replace('gce_', ''), test_doc['_source']['setup_details'].get(setup_param))])
        return setup_details

    @staticmethod
    def _get_email_tags(test_doc, is_gce):
        setup_details = BaseResultsAnalyzer._get_setup_details(test_doc, is_gce)
        return f"[{setup_details['cluster_backend']}][{setup_details['instance_type_db']}]"

    def _test_version(self, test_doc):
        if test_doc['_source'].get('base_target_versions'):
            base_version = test_doc['_source']['base_target_versions'][0]['base_version']
            base_build_id = test_doc['_source']['base_target_versions'][0]['base_build_id']
            target_version = test_doc['_source']['base_target_versions'][0]['target_version']
            target_build_id = test_doc['_source']['base_target_versions'][0]['target_build_id']
            return {'base_version': base_version,
                    'base_build_id': base_build_id,
                    'target_version': target_version,
                    'target_build_id': target_build_id
                    }
        if test_doc['_source'].get('versions'):
            for value in ('scylla-server', 'scylla-enterprise-server'):
                key = test_doc['_source']['versions'].get(value)
                if key:
                    return key
        else:
            self.log.error('Scylla version is not found for test %s', test_doc['_id'])
            return ''

        res = {
            'version': test_doc['_source']['versions'].get('version', ''),
            'date': test_doc['_source']['versions'].get('date', ''),
            'commit_id': test_doc['_source']['versions'].get('commit_id', ''),
            'build_id': test_doc['_source']['versions'].get('build_id', '')
        }
        return res

    def get_events(self, event_severity=None):
        last_events = {}
        events_summary = {}
        if not event_severity:
            event_severity = [event.name for event in Severity]

        if self._events:
            for event in event_severity:
                last_events[event] = self._events.get(event, [])
                events_summary[event] = len(last_events[event])
        return [last_events, events_summary]

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
        loader = jinja2.FileSystemLoader(os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '../report_templates'))
        print(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=[
                                 'jinja2.ext.loopcontrols', 'jinja2.ext.do'])
        env.filters["format_timestamp"] = format_timestamp
        template = env.get_template(email_template_fp)
        html = template.render(results)
        self.log.info("Results has been rendered to html")
        if html_file_path:
            with open(html_file_path, "w", encoding="utf-8") as html_file:
                html_file.write(html)
            self.log.info("HTML report saved to '%s'.", html_file_path)
        return html

    def send_email(self, subject, content, html=True, files=()):
        if self._email_recipients:
            self.log.debug('Send email to {}'.format(self._email_recipients))
            email = Email()
            email.send(subject, content, html=html, recipients=self._email_recipients, files=files)
        else:
            self.log.warning("Won't send email (recipients: %s)", self._email_recipients)

    def save_html_to_file(self, results, file_name, template_file):
        email = BaseEmailReporter()
        report_file = os.path.join(email.logdir, file_name)
        self.log.debug('report_file = %s', report_file)
        email.save_html_to_file(results, report_file, template_file=template_file)
        self.log.debug('HTML successfully saved to local file')
        return report_file

    def gen_kibana_dashboard_url(self, dashboard_path=""):
        return "%s/%s" % (self._conf.get('kibana_url'), dashboard_path)

    def save_email_data_file(self, subject, email_data, file_path='email_data.json'):
        file_path = os.path.join(TestConfig.logdir(), file_path)
        if os.path.exists(file_path):
            try:
                with open(file_path, encoding="utf-8") as file:
                    data = file.read().strip()
                    file_content = json.loads(data or '{}')
            except EnvironmentError as err:
                self.log.error('Failed to read file %s with error %s', file_path, err)
        else:
            file_content = {}
        file_content[subject] = email_data.copy()
        try:
            with open(file_path, 'w', encoding="utf-8") as file:
                json.dump(file_content, file)
        except EnvironmentError as err:
            self.log.error('Failed to write %s to file %s with error %s', file_content, file_path, err)
        else:
            self.log.debug('Successfully wrote %s to file %s', file_content, file_path)

    def _get_best_value(self, key, val1, val2, params=PARAMS):
        if key == params[0]:  # op rate
            return val1 if val1 > val2 else val2
        return val1 if val2 == 0 or val1 < val2 else val2  # latency

    def cmp(self, src, dst, version_dst, best_test_id):
        """
        Compare current test results with the best results
        :param src: current test results
        :param dst: previous best test results
        :param version_dst: scylla server version to compare with
        :param best_test_id: the best results test id(for each parameter)
        :return: dictionary with compare calculation results
        """
        cmp_res = {"version_dst": version_dst, "res": {}}
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
                cmp_res["res"][param_key_name] = {
                    "percent": f"{change_perc}%",
                    "val": src[param],
                    "best_val": dst[param],
                    "best_id": best_id,
                    "status": status,
                }
            except TypeError:
                self.log.exception('Failed to compare {} results: {} vs {}, version {}'.format(
                    param, src[param], dst[param], version_dst))
        return cmp_res


class LatencyDuringOperationsPerformanceAnalyzer(BaseResultsAnalyzer):
    """
    Get latency during operations performance analyzer
    """

    def __init__(self, es_index, email_recipients=(), logger=None, events=None):
        super().__init__(es_index=es_index, email_recipients=email_recipients,
                         email_template_fp="results_latency_during_ops_short.html", logger=logger, events=events)
        self.percentiles = ['percentile_90', 'percentile_99']

    def get_debug_events(self):
        return self.get_events(event_severity=[Severity.DEBUG.name])

    def get_reactor_stall_events(self):
        debug_events, _ = self.get_debug_events()
        events_list = []
        if debug_events:
            events_list = [stall for stall in debug_events[Severity.DEBUG.name] if 'type=REACTOR_STALLED' in stall]
        return events_list

    def get_kernel_callstack_events(self):
        debug_events, _ = self.get_debug_events()
        events_list = []
        if debug_events:
            events_list = [stall for stall in debug_events[Severity.DEBUG.name] if 'type=KERNEL_CALLSTACK' in stall]
        return events_list

    def _get_previous_results(self, test_doc, is_gce=False):
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results',
                       'hits.hits._source.versions',
                       'hits.hits._source.test_details',
                       'hits.hits._source.latency_during_ops']
        query = LatencyWithNemesisQueryFilter(test_doc, is_gce, use_wide_query=True, lastyear=True)()

        LOGGER.debug("ES QUERY: %s", query)
        test_results = self._es.search(
            index=self._es_index,
            q=query,
            filter_path=filter_path,
            size=self._limit)
        if not test_results:
            self.log.warning("No results found for query: %s", query)
            return []
        return test_results["hits"]["hits"]

    @staticmethod
    def _get_nemesis_data(stat):
        return stat.get('cycles', [])

    def _calculate_cycles_average(self, stat):
        nemesis_cycles = self._get_nemesis_data(stat)
        nemesis_average_by_workloads = {}
        operation_time_summary = []
        operation_time_average = 0
        for cycle in nemesis_cycles:
            hdr_cycle_summary = cycle.get('hdr_summary', {})
            for workload in hdr_cycle_summary:
                nemesis_average_by_workloads.setdefault(
                    workload, {perc: [] for perc in self.percentiles})
                for perc in self.percentiles:
                    nemesis_average_by_workloads[workload][perc].append(
                        hdr_cycle_summary[workload].get(perc, 0))
            operation_time_summary.append(cycle.get('duration_in_sec', 0))

        for _, cycles_stats in nemesis_average_by_workloads.items():
            for perc, values in cycles_stats.items():
                cycles_stats[perc] = round(sum(values) / len(values), 2)

        if operation_time_summary and sum(operation_time_summary):
            operation_time_average = sum(operation_time_summary) / len(operation_time_summary)

        stat.update({"hdr_summary_average": nemesis_average_by_workloads,
                     "average_time_operation": f"{timedelta(seconds=int(operation_time_average))}",
                     "average_time_operation_in_sec": int(operation_time_average)
                     })

    def _get_best_per_nemesis_for_each_version(self, test_doc, is_gce):
        try:
            if not test_doc["_source"].get("latency_during_ops"):
                LOGGER.error("Document with id=%s doesn't have 'latency_during_ops' statistics", test_doc['_id'])
                return {}

            results_per_nemesis_by_version = {nemesis: {}
                                              for nemesis in test_doc["_source"]["latency_during_ops"].keys()}
            best_results = {nemesis: {} for nemesis in test_doc["_source"]["latency_during_ops"].keys()}

            def sort_results_by_versions(results: list[Any]):
                """ filter not relevant results and build dict for sorting

                Filter out from search results any document, which doesn't have
                relevant data: version info, statistics for steady state,
                statistics per nemesis at all, or set of nemesis is differ
                from current run
                """
                # expected structure:
                # {'_add_node' : {'5.1.dev': [{latency_during_ops: {....}}, ...],
                #                 '5.0.dev': [{latency_during_ops: {....}}, ...]}}
                for doc in results:
                    full_version_info = self._test_version(doc)
                    latency_during_ops_stats = doc["_source"].get("latency_during_ops")
                    if doc["_id"] == test_doc["_id"] \
                            or not latency_during_ops_stats \
                            or not full_version_info \
                            or not latency_during_ops_stats.get("Steady State") \
                            or not all(nemesis in latency_during_ops_stats for nemesis in results_per_nemesis_by_version):
                        continue
                    scylla_version = full_version_info.get('version')
                    for nemesis in results_per_nemesis_by_version:
                        nemesis_stat = latency_during_ops_stats.get(nemesis)
                        results_per_nemesis_by_version[nemesis].setdefault(scylla_version, [])
                        self._calculate_cycles_average(nemesis_stat)
                        # if calculated average stat for nemesis is empty or 0, don't add it to version results
                        if not nemesis_stat["hdr_summary_average"] \
                           or not nemesis_stat["average_time_operation_in_sec"]:
                            continue
                        nemesis_stat.update({"version": full_version_info})
                        nemesis_stat.update({"Steady State": latency_during_ops_stats["Steady State"]})
                        results_per_nemesis_by_version[nemesis][scylla_version].append(nemesis_stat)

            # choose best result for each nemesis per version
            # by most less Cycles Average,  and Relative_to Steady
            all_results = self._get_previous_results(test_doc, is_gce)
            sort_results_by_versions(all_results)
            for nemesis in results_per_nemesis_by_version:
                for version in results_per_nemesis_by_version[nemesis]:
                    try:
                        best_results[nemesis][version] = sorted(
                            results_per_nemesis_by_version[nemesis][version],
                            key=lambda obj: obj.get("average_time_operation_in_sec"))[0]
                    except IndexError:
                        best_results[nemesis][version] = {}

                best_results[nemesis] = {per_version: best_results[nemesis][per_version]
                                         for per_version in sorted(best_results[nemesis].keys(),
                                                                   key=lambda version: version,
                                                                   reverse=True)}
            return best_results
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Search best results per version failed. Error: %s", exc)
            return {}

    def _compare_current_best_results_average(self, current_result, best_result):
        """compare results between current and best results

        Calculate difference in percentage between current test
        and best per version for each nemesis nemesis and
        update with new keys best_result dict:
        # expected structure:
            {'_add_node' : {'5.1.dev': {....
                                        hdr_summary_diff: {
                                          'READ' : {'percentile_90': 5.11,
                                                    'percentile_99': -12.11}
                                        }
                                        average_time_operation_in_sec: 1000,
                                        ...
                                        }}},
                            '5.0.dev': {....
                                        hdr_summary_diff: {
                                          'READ' : {'percentile_90': 1.11,
                                                    'percentile_99': -2.11}
                                        }
                                        average_time_operation_in_sec: 1000,
                                        ...
                                        }}}

        """
        def _calculate_relative_change_magnitude(current_value, best_value):
            if not best_value:
                return "N/A"
            else:
                return round(((current_value - best_value) / best_value) * 100, 2)
        try:
            for nemesis in current_result:
                if nemesis in ["Steady State", "summary"]:
                    continue
                nemesis_stat = current_result.get(nemesis)
                self._calculate_cycles_average(nemesis_stat)
            for nemesis in best_result:
                if nemesis in ['Steady State', 'summary']:
                    continue
                for _, best in best_result[nemesis].items():
                    for workload in best['hdr_summary_average']:
                        diff = best.setdefault('hdr_summary_diff', {})
                        diff.update({workload: {perc: 0 for perc in self.percentiles}})
                        for perc in self.percentiles:
                            diff[workload][perc] = _calculate_relative_change_magnitude(
                                current_result[nemesis]['hdr_summary_average'][workload][perc],
                                best['hdr_summary_average'][workload][perc])
                    best['average_time_operation_in_sec_diff'] = _calculate_relative_change_magnitude(
                        current_result[nemesis]['average_time_operation_in_sec'],
                        best['average_time_operation_in_sec'])
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Compare results failed: %s", exc)

    def check_regression(self, test_id, data, is_gce=False, node_benchmarks=None, email_subject_postfix=None):  # noqa: PLR0914
        doc = self.get_test_by_id(test_id)
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_name = full_test_name.split('.')[-1]  # Example: longevity_test.LongevityTest.test_custom_time
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        test_version_info = self._test_version(doc)
        if test_version_info:
            if 'base_version' in test_version_info:
                base_version = test_version_info['base_version']
                base_build_id = test_version_info['base_build_id']
                test_version = test_version_info['target_version']
                build_id = test_version_info['target_build_id']
            else:
                base_version = ''
                base_build_id = ''
                test_version = f"{test_version_info['version']}.{test_version_info['date']}.{test_version_info['commit_id']}"
                build_id = test_version_info.get('build_id', '')
        else:
            base_version = ''
            base_build_id = ''
            test_version = ''
            build_id = ''

        last_error_events, error_events_summary = self.get_events(event_severity=[Severity.ERROR.name])
        last_critical_events, critical_events_summary = self.get_events(event_severity=[Severity.CRITICAL.name])
        last_events = {Severity.ERROR.name: last_error_events[Severity.ERROR.name][:100] if Severity.ERROR.name in last_error_events else [],
                       Severity.CRITICAL.name: last_critical_events[Severity.CRITICAL.name][:100] if Severity.CRITICAL.name in last_critical_events else []}
        events_summary = {
            Severity.ERROR.name: error_events_summary[Severity.ERROR.name] if Severity.ERROR.name in error_events_summary else 0,
            Severity.CRITICAL.name: critical_events_summary[Severity.CRITICAL.name] if Severity.CRITICAL.name in critical_events_summary else 0
        }

        reactor_stall_events = self.get_reactor_stall_events()
        reactor_stall_events_summary = {Severity.DEBUG.name: len(reactor_stall_events)}
        kernel_callstack_events = self.get_kernel_callstack_events()
        kernel_callstack_events_summary = {Severity.DEBUG.name: len(kernel_callstack_events)}

        config_files = ' '.join(doc["_source"]["setup_details"]["config_files"])
        search_size = re.search(r'(\d.*(?#t|g)b)', config_files)
        dataset_size = search_size.group() if search_size else 'unknown size'

        subject = (f'{self._get_email_tags(doc, is_gce)} Performance Regression Compare Results '
                   f'({email_subject_postfix} {dataset_size}) -'
                   f' {test_name} - {test_version} - {str(test_start_time)}')
        best_results_per_nemesis = self._get_best_per_nemesis_for_each_version(doc, is_gce)
        self._compare_current_best_results_average(data, best_results_per_nemesis)

        results = dict(
            events_summary=events_summary,
            # limiting to 100 entries of each severity, to avoid oversized emails
            last_events=last_events,
            # limiting to 100 entries, avoiding oversize email
            reactor_stall_events={Severity.DEBUG.name: reactor_stall_events[:100]},
            reactor_stall_events_summary=reactor_stall_events_summary,
            kernel_callstack_events_summary=kernel_callstack_events_summary,
            stats=data,
            test_name=full_test_name,
            test_start_time=str(test_start_time),
            test_id=doc['_source']['test_details'].get('test_id', doc["_id"]),
            base_version=base_version,
            base_build_id=base_build_id,
            test_version=test_version,
            build_id=build_id,
            setup_details=self._get_setup_details(doc, is_gce),
            grafana_screenshots=self._get_grafana_screenshot(doc),
            job_url=doc['_source']['test_details'].get('job_url', ""),
            node_benchmarks=node_benchmarks,
            best_stat_per_version=best_results_per_nemesis,
        )
        attachment_file = self.prepare_attachment_files_for_email(results)

        email_data = {'email_body': results,
                      'attachments': attachment_file,
                      'template': self._email_template_fp}
        self.save_email_data_file(subject, email_data, file_path='email_data.json')

        return True

    def prepare_attachment_files_for_email(self, results):
        self.log.debug("prepare_attachment_files_for_email results: %s", results)
        return [
            self.save_html_to_file(results,
                                   file_name='reactor_stall_events_list.html',
                                   template_file='results_reactor_stall_events_list.html'),
            self.save_html_to_file(results,
                                   file_name='full_email_report.html',
                                   template_file='results_latency_during_ops.html'),
            self.save_html_to_file(results,
                                   file_name='hdr_details_report.html',
                                   template_file='results_latency_during_ops_details_hdr_report.html')
        ]


class SpecifiedStatsPerformanceAnalyzer(BaseResultsAnalyzer):
    """
    Get specified performance test results from elasticsearch DB and analyze it to find a regression
    """

    def __init__(self, es_index, email_recipients=(), logger=None, events=None):
        super().__init__(es_index=es_index, email_recipients=email_recipients,
                         email_template_fp="", logger=logger, events=events)

    def _test_stats(self, test_doc):
        # check if stats exists
        if 'results' not in test_doc['_source']:
            self.log.error('Cannot find the field: results for test id: {}!'.format(test_doc['_id']))
            return None
        return test_doc['_source']['results']

    def check_regression(self, test_id, stats):  # noqa: PLR0914
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

        for stat in stats.keys():  # Add all requested specific-stats to be retrieved from ES DB.
            stat_path = '.'.join([es_source_path, stat])
            filter_path.append(stat_path)

        tests_filtered = self._es.search(
            index=self._es_index,
            size=self._limit,
            filter_path=filter_path,
        )
        self.log.debug("Filtered tests found are: {}".format(tests_filtered))

        if not tests_filtered:
            self.log.info('Cannot find tests with the same parameters as {}'.format(test_id))
            return False
        cur_test_version = None
        tested_params = stats.keys()
        group_by_version = {}
        # repair_runtime result example:
        # { '_id': '20190303-105120-405065',
        #   '_index': 'performanceregressionrowlevelrepairtest',
        #   '_source': { 'coredumps': { },
        #                'errors': { },
        #                'nemesis': { },
        #                'repair_runtime': 11.847206830978394,
        #                'results': { 'latency_read_99': { },
        #                             'latency_write_99': { },
        #                             'repair_runtime': -1,
        #                             'throughput': { }},
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
                group_by_version[version] = {}
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
                cur_test_param_result = stats[param]
                list_param_stats = group_by_version[cur_test_version][param]
                param_avg = sum(list_param_stats) / float(len(list_param_stats))
                deviation_limit = param_avg * allowed_deviation
                self.log.info(
                    "Performance result for: {} is: {}. (average statistics deviation limit is: {}".format(param,
                                                                                                           cur_test_param_result,
                                                                                                           deviation_limit))
                for version, group in group_by_version.items():
                    if param in group:
                        list_param_results = group[param]
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

    def __init__(self, es_index, email_recipients=(), logger=None, events=None):
        super().__init__(es_index=es_index, email_recipients=email_recipients,
                         email_template_fp="results_performance.html", logger=logger, events=events)

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
        if not stats_average or not stats_total or any(stats_average[k] == '' for k in self.PARAMS):
            self.log.error('Cannot find average/total results for test: {}!'.format(test_doc['_id']))
            return None
        # replace average by total value for op rate
        stats_average['op rate'] = stats_total['op rate']
        return stats_average

    def _get_best_value(self, key, val1, val2):
        if key == self.PARAMS[0]:  # op rate
            return val1 if val1 > val2 else val2
        return val1 if val2 == 0 or val1 < val2 else val2  # latency

    @staticmethod
    def _query_filter(test_doc, is_gce,  use_wide_query=False, lastyear=False, extra_jobs_to_compare=None):
        if test_doc['_source']['test_details'].get('scylla-bench'):
            return PerformanceFilterScyllaBench(test_doc, is_gce, use_wide_query, lastyear,
                                                extra_jobs_to_compare=extra_jobs_to_compare)()
        elif test_doc['_source']['test_details'].get('ycsb'):
            return PerformanceFilterYCSB(test_doc, is_gce, use_wide_query, lastyear,
                                         extra_jobs_to_compare=extra_jobs_to_compare)()
        elif "cdc" in test_doc['_source']['test_details'].get('sub_type', ''):
            return CDCQueryFilterCS(test_doc, is_gce, use_wide_query, lastyear,
                                    extra_jobs_to_compare=extra_jobs_to_compare)()
        else:
            return PerformanceFilterCS(test_doc, is_gce, use_wide_query, lastyear,
                                       extra_jobs_to_compare=extra_jobs_to_compare)()

    def cmp(self, src, dst, version_dst, best_test_id):
        """
        Compare current test results with the best results
        :param src: current test results
        :param dst: previous best test results
        :param version_dst: scylla server version to compare with
        :param best_test_id: the best results test id(for each parameter)
        :return: dictionary with compare calculation results
        """
        cmp_res = {"version_dst": version_dst, "res": {}}
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
                cmp_res["res"][param_key_name] = {
                    "percent": f"{change_perc}%",
                    "val": src[param],
                    "best_val": dst[param],
                    "best_id": best_id,
                    "status": status,
                }
            except TypeError:
                self.log.exception('Failed to compare {} results: {} vs {}, version {}'.format(
                    param, src[param], dst[param], version_dst))
        return cmp_res

    def check_regression(self, test_id, is_gce=False, email_subject_postfix=None,  # noqa: PLR0914
                         use_wide_query=False, lastyear=False,
                         node_benchmarks=None, extra_jobs_to_compare=None) -> None:
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
            raise ValueError(f'Cannot find test by id: {test_id}')
        self.log.debug(PP.pformat(doc))

        test_stats = self._test_stats(doc)
        if not test_stats:
            raise ValueError(f'Cannot find test by id: {doc.get("_id", "unknown test")}')

        # filter tests
        query = self._query_filter(doc, is_gce, use_wide_query, lastyear, extra_jobs_to_compare=extra_jobs_to_compare)
        if not query:
            raise ValueError(f'Cannot find query for {doc.get("_id", "unknown test")}')

        self.log.debug("Query to ES: %s", query)
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.results.stats_total',
                       'hits.hits._source.results.throughput',
                       'hits.hits._source.versions']
        tests_filtered = self._es.search(index=self._es_index, q=query, filter_path=filter_path,
                                         size=self._limit, request_timeout=30)

        if not tests_filtered:
            raise ValueError(f'Cannot find tests with the same parameters as {test_id}')

        # get the best res for all versions of this job
        group_by_version = {}
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
            version_info = self._test_version(row)
            if not version_info:
                continue
            version = version_info['version']
            curr_test_stats = self._test_stats(row)
            if not curr_test_stats:
                continue

            formated_version_date = datetime.strptime(version_info['date'], "%Y%m%d").strftime("%Y-%m-%d")
            version_info_data = {"commit": version_info['commit_id'], "date": formated_version_date}

            if version not in group_by_version:
                group_by_version[version] = {"tests": SortedDict(), "stats_best": {}, "best_test_id": {}}
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
        res_list = []
        # compare with the best in the test version and all the previous versions
        test_version_info = self._test_version(doc)
        test_version = test_version_info['version']

        for version, group in group_by_version.items():
            if version == test_version and not group_by_version[test_version]['tests']:
                self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                continue
            cmp_res = self.cmp(test_stats, group['stats_best'], version, group['best_test_id'])
            latest_version_test = group["tests"].peekitem(index=-1)[1]
            latest_res = self.cmp(test_stats,
                                  latest_version_test["test_stats"],
                                  version,
                                  latest_version_test["version"])
            res_list.append({"best": cmp_res, "last": latest_res})
        if not res_list:
            raise ValueError('No test results to compare with')

        # send results by email
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        cassandra_stress = doc['_source']['test_details'].get('cassandra-stress')
        ycsb = doc['_source']['test_details'].get('ycsb')
        dashboard_path = "app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"

        last_events, events_summary = self.get_events(event_severity=[
            Severity.CRITICAL.name, Severity.ERROR.name, Severity.DEBUG.name])

        ebs = doc["_source"]["setup_details"].get("data_device")
        ebs_type = doc["_source"]["setup_details"].get("data_volume_disk_type") if ebs == "attached" else None
        results = {
            "test_name": full_test_name,
            "test_start_time": str(test_start_time),
            "test_version": test_version_info,
            "test_id": doc["_source"]["test_details"].get("test_id", ""),
            "res_list": res_list,
            "setup_details": self._get_setup_details(doc, is_gce),
            "prometheus_stats": {stat: doc["_source"]["results"].get(stat, {})
                                 for stat in TestStatsMixin.PROMETHEUS_STATS},
            "prometheus_stats_units": TestStatsMixin.PROMETHEUS_STATS_UNITS,
            "grafana_screenshots": self._get_grafana_screenshot(doc),
            "cs_raw_cmd": cassandra_stress.get('raw_cmd', "") if cassandra_stress else "",
            "ycsb_raw_cmd": ycsb.get('raw_cmd', "") if ycsb else "",
            "job_url": doc["_source"]["test_details"].get("job_url", ""),
            "kibana_url": self.gen_kibana_dashboard_url(dashboard_path),
            "events_summary": events_summary,
            "last_events": last_events,
            "node_benchmarks": node_benchmarks,
        }
        self.log.debug('Regression analysis:')
        self.log.debug(PP.pformat(results))
        try:
            test_name = full_test_name.split('.', 1)[1]  # Example: longevity_test.LongevityTest.test_custom_time
        except IndexError:
            test_name = full_test_name
        subject = f'{self._get_email_tags(doc, is_gce)} Performance Regression Compare Results - {test_name} - {test_version}'
        if email_subject_postfix:
            subject += f' - {email_subject_postfix}'
        subject += f' - {str(test_start_time)}'

        if ycsb:
            if ycsb_engine := ycsb.get('raw_cmd', "").split():
                if len(ycsb_engine) > 3:
                    index = (ycsb_engine.index("run") if "run" in ycsb_engine else None) or \
                            (ycsb_engine.index("load") if "load" in ycsb_engine else None)
                    ycsb_engine = ycsb_engine[index+1]
                else:
                    ycsb_engine = 'N/A'
            subject = f'YCSB({ycsb_engine}) Performance Regression - {test_name} - {test_version} - {str(test_start_time)}'
        if ebs:
            subject = f'{subject} (ebs volume type {ebs_type})'

        email_data = {'email_body': results,
                      'attachments': (),
                      'template': self._email_template_fp}
        self.save_email_data_file(subject, email_data, file_path='email_data.json')

    def check_regression_with_subtest_baseline(self, test_id, base_test_id, subtest_baseline, is_gce=False, extra_jobs_to_compare=None) -> None:  # noqa: PLR0914
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

        doc = self.get_test_by_id(test_id)
        if not doc:
            raise ValueError(f'Cannot find test by id: {test_id}')

        self.log.debug(PP.pformat(doc))

        test_stats = self._test_stats(doc)
        if not test_stats:
            raise ValueError(f'Cannot find test stats for: {doc.get("_id", "unknown test")}')

        # filter tests
        query = self._query_filter(doc, is_gce, use_wide_query=True, extra_jobs_to_compare=extra_jobs_to_compare)
        self.log.debug(query)
        if not query:
            raise ValueError(f'Cannot find query for {doc.get("_id", "unknown test")}')
        self.log.debug("Query to ES: %s", query)
        filter_path = ['hits.hits._id',
                       'hits.hits._source.results.stats_average',
                       'hits.hits._source.results.stats_total',
                       'hits.hits._source.results.throughput',
                       'hits.hits._source.results',
                       'hits.hits._source.versions',
                       'hits.hits._source.test_details']
        tests_filtered = self._es.search(
            index=self._es_index,
            q=query,
            size=self._limit,
            filter_path=filter_path,
        )

        if not tests_filtered:
            raise ValueError(f'Cannot find tests with the same parameters as {test_id}')

        # get the best res for all versions of this job
        group_by_version_sub_type = SortedDict()
        # Example:
        # group_by_type = {
        #     "version": {
        #           "sub_type": {
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

        current_tests = {}
        grafana_snapshots = {}
        grafana_screenshots = {}
        for row in tests_filtered['hits']['hits']:
            if '_source' not in row:  # non-valid record?
                self.log.error('Skip non-valid test: %s', row['_id'])
                continue
            version_info = self._test_version(row)
            version = version_info['version']
            version_info['date'] = datetime.strptime(version_info['date'], "%Y%m%d").strftime("%Y-%m-%d")
            if not version:
                self.log.error('Skip with wrong version %s', row['_id'])
                continue
            if "results" not in row["_source"]:
                self.log.error('Skip with no results %s', row['_id'])
                continue
            sub_type = row["_source"]['test_details'].get('sub_type')
            curr_test_stats = self._test_stats(row)
            if not curr_test_stats or not sub_type:
                self.log.error('Skip with no test stats %s', row['_id'])
                continue
            if base_test_id in row["_id"] and sub_type not in current_tests:
                current_tests[sub_type] = {}
                current_tests[sub_type]['stats'] = curr_test_stats
                current_tests[sub_type]['version'] = version_info
                current_tests[sub_type]['best_test_id'] = {
                    k: f"#{version_info['commit_id']}, {version_info['date']}" for k in self.PARAMS}
                current_tests[sub_type]['results'] = row['_source']['results']
                grafana_screenshots[sub_type] = self._get_grafana_screenshot(row)

                self.log.info('Added current test results %s. Check next', row['_id'])
                continue
            if version not in group_by_version_sub_type:
                group_by_version_sub_type[version] = {}

            if sub_type not in group_by_version_sub_type[version]:
                group_by_version_sub_type[version][sub_type] = {
                    "tests": SortedDict(),
                    "stats_best": {},
                    "best_test_id": {},
                }
                group_by_version_sub_type[version][sub_type]['stats_best'] = {k: 0 for k in self.PARAMS}
                group_by_version_sub_type[version][sub_type]['best_test_id'] = {
                    k: f"#{version_info['commit_id']}, {version_info['date']}" for k in self.PARAMS}

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
                        k] = f"#{version_info['commit_id']}, {version_info['date']}"

        current_res_list = []
        versions_res_list = []

        test_version_info = self._test_version(doc)
        test_version_info['date'] = datetime.strptime(test_version_info['date'], "%Y%m%d").strftime("%Y-%m-%d")
        test_version = test_version_info['version']
        base_line = current_tests.get(subtest_baseline)
        for sub_type, tests in current_tests.items():
            if not tests or sub_type == subtest_baseline:
                self.log.info('No tests with %s in the current run %s to compare', subtest_baseline, test_version)
                continue
            cmp_res = self.cmp(tests['stats'], base_line['stats'], sub_type, tests['best_test_id'])
            current_res_list.append(cmp_res)

        if not current_res_list:
            raise ValueError('No test results to compare with')

        current_prometheus_stats = SortedDict()
        for sub_type, tests in current_tests.items():
            current_prometheus_stats[sub_type] = {stat: tests["results"].get(stat, {})
                                                  for stat in TestStatsMixin.PROMETHEUS_STATS}

        for version, group in group_by_version_sub_type.items():
            cmp_res = {}
            for sub_type, tests in group.items():
                if not tests['tests']:
                    self.log.info('No previous tests in the current version {} to compare'.format(test_version))
                    continue
                if sub_type not in current_tests:
                    continue
                cmp_res[sub_type] = self.cmp(tests['stats_best'],
                                             current_tests[sub_type]['stats'],
                                             version,
                                             tests['best_test_id'])
            versions_res_list.append({version: cmp_res})

        # send results by email
        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        cassandra_stress = doc['_source']['test_details'].get('cassandra-stress')
        ycsb = doc['_source']['test_details'].get('ycsb')
        dashboard_path = "app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"
        last_events, events_summary = self.get_events()
        results = {
            "test_name": full_test_name,
            "test_id": doc["_source"]["test_details"].get("test_id", ""),
            "test_start_time": str(test_start_time),
            "test_version": test_version_info,
            "base_line": base_line,
            "res_list": current_res_list,
            "ver_res_list": versions_res_list,
            "setup_details": self._get_setup_details(doc, is_gce),
            "prometheus_stats": current_prometheus_stats,
            "prometheus_stats_units": TestStatsMixin.PROMETHEUS_STATS_UNITS,
            "grafana_snapshots": grafana_snapshots,
            "grafana_screenshots": grafana_screenshots,
            "cs_raw_cmd": cassandra_stress.get("raw_cmd", "") if cassandra_stress else "",
            "ycsb_raw_cmd": ycsb.get("raw_cmd", "") if ycsb else "",
            "job_url": doc["_source"]["test_details"].get("job_url", ""),
            "kibana_url": self.gen_kibana_dashboard_url(dashboard_path),
            "baseline_type": subtest_baseline,
            "events_summary": events_summary,
            "last_events": last_events,
        }
        self.log.debug('Regression analysis:')
        self.log.debug(PP.pformat(results))
        test_name = full_test_name.split('.', 1)[1]  # Example: longevity_test.LongevityTest.test_custom_time
        if ycsb:
            subject = f'{self._get_email_tags(doc, is_gce)} (Alternator) Performance Regression - {test_name} - {test_version} - ' \
                      f'{str(test_start_time)}'
        else:
            subject = f'{self._get_email_tags(doc, is_gce)} Performance Regression Compare Results - {test_name}' \
                      f' - {test_version} - {str(test_start_time)}'

        template = 'results_performance_baseline.html'
        email_data = {'email_body': results,
                      'attachments': (),
                      'template': template}
        self.save_email_data_file(subject, email_data, file_path='email_data.json')

    def get_test_instance_by_id(self, test_id):
        rp_main_test = TestResultClass.get_by_test_id(test_id, self._es_index)
        if rp_main_test:
            rp_main_test = rp_main_test[0]
        if not rp_main_test or not rp_main_test.is_valid():
            self.log.error('Cannot find main_test by id: {}!'.format(test_id))
            return None
        return rp_main_test

    @staticmethod
    def _add_remarks_to_test(test, remarks, tests_info):
        if isinstance(remarks, str):
            remarks = [remarks]
        if test not in tests_info:
            tests_info[test] = {'remarks': remarks}
        elif 'remarks' not in tests_info[test]:
            tests_info[test]['remarks'] = remarks
        else:
            tests_info[test]['remarks'].extend(remarks)

    @staticmethod
    def _add_best_for_info(test, subtest, metric_path, tests_info):
        subtest_name = subtest.subtest_name
        if test not in tests_info:
            tests_info[test] = {'best_for': {subtest_name: {metric_path: True}}}
        elif 'best_for' not in tests_info[test]:
            tests_info[test]['best_for'] = {subtest_name: {metric_path: True}}
        elif subtest_name not in tests_info[test]['best_for']:
            tests_info[test]['best_for'][subtest_name] = {metric_path: True}
        else:
            tests_info[test]['best_for'][subtest_name][metric_path] = True

    def _mark_best_tests(self, prior_subtests, metrics, tests_info, main_test_id):
        main_tests_by_id = MagicList(tests_info.keys()).group_by('test_id')
        for _, _prior_tests in prior_subtests.items():
            prior_tests = MagicList(
                [prior_test for prior_test in _prior_tests if prior_test.main_test_id != main_test_id])
            if not prior_tests:
                continue
            for metric_path in metrics:
                # Getting main test of the subtest that showed best score in this metric
                sorted_subtests = prior_tests.sort_by(f'{metric_path}.betterness')
                if not sorted_subtests:
                    continue
                best_subtest = sorted_subtests[-1]
                # Find subtests from any other tests
                self._add_best_for_info(
                    main_tests_by_id[best_subtest.main_test_id][0],
                    best_subtest,
                    metric_path,
                    tests_info
                )

    @staticmethod
    def _clean_up_not_marked_runs(prior_subtests, tests_info):
        main_tests_by_id = MagicList(tests_info.keys()).group_by('test_id')
        for _, prior_tests in prior_subtests.items():
            for prior_test in [prior_test for prior_test in prior_tests
                               if not main_tests_by_id.get(prior_test.main_test_id, False)]:
                prior_tests.remove(prior_test)

    def _find_versions_to_compare_with(self, test, prior_main_tests, tests_info):
        current_test_version_int = test.software.scylla_server_any.version.as_int
        current_test_version_major_int = test.software.scylla_server_any.version.major_as_int

        grouped_by_version = prior_main_tests.group_by(
            data_path='software.scylla_server_any.version.major_as_int',
            sort_keys=-1,
            group_values={
                'data_path': 'software.scylla_server_any.version.as_int',
                'sort_keys': -1
            }
        )

        for version_major_int, prior_tests_grouped in grouped_by_version.items():
            if version_major_int == current_test_version_major_int:
                for version_int, prior_tests_grouped2 in prior_tests_grouped.items():
                    if version_int == current_test_version_int:
                        if prior_tests_grouped2:
                            self._add_remarks_to_test(prior_tests_grouped2[0], 'Latest<br>(same version)', tests_info)
                    elif version_int < current_test_version_int:
                        if prior_tests_grouped2:
                            self._add_remarks_to_test(prior_tests_grouped2[0], [], tests_info)
            elif version_major_int != current_test_version_major_int:
                limit = 2
                for version_int, prior_tests_grouped2 in prior_tests_grouped.items():
                    if limit < 0:
                        break
                    if prior_tests_grouped2:
                        self._add_remarks_to_test(prior_tests_grouped2[0], [], tests_info)
                        limit -= 1

    @staticmethod
    def _get_prior_tests_for_subtests(subtests: list):
        output = collections.OrderedDict()
        for subtest in subtests:
            prior_tests = MagicList(
                [prior_sub_test for prior_sub_test in subtest.get_prior_tests()
                 if prior_sub_test.metrics and prior_sub_test.metrics.is_valid()
                 ])
            output[subtest] = prior_tests
        return output

    @staticmethod
    def _cleanup_not_complete_main_tests(prior_main_tests: list, prior_subtests: dict, expected_subtests_count):
        is_test_complete = {}
        for subtest, prior_tests in prior_subtests.items():
            for prior_test_id, _ in prior_tests.group_by('main_test_id').items():
                if prior_test_id not in is_test_complete:
                    is_test_complete[prior_test_id] = [subtest.subtest_name]
                else:
                    is_test_complete[prior_test_id].append(subtest.subtest_name)

        for prior_test_id in list(is_test_complete.keys()):
            is_test_complete[prior_test_id] = len(is_test_complete[prior_test_id]) >= expected_subtests_count
        to_delete = []
        for num, prior_main_test in enumerate(prior_main_tests):
            if not is_test_complete.get(prior_main_test.test_id, False):
                to_delete.append(num)
        for num in sorted(to_delete, reverse=True):
            prior_main_tests.pop(num)

        for _, prior_tests in prior_subtests.items():
            to_delete = []
            for num, prior_subtest in enumerate(prior_tests):
                if not is_test_complete.get(prior_subtest.main_test_id, False):
                    to_delete.append(num)
            if to_delete:
                for num in sorted(to_delete, reverse=True):
                    prior_tests.pop(num)

    def check_regression_multi_baseline(  # noqa: PLR0912, PLR0914, PLR0915
            self,
            test_id,
            subtests_info: list = None,
            metrics: list = None,
            subject: str = None,
    ) -> None:
        """
        Build regression report for subtests.
        test_id: Main test id
        subtests_info: List of subtest information.
        metrics: List of metrics paths,
        email_subject: email subject
        Returns True if report was generated ans issued, otherwise - False

        subtest_info example: [{
            'name': 'INSERT IF NOT EXISTS',
            'subtest_name': 'lwt-insert-not-exists',
            'baseline': 'lwt-insert-std'
        }, {
            'name': 'INSERT',
            'subtest_name': 'lwt-insert-std',
        }]
        'subtest_name' - should be same as you provide to sub_type argument of test.create_test_stats when creating stats for the subcase
        'name' - Textual representation for reports
        'baseline' - A subtest_name of baseline test, set it to None or do not add it of subtest does not have baseline.

        Subtest that has baseline will be compared to baseline subtest of the same test and result will be showed next to subtest value.

        metrics example: ['metrics.cs_metrics.latency_mean','metrics.cs_metrics.throughput']

        Metric path is path with starting point of TestDataClass that points to metric value.
        Look at sdcm/utils/es_test.py:TestDataClass for more details
        """
        if metrics is None:
            metrics = [
                'metrics.cs_metrics.latency_mean',
                'metrics.cs_metrics.throughput'
            ]
        rp_main_test = self.get_test_instance_by_id(test_id)

        if not rp_main_test:
            raise ValueError(f'Cannot find test with id: {test_id}!')

        if subject is None:
            subject = 'Performance Regression Compare Results - {test.test_name} - ' \
                '{test.software.scylla_server_any.version.as_string}'.format(test=rp_main_test)
        else:
            subject = subject.format(test=rp_main_test)

        rp_main_tests_to_compare = collections.OrderedDict()
        rp_metric_info = {}

        self._add_remarks_to_test(rp_main_test, 'Current', rp_main_tests_to_compare)

        # Get prior main tests and clean it up from tests that has bad start_time or
        # ran on scylla version newer than this test
        prior_main_tests = MagicList([
            prior_test for prior_test in rp_main_test.get_prior_tests() if
            prior_test.is_valid() and prior_test.start_time and prior_test.start_time.is_valid() and
            prior_test.test_id != rp_main_test.test_id
        ])

        if not prior_main_tests:
            raise ValueError(f'Cannot find prior runs for test with id: {test_id}')

        # Get all subtests of the current main test and sort them by subtest name
        rp_subtests_of_current_test = MagicList([
            subtest for subtest in rp_main_test.get_subtests() if
            subtest.is_valid() and subtest.complete_time and subtest.complete_time.is_valid()
        ]).sort_by('subtest_name')

        if not rp_subtests_of_current_test:
            raise ValueError(f'Cannot find subtests for test id: {test_id}')

        if not subtests_info:
            subtests_info = [
                {
                    'name': test.subtest_name,
                    'subtest_name': test.subtest_name,
                } for test in rp_subtests_of_current_test]
        else:
            new_subtests_info = []
            for subtest_info in subtests_info:
                for subtest in rp_subtests_of_current_test:
                    if subtest_info['subtest_name'] == subtest.subtest_name:
                        new_subtests_info.append(subtest_info)
                        break
            subtests_info = new_subtests_info
            del new_subtests_info
        rp_subtests_info = {subtest_info['subtest_name']: subtest_info for subtest_info in subtests_info}
        rp_subtests_of_current_test = MagicList(
            [test for test in rp_subtests_of_current_test if test.subtest_name in rp_subtests_info]
        )

        prior_subtests = self._get_prior_tests_for_subtests(rp_subtests_of_current_test)
        self._cleanup_not_complete_main_tests(prior_main_tests, prior_subtests, len(rp_subtests_of_current_test) // 2)
        self._find_versions_to_compare_with(rp_main_test, prior_main_tests, rp_main_tests_to_compare)
        self._clean_up_not_marked_runs(prior_subtests, rp_main_tests_to_compare)
        self._mark_best_tests(prior_subtests, metrics, rp_main_tests_to_compare, rp_main_test.test_id)

        for metric_path in metrics:
            rp_metric_info[metric_path] = rp_main_test.get_metric_class(metric_path)

        rp_metrics_table = collections.OrderedDict()
        prior_tests_by_main_test_id = {subtest.subtest_name: prior_tests.group_by(
            'main_test_id') for subtest, prior_tests in prior_subtests.items()}

        subtests_groups = {}

        for subtest_info in rp_subtests_info.values():
            groups = subtest_info.get('groups', [''])
            for group in groups:
                if group not in subtests_groups:
                    subtests_groups[group] = MagicList()
                subtests_groups[group].append(subtest_info)

        tmp_subtest_by_name = rp_subtests_of_current_test.group_by('subtest_name')

        for group_name, group_substest_infos in subtests_groups.items():
            rp_metrics_table[group_name] = rp_metrics_table_l1 = collections.OrderedDict()
            grouped_subtests = []
            subtest_info_grouped_by_baseline = group_substest_infos.group_by('baseline', '', sort_keys=1)
            no_baselines = subtest_info_grouped_by_baseline.get('', None)
            if no_baselines is None:
                no_baselines = MagicList()
            else:
                no_baselines = subtest_info_grouped_by_baseline.pop('')
            for baseline_subtest_name, subtest_infos in subtest_info_grouped_by_baseline.items():
                no_baselines.remove_where('subtest_name', baseline_subtest_name)
                if baseline_subtest_name in tmp_subtest_by_name and tmp_subtest_by_name[baseline_subtest_name]:
                    grouped_subtests.append(tmp_subtest_by_name[baseline_subtest_name][0])
                for subtest_info in MagicList(subtest_infos).sort_by('name'):
                    if subtest_info['subtest_name'] in tmp_subtest_by_name \
                            and tmp_subtest_by_name[subtest_info['subtest_name']]:
                        grouped_subtests.append(tmp_subtest_by_name[subtest_info['subtest_name']][0])
            for subtest_info in no_baselines:
                if subtest_info['subtest_name'] in tmp_subtest_by_name \
                        and tmp_subtest_by_name[subtest_info['subtest_name']]:
                    grouped_subtests.append(tmp_subtest_by_name[subtest_info['subtest_name']][0])
            del subtest_info_grouped_by_baseline

            for subtest in grouped_subtests:
                prior_tests = prior_subtests.get(subtest)
                baseline_subtest_name = rp_subtests_info.get(subtest.subtest_name, {}).get('baseline', None)
                baseline_prior_tests = None
                if baseline_subtest_name:
                    baseline_prior_tests = prior_tests_by_main_test_id.get(baseline_subtest_name, None)

                grouped_by_main_test_id = prior_tests.group_by('main_test_id')
                rp_metrics_table_l1[subtest.subtest_name] = rp_metrics_table_l2 = {
                    'total_metrics': 0,
                    'data': collections.OrderedDict()
                }
                for metric_path in metrics:
                    rp_metrics_table_l2['data'][metric_path] = rp_metrics_table_l3 = {
                        'relative': collections.OrderedDict(),
                        'absolute': collections.OrderedDict()
                    }
                    metric_bucket_main = rp_metrics_table_l3['absolute']
                    metric_bucket_baseline = rp_metrics_table_l3['relative']
                    number_of_metrics = 0
                    if not baseline_prior_tests:
                        del rp_metrics_table_l3['relative']
                    else:
                        holds_any_value = False
                        for main_test, _ in rp_main_tests_to_compare.items():
                            prior_subtest = grouped_by_main_test_id.get(main_test.test_id, [])
                            if not prior_subtest:
                                metric_bucket_baseline[main_test] = None
                                continue
                            baseline_subtest = baseline_prior_tests.get(main_test.test_id, [])
                            if not baseline_subtest:
                                metric_bucket_baseline[main_test] = None
                                continue
                            baseline_metric = get_data_by_path(baseline_subtest[0], metric_path)
                            metric_instance = get_data_by_path(prior_subtest[0], metric_path)
                            if baseline_metric is None or metric_instance is None:
                                metric_bucket_baseline[main_test] = None
                                continue
                            metric_bucket_baseline[main_test] = baseline_metric.rate(
                                metric_instance,
                                name=f'{metric_instance.name}<br>baseline diff'
                            )
                            holds_any_value = True
                        if not holds_any_value:
                            del rp_metrics_table_l3['relative']
                        else:
                            number_of_metrics += 1

                    holds_any_value = False
                    for main_test, _ in rp_main_tests_to_compare.items():
                        prior_subtest = grouped_by_main_test_id.get(main_test.test_id, [])
                        if not prior_subtest:
                            metric_bucket_main[main_test] = None
                            continue
                        metric_instance = get_data_by_path(prior_subtest[0], metric_path, None)
                        if metric_instance is None:
                            metric_bucket_main[main_test] = None
                            continue
                        metric_bucket_main[main_test] = metric_instance
                        holds_any_value = True
                    if not holds_any_value:
                        del rp_metrics_table_l2['data'][metric_path]
                        number_of_metrics = 0
                    else:
                        number_of_metrics += 1
                    rp_metrics_table_l2['total_metrics'] += number_of_metrics

        last_events, events_summary = self.get_events()
        results = {
            "test_id": test_id,
            "current_main_test": rp_main_test,
            "results": rp_metrics_table,
            "metric_info": rp_metric_info,
            "tests_info": rp_main_tests_to_compare,
            "subtests": rp_subtests_of_current_test,
            "subtests_info": rp_subtests_info,
            "events_summary": events_summary,
            "last_events": last_events,
        }
        if len(rp_metric_info) == 1:
            template = 'results_performance_multi_baseline_single_metric.html'
        else:
            template = 'results_performance_multi_baseline.html'

        email_data = {'email_body': results,
                      'attachments': (),
                      'template': template}
        self.save_email_data_file(subject, email_data, file_path='email_data.json')


class PredefinedStepsTestPerformanceAnalyzer(LatencyDuringOperationsPerformanceAnalyzer):
    """
    Performance Analyzer for results with throughput and latency of gradual payload increase
    """

    def __init__(self, es_index, email_recipients=(), logger=None, events=None):
        super().__init__(es_index=es_index, email_recipients=email_recipients,
                         logger=logger, events=events)
        self._email_template_fp = "results_performance_predefined_steps.html"
        self.percentiles = ['percentile_95', 'percentile_99']

    def _test_stats(self, test_doc):
        # check if stats exists
        if 'perf_gradual_stats' not in test_doc['_source']:
            self.log.error('Cannot find the field: results for test id: {}!'.format(test_doc['_id']))
            return None
        test_stats = {}
        for step, stats in test_doc['_source']['perf_gradual_stats'].items():
            test_stats[step] = {'sub_type': stats['sub_type']}
            for param, agg in self.PARAMS.items():
                if param in stats:
                    current_step_result = stats[param]
                    if isinstance(stats[param], dict) and agg in stats[param]:
                        current_step_result = stats[param][agg]
                    test_stats[step].update({param: current_step_result})
        return test_stats

    @staticmethod
    def _query_filter(test_doc, is_gce,  use_wide_query=False, lastyear=False, extra_jobs_to_compare=None):
        if test_doc['_source']['test_details'].get('scylla-bench'):
            return PerformanceFilterScyllaBench(test_doc, is_gce, use_wide_query, lastyear,
                                                extra_jobs_to_compare=extra_jobs_to_compare)()
        elif test_doc['_source']['test_details'].get('ycsb'):
            return PerformanceFilterYCSB(test_doc, is_gce, use_wide_query, lastyear,
                                         extra_jobs_to_compare=extra_jobs_to_compare)()
        elif "cdc" in test_doc['_source']['test_details'].get('sub_type', ''):
            return CDCQueryFilterCS(test_doc, is_gce, use_wide_query, lastyear,
                                    extra_jobs_to_compare=extra_jobs_to_compare)()
        else:
            return PerformanceFilterCS(test_doc, is_gce, use_wide_query, lastyear,
                                       extra_jobs_to_compare=extra_jobs_to_compare)()

    @staticmethod
    def _get_nemesis_data(stat):
        return stat.get('run_step', []).get('cycles', [])

    def prepare_attachment_files_for_email(self, results):
        return [
            self.save_html_to_file(results,
                                   file_name='reactor_stall_events_list.html',
                                   template_file='results_reactor_stall_events_list.html'),
            self.save_html_to_file(results,
                                   file_name='full_email_report.html',
                                   template_file='results_performance_predefined_steps.html'),
        ]


class SearchBestThroughputConfigPerformanceAnalyzer(BaseResultsAnalyzer):
    """
    Get latency during operations performance analyzer
    """

    def __init__(self, es_index, email_recipients=(), logger=None, events=None):
        super().__init__(es_index=es_index, email_recipients=email_recipients,
                         email_template_fp="results_search_best_throughput_config.html", logger=logger, events=events)

    def check_regression(self, test_name, setup_details, test_results) -> None:
        subject = f"Performance Regression Best throughput with configuation - {test_name} - {setup_details['start_time']}"
        results = {
            "test_id": setup_details.get("test_id", ""),
            "test_name": test_name,
            "setup_details": setup_details,
            "test_results": test_results
        }
        email_data = {'email_body': results,

                      'template': self._email_template_fp}
        self.save_email_data_file(subject, email_data, file_path='email_data.json')
