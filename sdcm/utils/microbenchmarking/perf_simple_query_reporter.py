import os
import json

from sdcm.results_analyze import BaseResultsAnalyzer, collections, TestConfig


def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


class PerfSimpleQueryAnalyzer(BaseResultsAnalyzer):

    collect_last_scylla_date_count = 10

    def __init__(self, es_index):
        super().__init__(es_index, query_limit=1000, logger=None)

    def _get_perf_simple_query_result(self, test_doc):
        if not keys_exists(test_doc, "_source", "results", "perf_simple_query_result"):
            self.log.error('Cannot find the field: perf_simple_query_result for test id: {}!'.format(test_doc['_id']))
            return None
        return test_doc['_source']['results']['perf_simple_query_result']

    def _get_test_details(self, test_doc):
        if not keys_exists(test_doc, "_source", "test_details"):
            self.log.error('Cannot find the field: test_details for test id: {}!'.format(test_doc['_id']))
            return None
        return test_doc["_source"]["test_details"]

    @staticmethod
    def get_sorted_results_as_list(results):
        keys = list(results.keys())
        keys.sort(reverse=True)
        return [results[key] for key in keys]

    def check_regression(self, test_id, mad_deviation_limit=0.02, regression_limit=0.05, is_gce=False, extra_jobs_to_compare=None  # noqa: PLR0914
                         ) -> dict:
        doc = self.get_test_by_id(test_id)
        if not doc:
            self.log.error('Cannot find test by id: {}!'.format(test_id))
            return False

        test_stats = self._get_perf_simple_query_result(doc)
        columns = test_stats['stats'].keys()
        test_details = self._get_test_details(doc)
        if not (test_stats and test_details):
            self.log.debug("Could not find test statistics, regression check is skipped")
            return False

        diff = test_stats['stats']['mad tps'] / test_stats['stats']['median tps']
        is_deviation_within_limits = bool(diff < mad_deviation_limit)
        deviation_diff = round((diff) * 100, 2)

        es_base_path = 'hits.hits'
        es_source_path = es_base_path + '._source'
        filter_path = ['.'.join([es_base_path, '_id']),
                       '.'.join([es_source_path, 'results', 'perf_simple_query_result']),
                       '.'.join([es_source_path, 'setup_details', 'instance_type_db']),
                       '.'.join([es_source_path, 'test_details']),
                       '.'.join([es_source_path, 'versions'])]

        tests_filtered = self._es.search(
            index=self._es_index,

            size=self._limit,
            filter_path=filter_path,
        )
        extra_jobs_to_compare = extra_jobs_to_compare or []
        sorted_results = {}
        for tag_row in tests_filtered['hits']['hits']:
            if keys_exists(tag_row, "_source", "versions", "scylla-server", 'date') and \
                    keys_exists(tag_row, "_source", "results", "perf_simple_query_result") and \
                    keys_exists(tag_row, "_source", "setup_details", "instance_type_db", ):
                if tag_row["_source"]["setup_details"]["instance_type_db"] == doc["_source"]["setup_details"]["instance_type_db"] and \
                        tag_row["_id"] != doc["_id"] and \
                        tag_row["_source"]["test_details"]["job_name"] in (doc["_source"]["test_details"]["job_name"], *extra_jobs_to_compare):
                    sorted_results[int(str(tag_row["_source"]["versions"]["scylla-server"]["date"]) +
                                       str(tag_row["_source"]["test_details"]["start_time"]))] = tag_row["_source"]

        sorted_results = self.get_sorted_results_as_list(sorted_results)

        filtered_by_scylla_date = {}
        for row in sorted_results:
            date = row["versions"]["scylla-server"]['date']
            median_tps = row["results"]["perf_simple_query_result"]["stats"]["median tps"]
            if len(filtered_by_scylla_date.keys()) < self.collect_last_scylla_date_count:
                if date not in filtered_by_scylla_date.keys() or median_tps > \
                        filtered_by_scylla_date[date]["results"]["perf_simple_query_result"]["stats"][
                            "median tps"]:
                    filtered_by_scylla_date[date] = row

            if len(filtered_by_scylla_date.keys()) >= self.collect_last_scylla_date_count:
                break

        def make_table_line_for_render(data):
            table_line = collections.OrderedDict()
            table_line['test_version'] = data['results']['perf_simple_query_result']['versions'][
                'scylla-server']
            stats = data['results']['perf_simple_query_result']['stats']
            for key in columns:
                value = stats.get(key, 'N/A')
                table_line[key] = value
                if value == 'N/A':
                    continue
                if value > 0 and key != "mad tps":
                    diff = test_stats['stats'][key] / value
                    table_line["is_" + key + "_within_limits"] = False
                    if "_per_op" in key:
                        if diff < 1 + regression_limit:
                            table_line["is_" + key + "_within_limits"] = True
                    elif diff > 1 - regression_limit:
                        table_line["is_" + key + "_within_limits"] = True
                    table_line[key + "_diff"] = round((diff - 1) * 100, 2)
                    table_line[key] = round(table_line[key], 2)
            table_line["mad tps"] = round(table_line["mad tps"], 2)
            return table_line

        scylla_date_results_table = []
        for date, result in filtered_by_scylla_date.items():
            scylla_date_results_table.append(make_table_line_for_render(result))
        test_stats_for_render = {'test_version':  test_stats['versions']['scylla-server']}
        test_stats_for_render.update({key: round(val, 2) for key, val in test_stats["stats"].items()})
        subject = (f"{self._get_email_tags(doc, is_gce)} perf_simple_query Microbenchmark - Regression - "
                   f"{test_stats['versions']['scylla-server']['run_date_time']}")
        for_render = {
            "reporter": "PerfSimpleQuery",
            "subject": subject,
            "testrun_id": test_details['test_id'],
            "test_stats": test_stats_for_render,
            "scylla_date_results_table": scylla_date_results_table,
            "job_url": test_details['job_url'],
            "test_version": test_stats['versions']['scylla-server'],
            "collect_last_scylla_date_count": f"last {self.collect_last_scylla_date_count} Scylla builds dates results",
            "deviation_diff": deviation_diff,
            "is_deviation_within_limits": is_deviation_within_limits
        }
        file_path = os.path.join(TestConfig.logdir(), 'email_data.json')
        with open(file_path, 'w', encoding="utf-8") as file:
            json.dump(for_render, file)
        return for_render
