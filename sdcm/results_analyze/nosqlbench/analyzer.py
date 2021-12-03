import logging
from dataclasses import dataclass, field
from datetime import datetime
from itertools import groupby
from pathlib import Path
from typing import Any, NamedTuple

from sdcm.db_stats import TestStatsMixin
from sdcm.results_analyze import BaseResultsAnalyzer
from sdcm.sct_events import Severity
from sdcm.utils.es_queries import NoSQLBenchQueryFilter

LOGGER = logging.getLogger(__name__)


class ProgressStatus(NamedTuple):
    progress: str = "Progress"
    regression: str = "Regression"


class ComparisonAxis(NamedTuple):
    name: str
    smaller_is_better: bool


class NoSQLBenchAnalyzerArgs(NamedTuple):
    test_id: str
    es_index: str
    es_doc_type: str
    logdir: str | Path
    email_recipients: list[str]
    events: dict[str, Any]
    is_gce: bool
    use_wide_query: bool


@dataclass
class MetricComparator:
    current_test_results: dict
    filtered_test_results: dict
    comparison_axis: list[tuple[str, bool]]
    percent_precision: int
    vs_last: dict[str, Any] = field(default_factory=dict, init=False)
    vs_history: dict[str, Any] = field(default_factory=dict, init=False)

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __post_init__(self):
        run_results = self.filtered_test_results["hits"]["hits"]
        grouped = self._group_by_version(run_results)
        self._extract_metrics_history(run_results=run_results, grouped=grouped)
        self._compare_with_version_history()
        self._compare_with_last_run()

    @staticmethod
    def _group_by_version(data: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        sorted_data = sorted(data, key=lambda item: item["_source"]["versions"]["scylla-server"]["version"])
        groups = []
        group_keys = []

        for key, group in groupby(sorted_data, key=lambda item: item["_source"]["versions"]["scylla-server"]["version"]):
            groups.append(list(group))
            group_keys.append(key)
        return groups

    def _extract_metrics_history(self, run_results: list[list[dict[str, Any]]], grouped: list[list[dict[str, Any]]]):
        for metric, reverse in self.comparison_axis:
            sorted_runs = self._sort_by_metric(data=grouped, metric=metric, reverse=reverse)
            last_run = self._get_previous_run_values(run_results, metric)
            min_max_mean = self._get_run_history_values(sorted_metrics=sorted_runs, reverse=reverse)
            min_val = {k: v["min"] for k, v in min_max_mean.items()}
            max_val = {k: v["max"] for k, v in min_max_mean.items()}
            mean_val = {k: v["mean"] for k, v in min_max_mean.items()}
            self.__setattr__(metric, {"last_run": last_run, "min": min_val, "max": max_val, "mean": mean_val})

    def _compare_with_version_history(self):
        statuses = ProgressStatus()
        current_version = self.current_test_results["_source"]["versions"]["scylla-server"]["version"]
        vs_history_output = {}
        for metric, smaller_is_better in self.comparison_axis:
            best = self[metric]["max"][current_version] if smaller_is_better else self[metric]["min"][current_version]
            comparison_value = float(best["value"])
            version_best = float(self._get_timer_value(self.current_test_results, metric))
            diff_time_completed = self[metric]["max"][current_version]["time_completed"] if smaller_is_better \
                else self[metric]["min"][current_version]["time_completed"]
            version_mean = float(self[metric]["mean"][current_version])
            diff_best = round(comparison_value - version_best, self.percent_precision)
            delta_best_percent = round(diff_best / version_mean * 100, self.percent_precision)

            diff_mean = version_best - self[metric]["mean"][current_version]

            #  we declare progress only of both of the variables have the same boolean value
            if smaller_is_better + (delta_best_percent < 0) != 1:
                status = statuses.progress
            else:
                status = statuses.regression

            vs_history_output.update({metric: {
                "current": comparison_value,
                "version_best": version_best,
                "commit_id_best": best["commit_id"],
                "build_id_best": best["build_id"],
                "date_best": datetime.strptime(best["date"], "%Y%m%d").isoformat(),
                "diff_best": diff_best,
                "delta_best_percent": delta_best_percent,
                "best_run_time_completed": diff_time_completed,
                "diff_mean": diff_mean,
                "status": status
            }})

        self.vs_history = vs_history_output

    def _compare_with_last_run(self):
        vs_last_run_output = {}
        statuses = ProgressStatus()

        for metric, smaller_is_better in self.comparison_axis:
            current = float(self._get_timer_value(self.current_test_results, metric))
            last_run_value = float(self[metric]["last_run"]["value"])
            last_run_time_completed = self[metric]["last_run"]["time_completed"]
            diff = round(float(current) - float(last_run_value), self.percent_precision)
            delta_percent = round(diff / float(last_run_value) * 100, self.percent_precision)

            #  we declare progress only of both of the variables have the same boolean value
            if smaller_is_better + (delta_percent < 0) != 1:
                status = statuses.progress
            else:
                status = statuses.regression

            vs_last_run_output.update({metric: {
                "current": current,
                "last_value": last_run_value,
                "last_run_commit_id": self[metric]["last_run"]["commit_id"],
                "last_run_build_id": self[metric]["last_run"]["build_id"],
                "last_run_commit_date": datetime.strptime(self[metric]["last_run"]["date"], "%Y%m%d").isoformat(),
                "last_run_time_completed": last_run_time_completed,
                "diff": diff,
                "delta_percent": delta_percent,
                "status": status
            }})

        self.vs_last = vs_last_run_output

    @staticmethod
    def _get_timer_value(item: dict, key: str):
        if "results" not in item["_source"]:
            LOGGER.info("Did not find results in ES json:\n%s", item)
            return None
        return item["_source"]["results"]["timers"][key]

    @staticmethod
    def _get_time_completed(run: dict):
        return run['_source']['test_details']['time_completed']

    def _sort_by_metric(self, data: list[list[dict[str, Any]]], metric: str, reverse: bool = False):
        sorted_output = []
        for item in data:
            sorted_output.append(sorted(
                [
                    {
                        "_id": i['_id'],
                        "version": i["_source"]["versions"]["scylla-server"]["version"],
                        "commit_id": i["_source"]["versions"]["scylla-server"]["commit_id"],
                        "build_id": i["_source"]["versions"]["scylla-server"]["build_id"],
                        "date": i["_source"]["versions"]["scylla-server"]["date"],
                        "time_completed": self._get_time_completed(i),
                        "value": float(self._get_timer_value(i, metric))
                    } for i in item], key=lambda x: x["value"], reverse=reverse
            ))
        sorted_output = [item for item in sorted_output if item is not None]

        return sorted_output

    @staticmethod
    def _get_run_history_values(sorted_metrics: list, reverse: bool = False):
        output_dict = {}
        for group in sorted_metrics:
            output_dict.update(
                {
                    group[0]["version"]: {
                        "min": {**group[-1]} if reverse else {**group[0]},
                        "max": {**group[0]} if reverse else {**group[-1]},
                        "mean": sum(g["value"] for g in group) / len(group)
                    }
                }
            )

        return output_dict

    def _get_previous_run_values(self,
                                 data: list[list[dict[str, Any]]], metric: str):
        all_runs = data.copy()
        sorted_runs = sorted(all_runs,
                             key=lambda x: datetime.fromisoformat(x['_source']['test_details']['time_completed']),
                             reverse=True)
        filtered_sorted_runs = [
            {
                "id": run["_id"],
                "version": run["_source"]["versions"]["scylla-server"]["version"],
                "commit_id": run["_source"]["versions"]["scylla-server"]["commit_id"],
                "build_id": run["_source"]["versions"]["scylla-server"]["build_id"],
                "date": run["_source"]["versions"]["scylla-server"]["date"],
                "time_completed": run['_source']['test_details']['time_completed'],
                "value": float(self._get_timer_value(run, metric))
            } for run in sorted_runs
        ]
        most_recent_run = filtered_sorted_runs[1]  # offset of 1, since 0 is the current run

        return most_recent_run


class NoSQLBenchResultsAnalyzer(BaseResultsAnalyzer):
    def __init__(self,
                 analyzer_args: NoSQLBenchAnalyzerArgs):
        self._analyzer_args = analyzer_args
        super().__init__(self._analyzer_args.es_index, self._analyzer_args.es_doc_type,
                         email_template_fp="results_performance_nosqlbench.html")

    def check_regression(self):
        """
        Get test results by id, filter similar results and calculate max values for each version,
        then compare with max in the test version and all the found versions.
        Save the analysis in log and send by email.
        :param test_id: test id created by performance test
        :param is_gce: is gce instance
        :return: True/False
        """
        # pylint: disable=unexpected-keyword-arg,too-many-locals

        # get test res
        doc = self._get_es_test_results()
        LOGGER.info("Retrieved es test results: %s", doc)
        query = self._get_query_filter(doc).build_query()
        filter_path = ['hits.hits._id',
                       'hits.hits._source.test_details.test_name',
                       'hits.hits._source.test_details.time_completed',
                       'hits.hits._source.test_details.job_url',
                       'hits.hits._source.results.timers.main_cycles_servicetime*',
                       'hits.hits._source.results.timers.main_result*',
                       'hits.hits._source.results.timers.main_result_success*',
                       'main_strides_servicetime*',
                       'hits.hits._source.versions']
        filtered_tests = self._es.search(index=self._es_index, q=query,
                                         filter_path=filter_path, size=self._limit,
                                         request_timeout=30)

        comparison_axes = [
            ComparisonAxis(name="main_cycles_servicetime_median", smaller_is_better=True),
            ComparisonAxis(name="main_cycles_servicetime_max", smaller_is_better=True),
            ComparisonAxis(name='main_cycles_servicetime_min', smaller_is_better=True),
            ComparisonAxis(name='main_cycles_servicetime_p95', smaller_is_better=True),
            ComparisonAxis(name='main_cycles_servicetime_p99', smaller_is_better=True),
            ComparisonAxis(name='main_result_mean_rate', smaller_is_better=False)
        ]

        comparator = MetricComparator(current_test_results=doc, filtered_test_results=filtered_tests,
                                      comparison_axis=comparison_axes, percent_precision=2)

        email_data, subject = self._prepare_results_for_email(doc, comparator)

        self.save_email_data_file(subject, email_data,
                                  file_path=Path(self._analyzer_args.logdir).joinpath('email_data.json'))

        return True

    def _get_es_test_results(self):
        try:
            return self.get_test_by_id(self._analyzer_args.test_id)
        except Exception as exc:
            LOGGER.info("Retrieving Elasticsearch test results failed with:\n%s", exc)
            raise exc

    def _get_query_filter(self, test_doc: dict[str, Any], lastyear: bool = False):
        return NoSQLBenchQueryFilter(test_doc=test_doc,
                                     is_gce=self._analyzer_args.is_gce,
                                     use_wide_query=self._analyzer_args.use_wide_query,
                                     lastyear=lastyear)

    def _prepare_results_for_email(self,
                                   doc: dict[str, Any],
                                   comparator: MetricComparator) -> tuple[dict[str, Any], str]:
        res_list = [{"best": comparator.vs_history, "last": comparator.vs_last}]
        LOGGER.info("Comparisons:")
        LOGGER.info(res_list)

        full_test_name = doc["_source"]["test_details"]["test_name"]
        test_start_time = datetime.utcfromtimestamp(float(doc["_source"]["test_details"]["start_time"]))
        test_version = self._test_version(doc)
        dashboard_path = "app/kibana#/dashboard/03414b70-0e89-11e9-a976-2fe0f5890cd0?_g=()"
        last_events, events_summary = self.get_events(event_severity=[
            Severity.CRITICAL.name, Severity.ERROR.name, Severity.DEBUG.name])
        results = {
            "test_name": full_test_name,
            "test_start_time": str(test_start_time),
            "test_version": test_version,
            "res_list": res_list,
            "setup_details": self._get_setup_details(doc, self._analyzer_args.is_gce),
            "prometheus_stats": {stat: doc["_source"]["results"].get(stat, {})
                                 for stat in TestStatsMixin.PROMETHEUS_STATS},
            "prometheus_stats_units": TestStatsMixin.PROMETHEUS_STATS_UNITS,
            "grafana_snapshots": self._get_grafana_snapshot(doc),
            "grafana_screenshots": self._get_grafana_screenshot(doc),
            "job_url": doc["_source"]["test_details"].get("job_url", ""),
            "kibana_url": self.gen_kibana_dashboard_url(dashboard_path),
            "events_summary": events_summary,
            "last_events": last_events,
        }
        test_name = full_test_name.split('.')[-1]  # Example: longevity_test.py:LongevityTest.test_custom_time
        subject = f'NoSQLBench Performance Regression Compare Results ' \
                  f'- {test_name} - {test_version["version"]} - {str(test_start_time)}'

        email_data = {'email_body': results,
                      'attachments': (),
                      'template': self._email_template_fp}

        return email_data, subject
