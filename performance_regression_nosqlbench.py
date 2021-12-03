import logging
from datetime import datetime
from pathlib import Path

from performance_regression_test import PerformanceRegressionTest
from sdcm.results_analyze.nosqlbench.analyzer import NoSQLBenchResultsAnalyzer, NoSQLBenchAnalyzerArgs
from sdcm.results_analyze.nosqlbench.report_builder import NoSQLBenchReportBuilder
from sdcm.sct_events.file_logger import get_events_grouped_by_category
from sdcm.sct_events.system import ElasticsearchEvent

LOGGER = logging.getLogger(__name__)


class PerformanceRegressionNosqlBenchTest(PerformanceRegressionTest):
    #  pylint: disable=useless-super-delegation
    def __init__(self, *args):
        super().__init__(*args)

    def test_nosqlbench_perf(self):
        """
        Run a performance workload with NoSQLBench. The specifics of the
        workload should be defined in the respective test case yaml file.
        """
        stress_cmd = self.params.get("stress_cmd")
        self.create_test_stats(sub_type="mixed", doc_id_with_timestamp=True)
        self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        # results = self.get_stress_results(queue=stress_queue, calculate_stats=False)
        # LOGGER.info("Raw nosqlbench run result: %s", results)
        self._update_test_details(include_setup_details=True)
        report_builder = self._get_report_builder()
        self._display_results(report_builder)
        report_builder.build_all_reports()
        self._update_stats_with_nosqlbench_report(report_builder)
        report_builder.save_abridged_report_to_file()
        report_builder.print_raw_report_file()
        self.get_prometheus_stats()
        self._check_regression()

    def create(self) -> None:
        LOGGER.info("Overwritten create method called for stats!")
        body = self._stats.copy()
        trimmed_setup_details = {
            "ami_id_db_scylla": body["setup_details"]["ami_id_db_scylla"],
            "append_scylla_args": body["setup_details"]["append_scylla_args"],
            "cluster_backend": body["setup_details"]["cluster_backend"],
            "instance_type_db": body["setup_details"]["instance_type_db"],
            "instance_type_loader": body["setup_details"]["instance_type_loader"],
            "instance_type_monitor": body["setup_details"]["instance_type_monitor"],
            "region_name": body["setup_details"]["region_name"],
        }
        body["setup_details"] = trimmed_setup_details

        if not self.elasticsearch:
            LOGGER.error("Failed to create test stats: ES connection is not created (doc_id=%s)", self._test_id)
            return
        try:
            self.elasticsearch.create_doc(
                index=self._test_index,
                doc_type=self._es_doc_type,
                doc_id=self._test_id,
                body=body,
            )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Failed to create test stats (doc_id=%s)", self._test_id)
            ElasticsearchEvent(doc_id=self._test_id, error=str(exc)).publish()

    def _check_regression(self):
        analyzer_args = NoSQLBenchAnalyzerArgs(
            test_id=self._test_id,
            es_index=self._test_index,
            logdir=self.logdir,
            es_doc_type=self._es_doc_type,
            email_recipients=self.params.get('email_recipients'),
            events=get_events_grouped_by_category(
                _registry=self.events_processes_registry,
                limit=self.params.get('events_limit_in_email')),
            is_gce=bool(self.params.get('cluster_backend') == 'gce'),
            use_wide_query=False
        )
        results_analyzer = NoSQLBenchResultsAnalyzer(analyzer_args=analyzer_args)
        try:
            results_analyzer.check_regression()
        except Exception as ex:  # pylint: disable=broad-except
            self.log.exception('Failed to check regression: %s', ex)

    @staticmethod
    def _display_results(report_builder: NoSQLBenchReportBuilder):
        report_builder.print_raw_report_file()

    def _update_stats_with_nosqlbench_report(self, report_builder: NoSQLBenchReportBuilder):
        self.update({"results": report_builder.abridged_report})

    def _update_test_details(self, include_setup_details: bool = False):
        if not self.create_stats:
            return

        if not self._stats:
            self.log.error("Stats was not initialized. Could be error during TestConfig")
            return

        update_data = {
            "test_details": self._stats.setdefault("test_details", {}),
            "status": self.status
        }

        update_data["test_details"].update({"time_completed": datetime.utcnow().strftime("%Y-%m-%d %H:%M")})

        if include_setup_details:
            update_data["setup_details"] = self._stats.setdefault("setup_details", {})

        if self.params.get("store_perf_results") and self.monitors and self.monitors.nodes:
            test_details = update_data["test_details"]
            update_data["results"] = self.get_prometheus_stats()
            grafana_dataset = self.monitors.get_grafana_screenshot_and_snapshot(test_details["start_time"])
            test_details.update({"grafana_screenshots": grafana_dataset.get("screenshots", []),
                                 "grafana_snapshots": grafana_dataset.get("snapshots", []),
                                 "grafana_annotations": self.monitors.upload_annotations_to_s3(),
                                 "prometheus_data": self.monitors.download_monitor_data(), })

        self.update(update_data)

    def _get_report_builder(self):
        loader = self.loaders.get_loader()
        summary_file = next(Path(loader.logdir).glob("*.summary"))
        report_builder = NoSQLBenchReportBuilder(summary_file)
        return report_builder
