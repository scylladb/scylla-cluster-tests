import logging
from pathlib import Path

from performance_regression_test import PerformanceRegressionTest
from sdcm.results_analyze.nosqlbench.analyzer import NoSQLBenchResultsAnalyzer, NoSQLBenchAnalyzerArgs
from sdcm.results_analyze.nosqlbench.report_builder import NoSQLBenchReportBuilder
from sdcm.sct_events.file_logger import get_events_grouped_by_category

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
        stress_cmd = self.params.get("stress_cmd_m")
        self.create_test_stats(sub_type="mixed", doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        # results = self.get_stress_results(queue=stress_queue, calculate_stats=False)
        # LOGGER.info("Raw nosqlbench run result: %s", results)
        self.update_test_details(scylla_conf=False)
        report_builder = self._get_report_builder()
        self._display_results(report_builder)
        report_builder.build_all_reports()
        self._update_stats_with_nosqlbench_report(report_builder)
        report_builder.save_abridged_report_to_file()
        # self.check_regression()
        report_builder.print_raw_report_file()
        self._check_regression()

    def _check_regression(self):
        analyzer_args = NoSQLBenchAnalyzerArgs(
            test_id=self._test_id,
            es_index=self._test_index,
            es_doc_type=self._es_doc_type,
            email_recipients=self.params.get('email_recipients'),
            events=get_events_grouped_by_category(
                _registry=self.events_processes_registry,
                limit=self.params.get('events_limit_in_email')),
            is_gce=bool(self.params.get('cluster_backend') == 'gce'),
            use_wide_query=True
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

    def _get_report_builder(self):
        loader = self.loaders.get_loader()
        summary_file = next(Path(loader.logdir).glob("*.summary"))
        report_builder = NoSQLBenchReportBuilder(summary_file)
        return report_builder
