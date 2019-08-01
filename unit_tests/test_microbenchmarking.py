import unittest
import os.path
import subprocess
import logging
import pickle
import tempfile

from sdcm.microbenchmarking import MicroBenchmarkingResultsAnalyzer, LargeNumberOfDatasetsException, EmptyResultFolder


logger = logging.getLogger("microbenchmarking-tests")


class TestMBM(unittest.TestCase):
    def setUp(self):
        self.mbra = MicroBenchmarkingResultsAnalyzer(email_recipients=('alex.bykov@scylladb.com', ))
        self.mbra.hostname = 'godzilla.cloudius-systems.com'
        self.cwd = '/sct/sdcm'
        self.mbra.test_run_date = "2019-06-27_11:39:40"

    @staticmethod
    def get_result_obj(path):
        with open(os.path.join(path, "result_obj"), "r") as result_file:
            expected_obj = pickle.load(result_file)
        return expected_obj

    @staticmethod
    def get_report_obj(path):
        with open(os.path.join(path, "report_obj"), "r") as result_file:
            expected_obj = pickle.load(result_file)
        return expected_obj

    def test_object_exists(self):
        self.assertIsInstance(self.mbra, MicroBenchmarkingResultsAnalyzer)
        self.assertEqual(self.mbra.hostname, 'godzilla.cloudius-systems.com')
        self.assertEqual(self.mbra._email_recipients, ('alex.bykov@scylladb.com', ))

    def test_get_result_with_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_AVGAIO')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        self.assertDictEqual(results, expected_obj)

    def test_get_result_without_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_without_AVGAIO')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        self.assertDictEqual(results, expected_obj)

    def test_get_result_with_new_metrics_avg_cpu(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_new_metric')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        self.assertDictEqual(results, expected_obj)

    def test_get_result_with_2_datasets(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_2_datasets')
        self.assertRaises(LargeNumberOfDatasetsException,
                          self.mbra.get_results,
                          results_path=result_path,
                          update_db=False)

    def test_get_result_for_empty_base_folder(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_empty_folder')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_get_result_for_empty_tests_folders(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_empty_tests_folders')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_get_result_for_empty_dataset_folders(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_empty_dataset_folders')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_check_regression_for_results_with_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_AVGAIO')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[result_obj.keys()[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_results_without_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_without_AVGAIO')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[result_obj.keys()[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_results_with_new_metrics_cpu_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_new_metric')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[result_obj.keys()[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_new_metrics_cpu_aio_and_empty_last_and_best(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_new_metric')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[result_obj.keys()[0]]['versions']['scylla-server']
        self.mbra.lower_better = ('avg cpu', )
        self.mbra.metrics = self.mbra.higher_better + self.mbra.lower_better
        report_results = self.mbra.check_regression(result_obj)

        self.assertTrue(report_results)
        self.assertEqual(report_results['small-partition-slicing_0-1.1']['avg cpu']['Current'], 26.0)
        self.assertEqual(report_results['small-partition-slicing_0-1.1']['avg cpu']['Last, commit, date'][0], None)
        self.assertEqual(report_results['small-partition-slicing_0-1.1']['avg cpu']['Best, commit, date'][0], None)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg cpu']['Current'], 58.0)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg cpu']['Last, commit, date'][0], None)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg cpu']['Best, commit, date'][0], None)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg cpu']['Current'], 62.0)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg cpu']['Last, commit, date'][0], None)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg cpu']['Best, commit, date'][0], None)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg cpu']['Current'], 97.0)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg cpu']['Last, commit, date'][0], None)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg cpu']['Best, commit, date'][0], None)

    def test_generate_html_report_file_and_email_body_for_results_with_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_AVGAIO')
        report_obj = self.get_report_obj(result_path)
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        report_file, report_html = self.mbra.send_html_report(report_obj, html_report_path=html_report, send=False)

        self.assertTrue(os.path.exists(report_file))
        self.assertGreater(os.path.getsize(report_file), 0)
        self.assertTrue(report_html)
        self.assertIn('large-partition-forwarding', report_html)
        self.assertIn('small-partition-slicing', report_html)

    def test_generate_html_report_file_and_email_body_for_results_without_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_without_AVGAIO')
        report_obj = self.get_report_obj(result_path)
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        report_file, report_html = self.mbra.send_html_report(report_obj, html_report_path=html_report, send=False)

        self.assertTrue(os.path.exists(report_file))
        self.assertGreater(os.path.getsize(report_file), 0)
        self.assertTrue(report_html)
        self.assertIn('large-partition-forwarding', report_html)
        self.assertIn('large-partition-slicing-single-key-reader', report_html)

    def test_generate_html_report_for_avg_aio_with_zero_value(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_AVGAIO_0')
        result_obj = self.mbra.get_results(results_path=result_path, update_db=False)
        self.mbra.cur_version_info = result_obj[result_obj.keys()[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)

        self.assertEqual(report_results['small-partition-slicing_0-256.1']['avg aio']['Current'], 0.0)
        self.assertEqual(report_results['small-partition-slicing_0-256.1']['avg aio']['Diff best [%]'], None)
        self.assertEqual(report_results['small-partition-slicing_0-256.1']['avg aio']['Diff last [%]'], None)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg aio']['Current'], 0.0)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg aio']['Diff best [%]'], None)
        self.assertEqual(report_results['large-partition-forwarding_no.1']['avg aio']['Diff last [%]'], None)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg aio']['Current'], 0.0)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg aio']['Diff best [%]'], None)
        self.assertEqual(report_results['large-partition-forwarding_yes.1']['avg aio']['Diff last [%]'], None)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg aio']['Current'], 0.0)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg aio']['Diff best [%]'], None)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']['avg aio']['Diff last [%]'], None)

        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        report_file, report_html = self.mbra.send_html_report(report_results, html_report_path=html_report, send=False)

        self.assertTrue(os.path.exists(report_file))
        self.assertGreater(os.path.getsize(report_file), 0)
        self.assertTrue(report_html)
        self.assertIn('large-partition-forwarding', report_html)
        # no regression for small-partition-slicing tests
        self.assertNotIn('small-partition-slicing', report_html)

    def test_empty_current_result(self):
        result_obj = {}
        report_results = self.mbra.check_regression(result_obj)
        self.assertFalse(report_results)

    def test_main_help_message(self):
        ps = subprocess.Popen(['./microbenchmarking.py', '--help'],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        stdout, stderr = ps.communicate()
        self.assertEqual(ps.returncode, 0)
        self.assertIn('usage: microbenchmarking.py [-h] Modes ...', stdout)
        self.assertFalse(stderr)

    def test_help_parameter_for_exclude(self):
        msg = 'usage: microbenchmarking.py exclude [-h]'
        ps = subprocess.Popen(['./microbenchmarking.py', 'exclude', '--help'],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        stdout, stderr = ps.communicate()
        self.assertEqual(ps.returncode, 0)
        self.assertIn(msg, stdout)
        self.assertFalse(stderr)

    def test_help_parameter_for_check(self):
        msg = """usage: microbenchmarking.py check [-h] [--update-db]"""

        ps = subprocess.Popen(['./microbenchmarking.py', 'check', '--help'],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

        stdout, stderr = ps.communicate()
        self.assertEqual(ps.returncode, 0)
        self.assertIn(msg, stdout)
        self.assertFalse(stderr)

    def test_execute_mbm_from_cli(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/MBM/PFF_with_AVGAIO')
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        ps = subprocess.Popen(['./microbenchmarking.py', 'check',
                               '--results-path', result_path,
                               '--email-recipients', 'alex.bykov@scylladb.com',
                               '--report-path', html_report,
                               '--hostname', self.mbra.hostname],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        stdout, stderr = ps.communicate()

        self.assertEqual(ps.returncode, 0)
        self.assertIn("Send email to ['alex.bykov@scylladb.com']", stdout)
        self.assertIn("HTML report saved to '/tmp/microbenchmarking", stdout)
        self.assertIn("Rendering results to html using 'results_microbenchmark.html' template...", stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
