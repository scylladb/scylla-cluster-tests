from __future__ import absolute_import
import unittest
import os.path
import subprocess
import logging
import pickle
import tempfile
import json
import zipfile
import hashlib

from sdcm.microbenchmarking import MicroBenchmarkingResultsAnalyzer, LargeNumberOfDatasetsException, EmptyResultFolder


LOGGER = logging.getLogger("microbenchmarking-tests")


class MicroBenchmarkingResultsAnalyzerMock(MicroBenchmarkingResultsAnalyzer):
    _mock_returns = None
    _mock_returns_path = os.path.join(os.path.dirname(
        __file__), 'test_data/test_microbenchmarking/mock_response/{funct_name}/{hash}.zip')

    def _send_report(self, subject, summary_html, files):
        return files[0], summary_html

    def _get_prior_tests(self, filter_path, additional_filter=''):
        return self._mock_function('_get_prior_tests', filter_path, additional_filter)

    def _mock_function(self, target_fn, *args, **kwargs):
        output = self._load_mock_return('_get_prior_tests', args=args, kwargs=kwargs)
        if output is None:
            tmp = super()
            output = getattr(tmp, target_fn)(*args, **kwargs)
            file_path = self._save_mock_return(
                '_get_prior_tests',
                args=args,
                kwargs=kwargs,
                result=output)
            raise RuntimeError(f'There is no mock response {file_path} for this call ')
        return output

    def _load_mock_return(self, target_fn, args=None, kwargs=None):
        params_hash = self._get_hash(args, kwargs)
        source_file_full_path = self._mock_returns_path.format(funct_name=target_fn, hash=params_hash)
        if not os.path.exists(source_file_full_path):
            return None
        with zipfile.ZipFile(source_file_full_path, 'r') as zip_ref:
            return json.load(zip_ref.open('return.json'))

    def _save_mock_return(self, target_fn, args=None, kwargs=None, result=None):
        params_hash = self._get_hash(args, kwargs)
        source_file_full_path = self._mock_returns_path.format(funct_name=target_fn, hash=params_hash)
        with zipfile.ZipFile(source_file_full_path, 'w', compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr('return.json', json.dumps(result).encode())
        return source_file_full_path

    @staticmethod
    def _get_hash(*args, **kwargs):
        kwargs = sorted(kwargs.items(), key=lambda x: x[0])
        params_hash = hashlib.md5(repr({'args': args, 'kwarg': kwargs}).encode()).hexdigest()
        return params_hash


class TestMBM(unittest.TestCase):
    def setUp(self):
        self.mbra = MicroBenchmarkingResultsAnalyzerMock(email_recipients=(
            'alex.bykov@scylladb.com', ), es_index="microbenchmarking")
        self.mbra.hostname = 'monster'
        self.cwd = os.path.join(os.path.dirname(__file__), '..', 'sdcm')
        self.mbra.test_run_date = "2019-06-27_11:39:40"
        self.es_index = "microbenchmarkingv3"

    @staticmethod
    def get_result_obj(path):
        with open(os.path.join(path, "result_obj"), "rb") as result_file:
            expected_obj = pickle.load(result_file, encoding="latin-1")
        return expected_obj

    @staticmethod
    def get_report_obj(path):
        with open(os.path.join(path, "report_obj"), "rb") as result_file:
            expected_obj = pickle.load(result_file, encoding="latin-1")
        return expected_obj

    def test_object_exists(self):

        self.assertIsInstance(self.mbra, MicroBenchmarkingResultsAnalyzer)
        self.assertEqual(self.mbra.hostname, 'monster')
        self.assertEqual(self.mbra._email_recipients, ('alex.bykov@scylladb.com', ))

    def test_get_result_with_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_AVGAIO')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        assert results == expected_obj

    def test_get_result_without_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_without_AVGAIO')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        self.assertDictEqual(results, expected_obj)

    def test_get_result_with_new_metrics_avg_cpu(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_new_metric')
        results = self.mbra.get_results(results_path=result_path, update_db=False)
        self.assertTrue(results)
        expected_obj = self.get_result_obj(result_path)
        self.assertDictEqual(results, expected_obj)

    def test_get_result_with_2_datasets(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_2_datasets')
        self.assertRaises(LargeNumberOfDatasetsException,
                          self.mbra.get_results,
                          results_path=result_path,
                          update_db=False)

    def test_get_result_for_empty_base_folder(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_empty_folder')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_get_result_for_empty_tests_folders(self):
        result_path = os.path.join(os.path.dirname(
            __file__), 'test_data/test_microbenchmarking/PFF_empty_tests_folders')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_get_result_for_empty_dataset_folders(self):
        result_path = os.path.join(os.path.dirname(
            __file__), 'test_data/test_microbenchmarking/PFF_empty_dataset_folders')
        self.assertRaises(EmptyResultFolder, self.mbra.get_results, results_path=result_path, update_db=False)

    def test_check_regression_for_results_with_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_AVGAIO')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[list(result_obj.keys())[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_results_without_avg_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_without_AVGAIO')
        result_obj = self.get_result_obj(result_path)
#        self.mbra.load_mock_return('_get_prior_tests', 'PFF_without_AVGAIO/_get_prior_tests.zip')
        self.mbra.cur_version_info = result_obj[list(result_obj.keys())[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_results_with_new_metrics_cpu_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_new_metric')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[list(result_obj.keys())[0]]['versions']['scylla-server']
        report_results = self.mbra.check_regression(result_obj)
        self.assertTrue(report_results)

    def test_check_regression_for_new_metrics_cpu_aio_and_empty_last_and_best(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_new_metric')
        result_obj = self.get_result_obj(result_path)
        self.mbra.cur_version_info = result_obj[list(result_obj.keys())[0]]['versions']['scylla-server']
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
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']
                         ['avg cpu']['Last, commit, date'][0], None)
        self.assertEqual(report_results['small-partition-slicing_500000-4096.1']
                         ['avg cpu']['Best, commit, date'][0], None)

    def test_generate_html_report_file_and_email_body_for_results_with_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_AVGAIO')
        report_obj = self.get_report_obj(result_path)
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        report_file, report_html = self.mbra.send_html_report(report_obj, html_report_path=html_report)

        self.assertTrue(os.path.exists(report_file))
        self.assertGreater(os.path.getsize(report_file), 0)
        self.assertTrue(report_html)
        self.assertIn('large-partition-forwarding', report_html)
        self.assertIn('small-partition-slicing', report_html)

    def verify_html_report_correctness(self, report_results):
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        report_file, report_html = self.mbra.send_html_report(report_results, html_report_path=html_report)

        self.assertTrue(os.path.exists(report_file))
        self.assertGreater(os.path.getsize(report_file), 0)
        self.assertTrue(report_html)
        for key in report_results.keys():
            if report_results[key].get('has_diff', False) or report_results[key].get('has_improve', False):
                self.assertIn(key, report_results)
        self.assertIn('Kibana dashboard', report_html)

    def test_generate_html_report_file_and_email_body_for_results_without_aio(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_without_AVGAIO')
        report_obj = self.get_report_obj(result_path)

        self.verify_html_report_correctness(report_obj)

    def test_generate_html_report_for_avg_aio_with_zero_value(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_AVGAIO_0')
        result_obj = self.mbra.get_results(results_path=result_path, update_db=False)
        self.mbra.cur_version_info = result_obj[list(result_obj.keys())[0]]['versions']['scylla-server']
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

        self.verify_html_report_correctness(report_results)

    def test_empty_current_result(self):
        result_obj = {}
        report_results = self.mbra.check_regression(result_obj)
        self.assertFalse(report_results)

    def test_main_help_message(self):
        with subprocess.Popen(['./microbenchmarking.py', '--help'], cwd=self.cwd,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE) as ps:
            stdout, stderr = ps.communicate()
            self.assertEqual(ps.returncode, 0)
            self.assertIn(b'usage: microbenchmarking.py [-h] Modes ...', stdout)
            self.assertFalse(stderr)

    def test_help_parameter_for_exclude(self):
        msg = b'usage: microbenchmarking.py exclude [-h]'
        with subprocess.Popen(['./microbenchmarking.py', 'exclude', '--help'],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as ps:
            stdout, stderr = ps.communicate()
            self.assertEqual(ps.returncode, 0)
            self.assertIn(msg, stdout)
            self.assertFalse(stderr)

    def test_help_parameter_for_check(self):
        msg = b"""usage: microbenchmarking.py check [-h] [--update-db]"""

        with subprocess.Popen(['./microbenchmarking.py', 'check', '--help'],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as ps:

            stdout, stderr = ps.communicate()
            self.assertEqual(ps.returncode, 0)
            self.assertIn(msg, stdout)
            self.assertFalse(stderr)

    def test_execute_mbm_from_cli(self):
        result_path = os.path.join(os.path.dirname(__file__), 'test_data/test_microbenchmarking/PFF_with_AVGAIO')
        html_report = tempfile.mkstemp(suffix=".html", prefix="microbenchmarking-")[1]
        with subprocess.Popen(['./microbenchmarking.py', 'check',
                               '--results-path', result_path,
                               '--email-recipients', 'alex.bykov@scylladb.com',
                               '--report-path', html_report,
                               '--hostname', self.mbra.hostname,
                               "--es-index", self.es_index],
                              cwd=self.cwd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as ps:
            stdout, _ = ps.communicate()
            print(_)
            print(stdout)
            self.assertEqual(ps.returncode, 0)
            self.assertIn(b"Send email to ['alex.bykov@scylladb.com']", stdout)
            self.assertIn(b"HTML report saved to '/tmp/microbenchmarking", stdout)
            self.assertIn(b"Rendering results to html using 'results_microbenchmark.html' template...", stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
