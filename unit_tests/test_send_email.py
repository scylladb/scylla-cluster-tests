import os
import unittest
import tempfile
import shutil
import zipfile

from sdcm.send_email import LongevityEmailReporter, read_email_data_from_file


class LongevityEmailReporterTest(LongevityEmailReporter):
    def send_email(self, email):
        return


class EmailReporterTest(unittest.TestCase):
    temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir)

    def _get_report_data(self, zipfile_path):
        with zipfile.ZipFile(os.path.join(os.path.dirname(__file__), zipfile_path), 'r') as file:
            file.extractall(self.temp_dir)
            return read_email_data_from_file(os.path.join(self.temp_dir, 'email_data.json'))

    def test_longevity_report_big_file(self):
        test_results = self._get_report_data('test_data/test_send_email/LongevityEmailReporter/big_report.zip')
        self.assertTrue(test_results)
        reporter = LongevityEmailReporterTest(email_recipients='some@host.com', logdir=self.temp_dir)
        reporter.send_report(test_results)
