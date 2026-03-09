import os
import zipfile

import pytest

from sdcm.send_email import LongevityEmailReporter, read_email_data_from_file


class LongevityEmailReporterTest(LongevityEmailReporter):
    def send_email(self, email):
        return


@pytest.fixture
def report_data(tmp_path):
    def _load(zipfile_path):
        with zipfile.ZipFile(os.path.join(os.path.dirname(__file__), zipfile_path), "r") as file:
            file.extractall(tmp_path)
            return read_email_data_from_file(os.path.join(tmp_path, "email_data.json"))

    return _load


def test_longevity_report_big_file(tmp_path, report_data):
    test_results = report_data("test_data/test_send_email/LongevityEmailReporter/big_report.zip")
    assert test_results
    reporter = LongevityEmailReporterTest(email_recipients="some@host.com", logdir=str(tmp_path))
    reporter.send_report(test_results)
