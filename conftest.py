import pytest

from sdcm.utils.subtest_utils import SUBTESTS_FAILURES


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """
    Hook to capture the test report and attach it to the test item,
    so it can be accessed later during teardown or in fixtures.
    """
    outcome = yield
    report = outcome.get_result()
    setattr(item, "rep_" + report.when, report)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report: pytest.TestReport):
    """
    Hook to log subtest failures and their reports,
    so it can be accessed later during teardown or in fixtures.
    """
    if report.when == "call" and getattr(report, "context", None):
        if report.failed:
            SUBTESTS_FAILURES[report.nodeid].append(report)
