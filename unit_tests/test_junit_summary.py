from pathlib import Path

import pytest

from sdcm.utils.junit_summary import (
    COMMENT_MARKER,
    MAX_COMMENT_CHARS,
    FailureDetail,
    PrecommitHookResult,
    SuiteResults,
    compose_comment,
    format_markdown,
    format_precommit_markdown,
    merge_results,
    parse_junit_xml,
    parse_precommit_log,
)

TEST_DATA_DIR = Path(__file__).parent / "test_data" / "junit_summary"


@pytest.fixture()
def passing_xml():
    return TEST_DATA_DIR / "passing.xml"


@pytest.fixture()
def failing_xml():
    return TEST_DATA_DIR / "failing.xml"


@pytest.fixture()
def single_suite_xml():
    return TEST_DATA_DIR / "single_suite.xml"


@pytest.fixture()
def precommit_all_passing():
    return (TEST_DATA_DIR / "precommit_all_passing.txt").read_text()


@pytest.fixture()
def precommit_with_failures():
    return (TEST_DATA_DIR / "precommit_with_failures.txt").read_text()


# -- parse_junit_xml --


def test_parse_junit_xml_all_passing(passing_xml):
    results = parse_junit_xml(passing_xml)

    assert results.passed == 3
    assert results.failed == 0
    assert results.errors == 0
    assert results.skipped == 0
    assert results.failures == []


def test_parse_junit_xml_mixed_results(failing_xml):
    results = parse_junit_xml(failing_xml)

    assert results.passed == 1
    assert results.failed == 1
    assert results.errors == 1
    assert results.skipped == 1
    assert len(results.failures) == 2


def test_parse_junit_xml_failure_details(failing_xml):
    results = parse_junit_xml(failing_xml)
    failure = results.failures[0]

    assert failure.test == "unit_tests.test_config.test_invalid_backend"
    assert failure.file == "unit_tests/test_config.py"
    assert failure.failure_type == "FAILURE"
    assert "AssertionError" in failure.message


def test_parse_junit_xml_error_details(failing_xml):
    results = parse_junit_xml(failing_xml)
    error = results.failures[1]

    assert error.test == "unit_tests.test_nemesis.test_registration"
    assert error.failure_type == "ERROR"
    assert "KeyError" in error.message


def test_parse_junit_xml_single_testsuite_root(single_suite_xml):
    """JUnit XML with <testsuite> as root instead of <testsuites>."""
    results = parse_junit_xml(single_suite_xml)

    assert results.passed == 1
    assert results.failed == 1
    assert results.failures[0].test == "unit_tests.test_foo.test_bar"


# -- merge_results --


def test_merge_results_counts():
    r1 = SuiteResults(
        passed=3,
        failed=1,
        errors=0,
        skipped=0,
        failures=[
            FailureDetail(test="a.b", file="a.py", failure_type="FAILURE", message="fail", details=""),
        ],
    )
    r2 = SuiteResults(
        passed=5,
        failed=0,
        errors=1,
        skipped=2,
        failures=[
            FailureDetail(test="c.d", file="c.py", failure_type="ERROR", message="err", details=""),
        ],
    )

    merged = merge_results([r1, r2])

    assert merged.passed == 8
    assert merged.failed == 1
    assert merged.errors == 1
    assert merged.skipped == 2
    assert len(merged.failures) == 2


def test_merge_results_empty_list():
    merged = merge_results([])

    assert merged.passed == 0
    assert merged.failures == []


# -- format_markdown --


def test_format_markdown_passing_contains_checkmark():
    results = SuiteResults(passed=5)

    output = format_markdown(results, "Unit Tests")

    assert ":white_check_mark:" in output
    assert "PASSED" in output
    assert "Failed Tests" not in output


def test_format_markdown_failing_contains_failure_details():
    results = SuiteResults(
        passed=3,
        failed=1,
        failures=[
            FailureDetail(
                test="unit_tests.test_config.test_defaults",
                file="unit_tests/test_config.py",
                failure_type="FAILURE",
                message="assert False",
                details="traceback here",
            ),
        ],
    )

    output = format_markdown(results, "Unit Tests", "https://jenkins/build/1")

    assert ":x:" in output
    assert "FAILED" in output
    assert "`unit_tests.test_config.test_defaults`" in output
    assert "`unit_tests/test_config.py`" in output
    assert "traceback here" in output
    assert "https://jenkins/build/1" in output


def test_format_markdown_contains_comment_marker():
    results = SuiteResults(passed=1)

    output = format_markdown(results, "Tests")

    assert COMMENT_MARKER in output


def test_format_markdown_contains_summary_table():
    results = SuiteResults(passed=10, failed=2, errors=1, skipped=3)

    output = format_markdown(results, "Tests")

    assert "| 16 | 10 | 2 | 1 | 3 |" in output


def test_format_markdown_large_output_truncated():
    """Output exceeding MAX_COMMENT_CHARS is truncated with a notice."""
    failures = [
        FailureDetail(
            test=f"test_{i}",
            file=f"test_{i}.py",
            failure_type="FAILURE",
            message="x" * 200,
            details="y" * 2000,
        )
        for i in range(100)
    ]
    results = SuiteResults(failed=100, failures=failures)

    output = format_markdown(results, "Tests")

    assert len(output) <= MAX_COMMENT_CHARS + 200  # small buffer for truncation notice
    assert "omitted" in output


def test_format_markdown_no_build_url_omits_link():
    results = SuiteResults(passed=1)

    output = format_markdown(results, "Tests")

    assert "Full build log" not in output


# -- parse_precommit_log --


def test_parse_precommit_all_passing(precommit_all_passing):
    results = parse_precommit_log(precommit_all_passing)

    assert len(results) == 5
    assert all(r.status in ("Passed", "Skipped") for r in results)
    assert results[0].hook == "trailing whitespace"
    assert results[2].hook == "check yaml"
    assert results[2].status == "Skipped"


def test_parse_precommit_with_failures(precommit_with_failures):
    results = parse_precommit_log(precommit_with_failures)

    passed = [r for r in results if r.status == "Passed"]
    failed = [r for r in results if r.status == "Failed"]
    skipped = [r for r in results if r.status == "Skipped"]

    assert len(passed) == 1
    assert len(failed) == 2
    assert len(skipped) == 1


def test_parse_precommit_failure_details(precommit_with_failures):
    results = parse_precommit_log(precommit_with_failures)
    ruff_format = [r for r in results if r.hook == "ruff-format"][0]

    assert ruff_format.status == "Failed"
    assert "ruff-format" in ruff_format.details
    assert "diff --git" in ruff_format.details


def test_parse_precommit_second_failure_details(precommit_with_failures):
    results = parse_precommit_log(precommit_with_failures)
    ruff = [r for r in results if r.hook == "ruff"][0]

    assert ruff.status == "Failed"
    assert "F401" in ruff.details


def test_parse_precommit_empty_input():
    results = parse_precommit_log("")

    assert results == []


def test_parse_precommit_passed_hooks_have_no_details(precommit_all_passing):
    results = parse_precommit_log(precommit_all_passing)

    for hook in results:
        assert hook.details == ""


# -- format_precommit_markdown --


def test_format_precommit_markdown_all_passing():
    hooks = [
        PrecommitHookResult(hook="ruff", status="Passed", details=""),
        PrecommitHookResult(hook="ruff-format", status="Passed", details=""),
    ]

    output = format_precommit_markdown(hooks)

    assert ":white_check_mark:" in output
    assert "PASSED" in output
    assert "Failed Hooks" not in output


def test_format_precommit_markdown_with_failure():
    hooks = [
        PrecommitHookResult(hook="ruff", status="Passed", details=""),
        PrecommitHookResult(hook="ruff-format", status="Failed", details="diff output here"),
    ]

    output = format_precommit_markdown(hooks)

    assert ":x:" in output
    assert "FAILED" in output
    assert "`ruff-format`" in output
    assert "diff output here" in output


def test_format_precommit_markdown_summary_table():
    hooks = [
        PrecommitHookResult(hook="a", status="Passed", details=""),
        PrecommitHookResult(hook="b", status="Failed", details="err"),
        PrecommitHookResult(hook="c", status="Skipped", details=""),
    ]

    output = format_precommit_markdown(hooks)

    assert "| 3 | 1 | 1 | 1 |" in output


# -- compose_comment --


def test_compose_comment_precommit_only():
    hooks = [
        PrecommitHookResult(hook="ruff", status="Failed", details="error"),
    ]

    output = compose_comment(precommit_hooks=hooks, stage_name="Test Summary")

    assert COMMENT_MARKER in output
    assert "Test Summary: FAILED" in output
    assert "Precommit: FAILED" in output


def test_compose_comment_tests_only():
    results = SuiteResults(passed=5)

    output = compose_comment(test_results=results, stage_name="Test Summary")

    assert COMMENT_MARKER in output
    assert "Test Summary: PASSED" in output


def test_compose_comment_combined():
    hooks = [PrecommitHookResult(hook="ruff", status="Passed", details="")]
    results = SuiteResults(
        passed=5,
        failed=1,
        failures=[
            FailureDetail(test="a.b", file="a.py", failure_type="FAILURE", message="fail", details=""),
        ],
    )

    output = compose_comment(
        test_results=results,
        precommit_hooks=hooks,
        stage_name="Test Summary",
    )

    assert COMMENT_MARKER in output
    assert "Test Summary: FAILED" in output
    assert "Precommit: PASSED" in output
    assert "`a.b`" in output


def test_compose_comment_all_passing():
    hooks = [PrecommitHookResult(hook="ruff", status="Passed", details="")]
    results = SuiteResults(passed=5)

    output = compose_comment(
        test_results=results,
        precommit_hooks=hooks,
        stage_name="Test Summary",
    )

    assert "Test Summary: PASSED" in output
