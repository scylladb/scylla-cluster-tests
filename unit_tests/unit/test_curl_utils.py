import pytest

from sdcm.utils.curl import RETRY_ALL_ERRORS_PROBE, curl_with_retry


@pytest.mark.parametrize(
    "url,kwargs,expected",
    [
        pytest.param(
            "https://example.com/file.tar.gz",
            {},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/file.tar.gz",
            id="defaults",
        ),
        pytest.param(
            "https://example.com/f.tar.gz",
            {"output": "/tmp/f.tar.gz"},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "-o /tmp/f.tar.gz https://example.com/f.tar.gz",
            id="output-file",
        ),
        pytest.param(
            "https://example.com/f",
            {"silent": True},
            f"curl -s --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/f",
            id="silent",
        ),
        pytest.param(
            "https://example.com/f",
            {"follow_redirects": True},
            f"curl -L --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/f",
            id="follow-redirects",
        ),
        pytest.param(
            "https://x.com",
            {"retry": 3, "retry_max_time": 60},
            f"curl --connect-timeout 10 --retry 3 --retry-max-time 60 {RETRY_ALL_ERRORS_PROBE} https://x.com",
            id="custom-retry",
        ),
        pytest.param(
            "https://x.com",
            {"extra_flags": "-H 'Accept: json'"},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "-H 'Accept: json' https://x.com",
            id="extra-flags",
        ),
        pytest.param(
            "https://x.com",
            {"fail_early": True},
            f"curl -f --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} https://x.com",
            id="fail-early",
        ),
        pytest.param(
            "http://localhost:9180/metrics",
            {"retry": 0},
            "curl --connect-timeout 10 http://localhost:9180/metrics",
            id="retry-zero",
        ),
        pytest.param(
            "https://x.com",
            {"retry_all_errors": False},
            "curl --connect-timeout 10 --retry 5 --retry-max-time 300 https://x.com",
            id="retry-all-errors-off",
        ),
        pytest.param(
            "https://x.com",
            {"retry": 0, "retry_all_errors": True},
            "curl --connect-timeout 10 https://x.com",
            id="retry-zero-all-errors-on",
        ),
        pytest.param(
            "https://x.com/vector.rpm",
            {"max_time": 600},
            f"curl --connect-timeout 10 --max-time 600 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://x.com/vector.rpm",
            id="max-time",
        ),
        pytest.param(
            "https://x.com/vector.rpm",
            {"max_time": 600, "retry": 0},
            "curl --connect-timeout 10 --max-time 600 https://x.com/vector.rpm",
            id="max-time-retry-zero",
        ),
    ],
)
def test_curl_with_retry(url, kwargs, expected):
    """curl_with_retry builds the expected curl command for each argument combination."""
    assert curl_with_retry(url, **kwargs) == expected


def test_starts_with_curl():
    """curl_with_retry output starts with the curl binary name."""
    assert curl_with_retry("https://x.com").startswith("curl ")


def test_retry_all_errors_probe_pinned():
    """RETRY_ALL_ERRORS_PROBE is the shell probe that adds --retry-all-errors only when curl supports it."""
    assert RETRY_ALL_ERRORS_PROBE == "$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors)"


def test_max_time_omitted_by_default():
    """A stalled transfer must be bounded only when the caller asks; other callers keep today's command."""
    assert "--max-time" not in curl_with_retry("https://x.com")
