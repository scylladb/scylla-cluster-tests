import pytest

from sdcm.utils.curl import RETRY_ALL_ERRORS_PROBE, curl_with_retry


@pytest.mark.parametrize(
    "url,kwargs,expected",
    [
        (
            "https://example.com/file.tar.gz",
            {},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/file.tar.gz",
        ),
        (
            "https://example.com/f.tar.gz",
            {"output": "/tmp/f.tar.gz"},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "-o /tmp/f.tar.gz https://example.com/f.tar.gz",
        ),
        (
            "https://example.com/f",
            {"silent": True},
            f"curl -s --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/f",
        ),
        (
            "https://example.com/f",
            {"follow_redirects": True},
            f"curl -L --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "https://example.com/f",
        ),
        (
            "https://x.com",
            {"retry": 3, "retry_max_time": 60},
            f"curl --connect-timeout 10 --retry 3 --retry-max-time 60 {RETRY_ALL_ERRORS_PROBE} https://x.com",
        ),
        (
            "https://x.com",
            {"extra_flags": "-H 'Accept: json'"},
            f"curl --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} "
            "-H 'Accept: json' https://x.com",
        ),
        (
            "https://x.com",
            {"fail_early": True},
            f"curl -f --connect-timeout 10 --retry 5 --retry-max-time 300 {RETRY_ALL_ERRORS_PROBE} https://x.com",
        ),
        (
            "http://localhost:9180/metrics",
            {"retry": 0},
            "curl --connect-timeout 10 http://localhost:9180/metrics",
        ),
        (
            "https://x.com",
            {"retry_all_errors": False},
            "curl --connect-timeout 10 --retry 5 --retry-max-time 300 https://x.com",
        ),
        (
            "https://x.com",
            {"retry": 0, "retry_all_errors": True},
            "curl --connect-timeout 10 https://x.com",
        ),
    ],
)
def test_curl_with_retry(url, kwargs, expected):
    assert curl_with_retry(url, **kwargs) == expected


def test_starts_with_curl():
    assert curl_with_retry("https://x.com").startswith("curl ")


def test_retry_all_errors_probe_pinned():
    assert RETRY_ALL_ERRORS_PROBE == "$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors)"
