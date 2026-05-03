import pytest

from sdcm.utils.curl import curl_with_retry


@pytest.mark.parametrize(
    "url,kwargs,expected",
    [
        (
            "https://example.com/file.tar.gz",
            {},
            "curl --connect-timeout 10 --retry 5 --retry-max-time 300 https://example.com/file.tar.gz",
        ),
        (
            "https://example.com/f.tar.gz",
            {"output": "/tmp/f.tar.gz"},
            "curl --connect-timeout 10 --retry 5 --retry-max-time 300 -o /tmp/f.tar.gz https://example.com/f.tar.gz",
        ),
        (
            "https://example.com/f",
            {"silent": True},
            "curl -s --connect-timeout 10 --retry 5 --retry-max-time 300 https://example.com/f",
        ),
        (
            "https://example.com/f",
            {"follow_redirects": True},
            "curl -L --connect-timeout 10 --retry 5 --retry-max-time 300 https://example.com/f",
        ),
        (
            "https://x.com",
            {"retry": 3, "retry_max_time": 60},
            "curl --connect-timeout 10 --retry 3 --retry-max-time 60 https://x.com",
        ),
        (
            "https://x.com",
            {"extra_flags": "-H 'Accept: json'"},
            "curl --connect-timeout 10 --retry 5 --retry-max-time 300 -H 'Accept: json' https://x.com",
        ),
        (
            "http://localhost:9180/metrics",
            {"retry": 0},
            "curl --connect-timeout 10 http://localhost:9180/metrics",
        ),
    ],
)
def test_curl_with_retry(url, kwargs, expected):
    assert curl_with_retry(url, **kwargs) == expected


def test_starts_with_curl():
    assert curl_with_retry("https://x.com").startswith("curl ")
