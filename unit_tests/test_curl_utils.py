from sdcm.utils.curl import curl_with_retry


class TestCurlWithRetry:
    def test_basic_url_adds_retry_flags(self):
        result = curl_with_retry("https://example.com/file.tar.gz")
        assert "--retry 5" in result
        assert "--retry-max-time 300" in result
        assert "--connect-timeout 10" in result
        assert "https://example.com/file.tar.gz" in result

    def test_output_flag(self):
        result = curl_with_retry("https://example.com/f.tar.gz", output="/tmp/f.tar.gz")
        assert "-o /tmp/f.tar.gz" in result

    def test_silent_flag(self):
        result = curl_with_retry("https://example.com/f", silent=True)
        assert "-s" in result

    def test_follow_redirects(self):
        result = curl_with_retry("https://example.com/f", follow_redirects=True)
        assert "-L" in result

    def test_custom_retry_params(self):
        result = curl_with_retry("https://x.com", retry=3, retry_max_time=60)
        assert "--retry 3" in result
        assert "--retry-max-time 60" in result

    def test_extra_flags_appended(self):
        result = curl_with_retry("https://x.com", extra_flags="-H 'Accept: json'")
        assert "-H 'Accept: json'" in result

    def test_localhost_skips_retry_when_requested(self):
        result = curl_with_retry("http://localhost:9180/metrics", retry=0)
        assert "--retry" not in result

    def test_starts_with_curl(self):
        result = curl_with_retry("https://x.com")
        assert result.startswith("curl ")
