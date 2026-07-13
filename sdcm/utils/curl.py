"""Retry-by-default curl command builder.

Every remoter.run("curl ...") call in SCT should use this helper
to ensure consistent retry/timeout behaviour across the framework.
"""

# expands to --retry-all-errors when the executing node curl supports it (curl >= 7.71)
RETRY_ALL_ERRORS_PROBE = "$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors)"


def curl_with_retry(
    url: str,
    *,
    retry: int = 5,
    retry_max_time: int = 300,
    retry_all_errors: bool = True,
    connect_timeout: int = 10,
    output: str | None = None,
    silent: bool = False,
    follow_redirects: bool = False,
    fail_early: bool = False,
    extra_flags: str = "",
) -> str:
    """Build a curl command string with retry and timeout flags.

    Args:
        url: Target URL.
        retry: ``--retry`` count (0 to disable retry, e.g. for localhost).
        retry_max_time: ``--retry-max-time`` in seconds.
        retry_all_errors: Retry connection resets (curl exit 35/56), which plain
            ``--retry`` does NOT retry. Added via ``RETRY_ALL_ERRORS_PROBE`` runtime
            capability check, so the command stays valid on distros with curl < 7.71.
            Pass ``False`` for non-idempotent requests. Ignored when ``retry`` is 0.
        connect_timeout: ``--connect-timeout`` in seconds.
        output: Path for ``-o`` (download target).
        silent: Add ``-s`` flag.
        follow_redirects: Add ``-L`` flag.
        fail_early: Add ``-f`` (fail on HTTP errors).
        extra_flags: Arbitrary extra flags appended verbatim.

    Returns:
        A complete ``curl ...`` command string.
    """
    parts = ["curl"]
    if silent:
        parts.append("-s")
    if follow_redirects:
        parts.append("-L")
    if fail_early:
        parts.append("-f")
    parts.append(f"--connect-timeout {connect_timeout}")
    if retry > 0:
        parts.append(f"--retry {retry}")
        parts.append(f"--retry-max-time {retry_max_time}")
        if retry_all_errors:
            parts.append(RETRY_ALL_ERRORS_PROBE)
    if output:
        parts.append(f"-o {output}")
    if extra_flags:
        parts.append(extra_flags)
    parts.append(url)
    return " ".join(parts)
