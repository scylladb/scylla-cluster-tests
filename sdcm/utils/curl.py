"""Retry-by-default curl command builder.

Every remoter.run("curl ...") call in SCT should use this helper
to ensure consistent retry/timeout behaviour across the framework.
"""


def curl_with_retry(
    url: str,
    *,
    retry: int = 5,
    retry_max_time: int = 300,
    retry_all_errors: bool = False,
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
        retry_all_errors: Add ``--retry-all-errors`` to also retry connection reset errors
            (curl exit 35/56), which plain ``--retry`` does NOT handle. Disabled by default
            because the flag requires curl >= 7.71. Enable only for commands that run on the
            SCT runner or loader/monitor images; do not use on the distro under test (e.g.
            rhel7/8, ubuntu2004, etc. with older curl). Ignored when ``retry`` is 0.
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
            parts.append("--retry-all-errors")
    if output:
        parts.append(f"-o {output}")
    if extra_flags:
        parts.append(extra_flags)
    parts.append(url)
    return " ".join(parts)
