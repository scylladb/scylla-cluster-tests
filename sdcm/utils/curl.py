"""Retry-by-default curl command builder.

Every remoter.run("curl ...") call in SCT should use this helper
to ensure consistent retry/timeout behaviour across the framework.
"""

# Shell snippet that emits --retry-all-errors only when the curl on PATH is >= 7.71.
# Embeds a version check via command substitution so the generated command is safe
# to run on any distro, including RHEL 7/8 or Ubuntu 20.04 with older curl.
_RETRY_ALL_ERRORS_FLAG = (
    r"""$(curl -V 2>/dev/null | awk 'NR==1{split($2,v,"."); if(v[1]*1000+v[2]>=7071)print "--retry-all-errors"}')"""
)


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
        retry_all_errors: Emit ``--retry-all-errors`` to also retry connection reset errors
            (curl exit 35/56), which plain ``--retry`` does NOT handle. The flag is injected
            as a shell ``$(...)`` version check so it is silently omitted when the curl on
            PATH is older than 7.71 (RHEL 7/8, Ubuntu 20.04, etc.). Safe to pass on any
            distro. Ignored when ``retry`` is 0.
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
            parts.append(_RETRY_ALL_ERRORS_FLAG)
    if output:
        parts.append(f"-o {output}")
    if extra_flags:
        parts.append(extra_flags)
    parts.append(url)
    return " ".join(parts)
