## Remote Commands and HTTP

### curl Through curl_with_retry

Each `remoter.run("curl ...")` call uses `curl_with_retry()` from
`sdcm/utils/curl.py`. Do not write a bare `--retry-all-errors` in a shell
script or in user data. Use `RETRY_ALL_ERRORS_PROBE`. The bare flag fails on
curl before 7.71. Set `retry_all_errors=False` only for a
non-idempotent request that must not run twice.

### requests Through a Retry Session

Each `requests` call goes through a session with a retry adapter, as in
`sdcm/rest/rest_client.py`. Mark an exception with a
`# no-retry: <reason>` comment.

### Package Installation

Each remote `apt-get`, `yum`, `dnf`, or `zypper` call has a timeout, a retry,
and the non-interactive flags. One hung package command can block a test for
hours. `skills/package-installation/SKILL.md` gives the flags for each
package manager.

Sources: `AGENTS.md` section HTTP/Curl Conventions,
`docs/http-retry-conventions.md`, `skills/package-installation/SKILL.md`.
