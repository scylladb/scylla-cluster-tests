# HTTP Retry & Timeout Conventions

Every outbound HTTP call in SCT — whether it goes out through Python `requests`
or through a `curl` command executed on a remote node — must retry and must have
a timeout. Transient network failures (CDN connection resets, 502s from a
mirror, a repo server hiccup) otherwise fail a whole test run, often during
`setUp` before a single Scylla node is provisioned.

Two utilities implement this. Use them instead of hand-rolling retry logic:

| Call style | Utility | Module |
|---|---|---|
| Python `requests` | `create_retry_session()` | `sdcm/utils/session.py` |
| `curl` on a node (or in a bash string) | `curl_with_retry()` | `sdcm/utils/curl.py` |

## `requests`: use the shared session factory

`create_retry_session()` returns a `requests.Session` with an
`HTTPAdapter(max_retries=Retry(...))` mounted on both `http://` and `https://`.

```python
from sdcm.utils.session import create_retry_session

session = create_retry_session()
response = session.get(url, timeout=30)
```

Defaults:

| Argument | Default | Meaning |
|---|---|---|
| `retries` | `3` | total retries per request |
| `backoff_factor` | `1` | multiplier for the delay between attempts |
| `status_forcelist` | `{429, 500, 502, 503, 504}` | status codes that trigger a retry |
| `allowed_methods` | `HEAD, GET, PUT, DELETE, OPTIONS, TRACE, POST` | methods eligible for retry |

Note that `POST` is retryable by default — SCT's `POST` endpoints (Argus, Scylla
REST API) are idempotent in practice. Pass a narrower `allowed_methods` if the
endpoint you are calling is not.

### Never call `requests.get()` / `requests.post()` bare

A module-level `requests.get(url)` has neither retry nor connect timeout. Create
a session instead. Classes that own an HTTP client should expose a
`_create_session()` that delegates to the factory, so retry depth stays
configurable per caller:

```python
class RestClient:
    def __init__(self, host: str, endpoint: str):
        ...
        self.session = self._create_session()

    @staticmethod
    def _create_session(retries: int = 5) -> requests.Session:
        return create_retry_session(retries=retries)
```

`sdcm/rest/rest_client.py` is the reference implementation.
`sdcm/db_stats.py`, `sdcm/prometheus.py`, `sdcm/kafka/kafka_cluster.py` and
`sdcm/cloud_api_client.py` follow the same shape. (`sdcm/logcollector.py` still
carries a private `_create_retry_session()` copy of the same logic — it should
delegate to the factory like the rest; don't copy it.)

**Retry is not a timeout.** `Retry` bounds the number of attempts; it does not
bound how long a single attempt may hang. Always pass `timeout=` to the request
itself as well.

## `curl`: use `curl_with_retry()`

`curl_with_retry()` builds a `curl ...` command string with retry and timeout
flags already applied. It returns a string — it does not execute anything — so
it works both for direct `remoter.run()` calls and for interpolation into bash
scripts.

```python
from sdcm.utils.curl import curl_with_retry

self.remoter.sudo(curl_with_retry(scylla_repo, output=repo_path, follow_redirects=True), retry=3)
```

Defaults produce:

```
curl --connect-timeout 10 --retry 5 --retry-max-time 300 <retry-all-errors-probe> <url>
```

| Argument | Default | Effect |
|---|---|---|
| `retry` | `5` | `--retry N`; `0` disables retry entirely |
| `retry_max_time` | `300` | `--retry-max-time` seconds |
| `retry_all_errors` | `True` | adds the `--retry-all-errors` probe (see below) |
| `connect_timeout` | `10` | `--connect-timeout` seconds |
| `output` | `None` | `-o <path>` |
| `silent` | `False` | `-s` |
| `follow_redirects` | `False` | `-L` |
| `fail_early` | `False` | `-f` — fail on HTTP error status |
| `extra_flags` | `""` | appended verbatim before the URL |

### Why `--retry-all-errors` goes through a probe

Plain `--retry` does **not** retry connection resets — curl exit codes 35 and
56, exactly the failure mode that broke `artifacts-rocky9-nonroot-test`.
`--retry-all-errors` does, but it only exists in curl >= 7.71, and older distros
in the matrix (rhel7/8-family, ubuntu2004) ship older curl and **hard-fail** on
the unknown flag.

`curl_with_retry()` therefore emits `RETRY_ALL_ERRORS_PROBE`, a runtime
capability check that expands to the flag only where it is supported:

```
$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors)
```

Rules that follow from this:

- **Never write a bare `--retry-all-errors` literal** in a shell script,
  userdata, or cloud-init template. Use `RETRY_ALL_ERRORS_PROBE`, or paste the
  snippet above verbatim in a plain (non-f) string.
- **Keep the probe quote-free** — it is interpolated into commands that are
  wrapped in both single and double quotes, and adding quotes breaks one of
  them.
- Pass `retry_all_errors=False` **only** for a genuinely non-idempotent request
  (a `POST`/`PUT`/`DELETE` that must not be replayed). It is not a valid
  workaround for anything else; on an idempotent download it silently removes
  connection-reset protection.

### curl inside bash scripts

Curl calls hidden in multi-line `shell_script_cmd(f"""...""")` blocks are the
easiest ones to miss — they don't match a `remoter.run("curl` grep. Interpolate
the helper into the f-string rather than writing curl flags by hand:

```python
vector_setup_curl = curl_with_retry(
    "https://setup.vector.dev", silent=True, follow_redirects=True, fail_early=True, extra_flags="-S"
)
return dedent(f"""\
    if bash -c "$({vector_setup_curl})"; then
    ...
""")
```

Build the command outside the f-string, then interpolate the variable —
`{curl_with_retry(...)}` inline works too but reads badly in long templates.
`sdcm/provision/common/utils.py`, `sdcm/sct_config.py`, `sdcm/cluster.py`,
`sdcm/sct_runner.py` and `sdcm/cluster_k8s/mini_k8s.py` all follow this pattern.

### Localhost and metadata endpoints

Calls to `localhost` or to a cloud metadata service have no network in between,
so a network retry buys nothing and just delays the failure. Pass `retry=0` —
but still go through the helper, so the call keeps a consistent
`--connect-timeout`:

```python
self.remoter.run(curl_with_retry(f"http://localhost:{port}/metrics", retry=0, silent=True), verbose=False)
```

Where the caller already wraps the call in `@retrying(...)`, that outer retry is
the one that matters; see `sdcm/utils/adaptive_timeouts/load_info_store.py`.

## Documenting exceptions

If a call genuinely must not retry, keep it out of the helper and say why, in
this exact form so review tooling can recognise it:

```python
# no-retry: <reason>
```

The reason has to be specific — "non-idempotent cluster-membership POST", not
"not needed". Anything without a `# no-retry:` comment is treated as an
oversight during review.

## Verifying compliance

These greps are triage starting points, not zero-hit gates — they match
docstrings, helper names and pre-existing call sites too. Run them against the
files your PR touches and check the hits by hand:

```bash
# curl strings that bypass the helper
grep -rn "curl" sdcm/ --include="*.py" | grep -v curl_with_retry | grep -v "# no-retry"

# requests calls that are not going through a session
grep -rn "requests\.\(get\|post\|put\|delete\)(" sdcm/ --include="*.py"

# bare --retry-all-errors literals (must go through the probe)
grep -rn -- "--retry-all-errors" sdcm/ | grep -v RETRY_ALL_ERRORS_PROBE

# unit tests for the curl helper
uv run python -m pytest unit_tests/test_curl_utils.py -v
```

The migration in PR #13509 converted the call sites that were failing runs; it
did not convert every one in the tree. A number of bare `requests.*` calls
remain (`sdcm/utils/version_utils.py`, `sdcm/utils/common.py`,
`sdcm/cluster.py`, others). Treat them as debt, not as precedent: new and
modified code follows the conventions above, and touching a legacy call site is
a good moment to convert it.

Both conventions are enforced in review: see
[Check 9 in the code-review skill](../skills/code-review/SKILL.md#check-9-http-resilience--retry-patterns)
and the `.coderabbit.yaml` path rules for `sdcm/utils/curl.py` and `sdcm/rest/**`.
A summary lives under "HTTP/Curl Conventions" in
[AGENTS.md](../AGENTS.md#code-style-guidelines) for agents and
contributors; this document is the full reference.
