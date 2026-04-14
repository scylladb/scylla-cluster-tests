# Mini-Plan: Curl & Requests Retry Enforcement

**Date:** 2026-04-05
**Estimated LOC:** ~800
**Related PR:** #13509

## Problem

Curl commands and `requests` calls without retry/timeout cause intermittent test failures from transient network issues (e.g. PR #13499). ~107 curl invocations have ~30% retry compliance, and most `requests` calls across `sdcm/rest/rest_client.py`, `sdcm/logcollector.py`, `sdcm/prometheus.py`, `sdcm/db_stats.py` lack retry adapters entirely.

## Approach

1. **Consolidate bash curl calls** -- create `sdcm/utils/curl.py` helper that enforces `--retry 5 --retry-max-time 300` by default, skips retry for localhost/metadata endpoints. Migrate all `remoter.run("curl ...")` calls to use it.

2. **Add retry adapters to RestClient** -- update `sdcm/rest/rest_client.py` to use `requests.Session` with `HTTPAdapter(max_retries=Retry(...))`, following the existing pattern in `sdcm/cloud_api_client.py:76-82`. Audit and fix bare `requests.get()`/`requests.post()` calls.

3. **Update AI review skill** -- add HTTP resilience check to `skills/code-review/SKILL.md` that flags bare `remoter.run("curl ...")` and bare `requests.get()`/`requests.post()` in PR reviews. Update `AGENTS.md` with the new conventions.

4. **Scan and fix remaining violations** -- grep for remaining bare curl/requests calls, migrate to utilities or add `--retry` flags directly (shell scripts). Document exceptions with `# no-retry: <reason>` comments.

## Files to Modify

- `sdcm/utils/curl.py` -- **new** -- retry-by-default curl helper
- `sdcm/rest/rest_client.py` -- add `requests.Session` with retry adapter
- `sdcm/rest/remote_curl_client.py` -- add curl-native `--retry` flags
- `sdcm/cluster.py` -- migrate `remoter.run("curl ...")` to utility
- `sdcm/utils/scylla_metrics_ctrl.py` -- migrate curl calls
- `sdcm/utils/adaptive_timeouts/load_info_store.py` -- migrate curl calls
- `sdcm/logcollector.py` -- route through session with retry
- `sdcm/prometheus.py` -- route through session with retry
- `sdcm/db_stats.py` -- route through session with retry
- `sdcm/kafka/kafka_cluster.py` -- route through session with retry
- `skills/code-review/SKILL.md` -- add HTTP resilience review check
- `AGENTS.md` -- add curl/requests conventions to code style guidelines
- `unit_tests/test_curl_utils.py` -- **new** -- tests for curl helper
- `unit_tests/test_rest_client.py` -- tests for retry adapter

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/test_curl_utils.py unit_tests/test_rest_client.py -v`
- [ ] `grep -rn 'remoter.run.*curl' sdcm/` shows zero bare curl calls (except documented exceptions)
- [ ] `RestClient` session has retry adapter mounted
- [ ] AI review skill mentions HTTP resilience check
- [ ] `uv run sct.py pre-commit` passes
