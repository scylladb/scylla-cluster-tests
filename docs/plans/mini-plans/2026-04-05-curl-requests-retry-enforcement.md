# Mini-Plan: Curl & Requests Retry Enforcement

**Date:** 2026-04-05
**Estimated LOC:** ~800
**Related PR:** #13509

## Problem

Curl commands and `requests` calls without retry/timeout cause intermittent test failures from transient network issues (e.g. PR #13499). ~107 curl invocations have ~30% retry compliance, and most `requests` calls across `sdcm/rest/rest_client.py`, `sdcm/logcollector.py`, `sdcm/prometheus.py`, `sdcm/db_stats.py` lack retry adapters entirely.

## Approach

1. **Shared session factory** -- create `sdcm/utils/session.py` with `create_retry_session()` that returns a `requests.Session` pre-configured with `HTTPAdapter(max_retries=Retry(...))`. All five existing `_create_session` duplicates (`rest_client.py`, `db_stats.py`, `prometheus.py`, `kafka_cluster.py`, `cloud_api_client.py`) delegate to this single implementation.

2. **Consolidate bash curl calls** -- `sdcm/utils/curl.py` helper enforces `--retry 5 --retry-max-time 300` by default, skips retry for localhost/metadata endpoints. Migrate all `remoter.run("curl ...")` calls to use it.

3. **Update AI review skill** -- add HTTP resilience check to `skills/code-review/SKILL.md` that flags bare `remoter.run("curl ...")` and bare `requests.get()`/`requests.post()` in PR reviews. Update `AGENTS.md` with the new conventions.

4. **Scan and fix remaining violations** -- grep for remaining bare curl/requests calls, migrate to utilities or add `--retry` flags directly (shell scripts). Document exceptions with `# no-retry: <reason>` comments.

## Files to Modify

- `sdcm/utils/session.py` -- **new** -- shared `create_retry_session()` factory
- `sdcm/utils/curl.py` -- **new** -- retry-by-default curl command builder
- `sdcm/rest/rest_client.py` -- delegate `_create_session` to shared factory
- `sdcm/rest/remote_curl_client.py` -- add curl-native `--retry` flags
- `sdcm/db_stats.py` -- delegate `_create_session` to shared factory
- `sdcm/prometheus.py` -- delegate `_create_session` to shared factory
- `sdcm/kafka/kafka_cluster.py` -- delegate `_create_session` to shared factory
- `sdcm/cloud_api_client.py` -- delegate `_create_session` to shared factory
- `sdcm/cluster.py` -- migrate `remoter.run("curl ...")` to utility
- `sdcm/utils/scylla_metrics_ctrl.py` -- migrate curl calls
- `sdcm/utils/adaptive_timeouts/load_info_store.py` -- migrate curl calls
- `sdcm/logcollector.py` -- route through session with retry
- `skills/code-review/SKILL.md` -- add HTTP resilience review check
- `AGENTS.md` -- add curl/requests conventions to code style guidelines
- `unit_tests/test_curl_utils.py` -- **new** -- tests for curl helper
- `unit_tests/unit/rest/test_rest_client.py` -- tests for retry adapter

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/test_curl_utils.py unit_tests/unit/rest/test_rest_client.py -v`
- [ ] `grep -rn 'remoter.run.*curl' sdcm/` shows zero bare curl calls (except documented exceptions)
- [ ] `RestClient` session has retry adapter mounted
- [ ] All five `_create_session` methods delegate to `create_retry_session()`
- [ ] AI review skill mentions HTTP resilience check
- [ ] `uv run sct.py pre-commit` passes
