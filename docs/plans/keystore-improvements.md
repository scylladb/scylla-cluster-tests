# KeyStore Improvements Plan

## 1. Problem Statement

SCT stores all credentials and secrets in a single shared S3 bucket (`scylla-qa-keystore`) accessed via the `KeyStore` class. While functional, this approach has accumulated significant technical debt and operational limitations:

- **No caching**: Every `KeyStore()` call creates a new boto3 client and fetches credentials from S3, even when the same credential was fetched seconds ago. In a single test run, `KeyStore()` is instantiated 40+ times across modules, each making separate S3 API calls.
- **No access control**: All credentials live in one flat bucket with no per-team or per-role access restrictions. Anyone with AWS access to the bucket can read every credential.
- **No audit trail**: There is no logging or tracking of which credential was accessed, by whom, or when.
- **No automated rotation**: Keys and secrets are rotated manually with no enforcement or alerting for stale credentials.
- **Hardcoded bucket name**: `KEYSTORE_S3_BUCKET = "scylla-qa-keystore"` is a module-level constant, making it impossible to use different keystores for different environments (staging, production, development).
- **Shared SSH key**: All cloud backends (EC2, GCE, Azure, OCI) use the same `scylla_test_id_ed25519` key pair, returned by four identical methods.
- **No error handling strategy**: S3 failures surface as raw `ClientError` exceptions with no retry logic, graceful degradation, or actionable error messages.
- **No unit test coverage**: There are zero unit tests for `KeyStore` itself. The only test (`unit_tests/test_sync.py`) is an integration test that requires real AWS credentials.
- **Thread safety concerns**: While `BOTO3_CLIENT_CREATION_LOCK` protects client creation, the `s3` property (resource) has no lock, and concurrent `get_file_contents` calls could race.

## 2. Current State

### Core Implementation

**`sdcm/keystore.py`** (262 lines) contains:

- **`KeyStore` class** (lines 37-252): Stateless class with no `__init__`, no caching, no configuration. Each method call creates a new boto3 S3 resource/client and fetches directly from the `scylla-qa-keystore` bucket.
- **`KEYSTORE_S3_BUCKET`** (line 30): Hardcoded module-level constant `"scylla-qa-keystore"`.
- **`SSHKey`** (line 32): `namedtuple("SSHKey", ["name", "public_key", "private_key"])` used across provisioning.
- **`BOTO3_CLIENT_CREATION_LOCK`** (line 34): Threading lock for boto3 client creation (not resource creation).
- **`pub_key_from_private_key_file()`** (lines 255-261): Standalone utility for SSH public key extraction.

### Key Methods

| Method | S3 Key | Callers |
|--------|--------|---------|
| `get_file_contents(file_name)` | any | Base method used by all others |
| `get_json(json_file)` | any `.json` | Wrapper around `get_file_contents` + JSON parse |
| `get_ec2_ssh_key_pair()` | `scylla_test_id_ed25519[.pub]` | `sdcm/cluster.py`, `sdcm/sct_config.py`, provisioners |
| `get_gce_ssh_key_pair()` | same as EC2 | `sdcm/utils/gce_utils.py`, GCE provisioner |
| `get_azure_ssh_key_pair()` | same as EC2 | `sdcm/utils/azure_utils.py` |
| `get_oci_ssh_key_pair()` | same as EC2 | `sdcm/utils/oci_utils.py` |
| `get_gcp_credentials()` | `gcp-sct-project-1.json` | `sdcm/utils/gce_utils.py`, `sdcm/cluster_k8s/gke.py` |
| `get_azure_credentials()` | `azure.json` | `sdcm/utils/azure_utils.py` |
| `get_oci_credentials()` | `oci.json` | `sdcm/utils/oci_utils.py` (with local config fallback) |
| `get_docker_hub_credentials()` | `docker.json` | `sdcm/utils/docker_utils.py` |
| `get_email_credentials()` | `email_config.json` | `sdcm/send_email.py` |
| `get_ldap_ms_ad_credentials()` | `ldap_ms_ad.json` | `sdcm/cluster.py` (lines 2044, 4888) |
| `get_argus_rest_credentials_per_provider()` | `argus_rest_credentials[_sct_<provider>].json` | `sdcm/utils/argus.py` |
| `get_jira_credentials()` | `scylladb_jira.json` | `sdcm/utils/issues.py` |
| `get_housekeeping_db_credentials()` | `housekeeping-db.json` | `sdcm/utils/housekeeping.py` |
| `get_backup_azure_blob_credentials()` | `backup_azure_blob.json` | `sdcm/test_config.py` (line 184) |
| `get_azure_kms_config()` | `azure_kms_config.json` | `sdcm/provision/azure/kms_provider.py` |
| `get_gcp_kms_config()` | `gcp_kms_config.json` | `sdcm/provision/gce/kms_provider.py`, `sdcm/utils/gcp_kms.py` |
| `sync(keys, local_path, permissions)` | multiple keys | `sdcm/sct_config.py`, SSH key distribution |
| `get_obj_if_needed(key, local_path, permissions)` | any key | Called by `sync()`, uses ETag-based caching for **file downloads only** |

### Usage Patterns

**`KeyStore()` is instantiated at point-of-use**, not injected or shared:
- `sdcm/cluster.py:2044` — `KeyStore().get_ldap_ms_ad_credentials()`
- `sdcm/test_config.py:184` — `KeyStore().set_backup_azure_blob_credentials()` via `KeyStore().get_backup_azure_blob_credentials()`
- `sdcm/utils/argus.py` — `KeyStore().get_argus_rest_credentials_per_provider()`
- `sdcm/sct_config.py:45` — imports `KeyStore`, uses it during config validation
- 41 files total instantiate `KeyStore()` directly

**`sdcm/sct_provision/region_definition_builder.py:19`** imports both `KeyStore` and `SSHKey`, fetching SSH keys during region definition building.

### Testing

- **`unit_tests/test_sync.py`**: Single integration test (`@pytest.mark.integration`) that tests `KeyStore.sync()` with real S3. No unit tests exist.
- **`unit_tests/test_aws_services.py`**, **`unit_tests/provisioner/test_provisioner.py`**, **`unit_tests/provisioner/test_azure_region_definition_builder.py`**: Mock `KeyStore` methods using `patch.object` to avoid S3 calls, but don't test `KeyStore` itself.
- **`unit_tests/lib/fake_region_definition_builder.py`**: Imports `SSHKey` to create fake SSH keys for tests.

### Existing Caching (Partial)

`get_obj_if_needed()` (line 227) implements ETag-based caching for **file downloads to disk** via `sync()`. However, the more commonly used `get_file_contents()` / `get_json()` path has **zero caching** — every call goes to S3.

## 3. Goals

1. **Add in-memory caching** to eliminate redundant S3 calls within a test run — target: reduce S3 API calls by 80%+ for a typical test execution.
2. **Make the bucket name configurable** via environment variable, enabling separate keystores for different environments (staging, dev, production).
3. **Add structured logging** for credential access — log which credential was fetched, when, and by which module, enabling basic audit capability.
4. **Consolidate duplicate SSH key methods** into a single method, eliminating the four identical `get_*_ssh_key_pair()` wrappers.
5. **Add retry logic with exponential backoff** for transient S3 failures, reducing flaky test failures caused by temporary AWS issues.
6. **Achieve >90% unit test coverage** for the `KeyStore` class with proper mocking (no real S3 calls).
7. **Provide a singleton/shared instance pattern** so callers don't need to instantiate `KeyStore()` at every call site, while maintaining backward compatibility.

## 4. Implementation Phases

### Phase 1: Unit Tests for Existing Behavior

**Importance**: Critical — establishes a safety net before any refactoring.

**Description**: Write comprehensive unit tests for `KeyStore` using `moto` (mock AWS) and `unittest.mock`. Cover all public methods, error paths, and edge cases.

**Dependencies**: None

**Deliverables**:
- New file `unit_tests/test_keystore.py` with pytest-style tests
- Tests for: `get_file_contents`, `get_json`, all credential getters, `sync`, `get_obj_if_needed`, `calculate_s3_etag`, `_parse_local_oci_config`
- Error case tests: missing S3 key, invalid JSON, network timeout, permission denied
- Thread safety test for concurrent `get_file_contents` calls

**Definition of Done**:
- [ ] All tests pass with `uv run python -m pytest unit_tests/test_keystore.py -v`
- [ ] No real S3 calls (verified by no `@pytest.mark.integration`)
- [ ] >90% coverage of `sdcm/keystore.py`
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 2: Configurable Bucket Name

**Importance**: High — unblocks multi-environment usage without touching credential-fetching logic.

**Description**: Replace the hardcoded `KEYSTORE_S3_BUCKET` constant with a configurable value that reads from the `SCT_KEYSTORE_S3_BUCKET` environment variable, falling back to the current default.

**Dependencies**: Phase 1 (tests ensure no regressions)

**Deliverables**:
- `KEYSTORE_S3_BUCKET` reads from `os.environ.get("SCT_KEYSTORE_S3_BUCKET", "scylla-qa-keystore")`
- Update unit tests to verify the environment variable override
- Document the new environment variable in `AGENTS.md` (Environment Variables section)

**Definition of Done**:
- [ ] `SCT_KEYSTORE_S3_BUCKET=my-bucket` overrides the default
- [ ] Existing behavior unchanged when env var is not set
- [ ] Unit tests verify both paths
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 3: In-Memory Caching with TTL

**Importance**: High — the single most impactful performance improvement.

**Description**: Add a thread-safe in-memory cache to `get_file_contents()` with a configurable TTL (default: 5 minutes). Since `get_json()` and all credential getters call `get_file_contents()`, this provides caching across the entire API surface without changing any caller.

**Dependencies**: Phase 1

**Deliverables**:
- Thread-safe cache using `threading.Lock` and a dict of `{key: (value, expiry_time)}`
- `cache_ttl_seconds` parameter on `KeyStore.__init__()` (default 300)
- `clear_cache()` method for explicit invalidation
- `get_file_contents(file_name, bypass_cache=False)` parameter for force-refresh

**Definition of Done**:
- [ ] Repeated calls to same credential return cached value (verified by mock call count)
- [ ] Cache expires after TTL
- [ ] `bypass_cache=True` forces S3 fetch
- [ ] `clear_cache()` empties all cached entries
- [ ] Thread-safe under concurrent access (tested with `ThreadPoolExecutor`)
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 4: Structured Credential Access Logging

**Importance**: Medium — provides audit capability and aids debugging.

**Description**: Add `logging.getLogger(__name__)` to `KeyStore` and log each credential access with structured fields: credential name, caller module, cache hit/miss, fetch duration.

**Dependencies**: Phase 3 (cache hit/miss info available)

**Deliverables**:
- `LOGGER` at module level in `keystore.py`
- Log at `DEBUG` level for cache hits, `INFO` level for S3 fetches
- Include caller info via `inspect.stack()` or module parameter
- Log warnings for slow fetches (>2 seconds)

**Definition of Done**:
- [ ] `DEBUG` log for every cache hit: `"KeyStore cache hit for '%s' (caller: %s)"`
- [ ] `INFO` log for every S3 fetch: `"KeyStore fetched '%s' from S3 in %.2fs (caller: %s)"`
- [ ] `WARNING` log for slow fetches: `"KeyStore slow fetch for '%s': %.2fs"`
- [ ] No logging at `INFO` or higher for normal cache-hit operation (avoid log spam)
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 5: Consolidate SSH Key Methods and Add Retry Logic

**Importance**: Medium — reduces code duplication and improves resilience.

**Description**:

1. **SSH key consolidation**: Replace the four identical methods (`get_ec2_ssh_key_pair`, `get_gce_ssh_key_pair`, `get_azure_ssh_key_pair`, `get_oci_ssh_key_pair`) with a single `get_ssh_key_pair()` call. Keep the old methods as thin wrappers that call the new method for backward compatibility, and add deprecation warnings.

2. **Retry logic**: Add `@retrying` decorator (from `sdcm/utils/decorators.py`) or `tenacity` retry to `get_file_contents()` for transient S3 failures (`ClientError` with codes `SlowDown`, `InternalError`, `ServiceUnavailable`).

**Dependencies**: Phase 1

**Deliverables**:
- `get_ssh_key_pair()` becomes the canonical method (already exists, takes `name` param)
- `get_ec2_ssh_key_pair()` etc. become one-liners calling `get_ssh_key_pair("scylla_test_id_ed25519")` (already the case — add deprecation warning)
- Retry with exponential backoff on transient S3 errors (3 attempts, 1s/2s/4s delays)
- Unit tests for retry behavior using mocked S3 errors

**Definition of Done**:
- [ ] Deprecation warnings added to backend-specific SSH methods
- [ ] Transient S3 errors are retried up to 3 times
- [ ] Non-transient errors (NoSuchKey, AccessDenied) are raised immediately
- [ ] Unit tests verify retry behavior
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 6: Shared Instance Pattern

**Importance**: Low — quality-of-life improvement, reduces boilerplate across codebase.

**Description**: Add a module-level function `get_keystore()` that returns a shared `KeyStore` instance (using `threading.Lock` for thread-safe initialization). This avoids the pattern of `KeyStore()` instantiation at every call site. Existing `KeyStore()` direct instantiation continues to work.

**Dependencies**: Phases 2, 3 (configurable bucket and caching must be in place)

**Deliverables**:
- `get_keystore() -> KeyStore` function in `sdcm/keystore.py`
- Shared instance initialized lazily on first call
- Migration guide comment in the function docstring

**Definition of Done**:
- [ ] `get_keystore()` returns the same instance across calls
- [ ] Thread-safe initialization
- [ ] Direct `KeyStore()` instantiation still works (backward compatible)
- [ ] Unit tests verify singleton behavior
- [ ] Passes `uv run sct.py pre-commit`

**Note**: Migrating existing `KeyStore()` call sites to `get_keystore()` is intentionally deferred to a follow-up effort. This phase only introduces the function — callers can adopt it incrementally.

---

### Phase 7: Documentation Update

**Importance**: Medium — ensures the changes are discoverable and usable.

**Dependencies**: Phases 1-6

**Deliverables**:
- Update `AGENTS.md` environment variables section with `SCT_KEYSTORE_S3_BUCKET`
- Add docstrings to all `KeyStore` methods following Google format
- Update the keystore section in repository documentation
- Archive this plan to `docs/plans/archive/`

**Definition of Done**:
- [ ] All public methods have Google-format docstrings
- [ ] `SCT_KEYSTORE_S3_BUCKET` documented in `AGENTS.md`
- [ ] Passes `uv run sct.py pre-commit`

## 5. Testing Requirements

### Unit Tests (Phase 1, expanded in subsequent phases)

| Test Area | Method | What to Verify |
|-----------|--------|---------------|
| Basic fetch | `get_file_contents` | Returns S3 object body |
| JSON parse | `get_json` | Returns parsed dict from JSON |
| Missing key | `get_file_contents` | Raises `ClientError` with `NoSuchKey` |
| Invalid JSON | `get_json` | Raises `json.JSONDecodeError` |
| SSH key pair | `get_ssh_key_pair` | Returns `SSHKey` namedtuple with correct fields |
| OCI local config | `get_oci_credentials` | Falls back to local config when env var set |
| OCI local parse | `_parse_local_oci_config` | Parses INI config correctly |
| Argus per-provider | `get_argus_rest_credentials_per_provider` | Tries provider-specific first, falls back |
| ETag check | `get_obj_if_needed` | Skips download when ETag matches |
| Sync parallel | `sync` | Downloads all keys in parallel |
| S3 ETag calc | `calculate_s3_etag` | Correct single-part and multi-part ETags |
| Cache hit | `get_file_contents` (Phase 3) | Second call returns cached, no S3 |
| Cache TTL | `get_file_contents` (Phase 3) | Cache expires after TTL |
| Cache bypass | `get_file_contents(bypass_cache=True)` | Forces S3 fetch |
| Retry transient | `get_file_contents` (Phase 5) | Retries on `SlowDown`, succeeds |
| No retry permanent | `get_file_contents` (Phase 5) | No retry on `NoSuchKey` |
| Shared instance | `get_keystore()` (Phase 6) | Returns same instance |

### Integration Tests (Existing)

- `unit_tests/test_sync.py` — already tests `sync()` with real S3. No changes needed.

### Manual Testing

- Run a full SCT test with `--backend docker` and verify credentials are fetched and cached correctly via log output.
- Set `SCT_KEYSTORE_S3_BUCKET` to a non-existent bucket and verify clear error message.

## 6. Success Criteria

1. **S3 API call reduction**: A typical docker backend test run makes ≤5 S3 API calls (down from 40+), verified by counting `INFO`-level fetch logs.
2. **Unit test coverage**: `sdcm/keystore.py` has >90% line coverage as reported by `pytest --cov=sdcm.keystore`.
3. **No behavioral regressions**: All existing unit and integration tests pass without modification (except adding mocks where `KeyStore` was previously unmocked).
4. **Configurable bucket**: Setting `SCT_KEYSTORE_S3_BUCKET=my-bucket` correctly redirects all credential fetches.
5. **Audit logging**: Running with `--log-level=DEBUG` shows cache hit/miss logs for every credential access.
6. **Resilient fetches**: Transient S3 errors are retried transparently; `SlowDown` errors no longer cause test failures.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Cache serves stale credentials after rotation | Low | High | Default 5-min TTL is short enough for test runs; `bypass_cache=True` and `clear_cache()` available for force-refresh |
| Shared instance causes issues with parallel test execution | Medium | Medium | `get_keystore()` is optional; direct `KeyStore()` instantiation still works. Cache is per-instance. |
| Retry logic masks permanent failures | Low | Medium | Only retry transient error codes (`SlowDown`, `InternalError`, `ServiceUnavailable`); raise immediately for `NoSuchKey`, `AccessDenied` |
| Deprecation warnings for SSH methods are noisy | Low | Low | Use `warnings.warn(..., DeprecationWarning, stacklevel=2)` which is suppressed by default in production; visible in tests with `-W default` |
| Breaking change if callers depend on fresh S3 data per call | Low | Medium | Cache bypass parameter available; existing `KeyStore()` instantiation creates new instance with fresh cache |
| `moto` doesn't fully replicate S3 behavior | Medium | Low | Use `moto` for unit tests but keep the existing integration test in `test_sync.py` for real S3 validation |
