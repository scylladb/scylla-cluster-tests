# KeyStore Improvements Plan

## 1. Problem Statement

SCT stores all credentials and secrets in a single shared S3 bucket (`scylla-qa-keystore`) accessed via the `KeyStore` class. While functional, this approach has accumulated significant technical debt and operational limitations:

- **No encryption at rest with managed keys**: S3 objects use default encryption, not a dedicated secrets management service with automatic KMS integration, per-secret access policies, and audit logging.
- **No caching**: Every `KeyStore()` call creates a new boto3 client and fetches credentials from S3, even when the same credential was fetched seconds ago. In a single test run, `KeyStore()` is instantiated 40+ times across modules, each making separate S3 API calls.
- **No access control**: All credentials live in one flat bucket with no per-team or per-role access restrictions. Anyone with AWS access to the bucket can read every credential.
- **No audit trail**: There is no logging or tracking of which credential was accessed, by whom, or when. S3 access logs are bucket-level, not per-object with caller identity.
- **No automated rotation**: Keys and secrets are rotated manually with no enforcement or alerting for stale credentials.
- **Hardcoded bucket name**: `KEYSTORE_S3_BUCKET = "scylla-qa-keystore"` is a module-level constant, making it impossible to use different keystores for different environments (staging, production, development).
- **Shared SSH key**: All cloud backends (EC2, GCE, Azure, OCI) use the same `scylla_test_id_ed25519` key pair, returned by four identical methods.
- **No error handling strategy**: S3 failures surface as raw `ClientError` exceptions with no retry logic, graceful degradation, or actionable error messages.
- **No unit test coverage**: There are zero unit tests for `KeyStore` itself. The only test (`unit_tests/test_sync.py`) is an integration test that requires real AWS credentials.
- **Thread safety concerns**: While `BOTO3_CLIENT_CREATION_LOCK` protects client creation, the `s3` property (resource) has no lock, and concurrent `get_file_contents` calls could race.

### Why AWS Secrets Manager

SCT operates across AWS, GCE, Azure, and OCI, but **all credential access originates from AWS** — Jenkins runs on AWS EC2, and developers authenticate via Okta-to-AWS. The KeyStore stores credentials *for* other clouds but is always accessed *from* AWS infrastructure with existing IAM roles. AWS Secrets Manager is the natural fit because:

- **Zero authentication changes**: Uses the same `boto3` SDK and IAM credentials SCT already has. No bootstrap problem (unlike GCP/Azure/Okta options that would require storing a credential to access the credential store).
- **$8/month cost**: 20 secrets × $0.40/secret/month. Negligible compared to daily cloud provisioning costs.
- **Per-secret IAM policies**: Each secret can have its own resource policy restricting access by IAM role, enabling team-scoped access control.
- **Native audit trail**: CloudTrail logs every `GetSecretValue` call with caller identity, timestamp, and source IP. GuardDuty can alert on anomalous access patterns.
- **Built-in rotation**: Lambda-based automatic rotation for supported credential types.
- **Same SDK**: Replace `boto3.client("s3").get_object()` with `boto3.client("secretsmanager").get_secret_value()` — minimal code change.
- **Jenkins integration**: All 20+ pipelines already inject `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` via Jenkins `credentials()`. Zero pipeline changes needed.

Alternatives evaluated and rejected:
- **GCP Secret Manager / Azure Key Vault**: Chicken-and-egg problem — need cloud credentials to access the store that holds cloud credentials.
- **Okta Privileged Access**: PAM-oriented, no Python SDK for secrets retrieval, requires separate API token bootstrap.
- **Doppler**: Env-var-only injection doesn't fit SCT's `get_json()` pattern; flat org structure.
- **Akeyless**: Enterprise pricing, opaque costs, overkill for ~20 static secrets.
- **HashiCorp Vault (self-hosted)**: Significant operational overhead to run a Vault cluster for ~20 secrets.
- **Infisical (self-hosted)**: Running another service (PostgreSQL + Redis + Docker) for ~20 secrets is disproportionate overhead.

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

### Secrets Inventory

Current S3 bucket contents that will be migrated to AWS Secrets Manager:

| Secret Name | Type | Format | Size Estimate |
|-------------|------|--------|---------------|
| `scylla_test_id_ed25519` | SSH private key | Binary (PEM) | ~500B |
| `scylla_test_id_ed25519.pub` | SSH public key | Text | ~100B |
| `gcp-sct-project-1.json` | GCP service account | JSON | ~2KB |
| `azure.json` | Azure credentials | JSON | ~500B |
| `oci.json` | OCI credentials | JSON | ~1KB |
| `docker.json` | Docker Hub credentials | JSON | ~200B |
| `email_config.json` | Email SMTP config | JSON | ~300B |
| `ldap_ms_ad.json` | LDAP/AD credentials | JSON | ~500B |
| `argus_rest_credentials.json` | Argus API credentials | JSON | ~300B |
| `scylladb_jira.json` | Jira API credentials | JSON | ~300B |
| `housekeeping-db.json` | Database credentials | JSON | ~300B |
| `backup_azure_blob.json` | Azure Blob credentials | JSON | ~300B |
| `azure_kms_config.json` | Azure KMS config | JSON | ~500B |
| `gcp_kms_config.json` | GCP KMS config | JSON | ~500B |
| ~6 additional JSON files | Various configs | JSON | ~300B each |

All secrets are well within the 64KB Secrets Manager limit.

## 3. Goals

1. **Migrate credential storage from S3 to AWS Secrets Manager** — all secrets stored in Secrets Manager with KMS encryption, per-secret IAM policies, and CloudTrail audit logging.
2. **Add in-memory caching** to eliminate redundant API calls within a test run — target: reduce Secrets Manager API calls by 80%+ for a typical test execution.
3. **Maintain backward compatibility** — existing `KeyStore()` API surface (all public methods) must continue to work unchanged. Callers should not need modification.
4. **Support S3 fallback during migration** — a configuration flag allows reading from S3 (old) or Secrets Manager (new) to enable gradual rollout without a big-bang cutover.
5. **Consolidate duplicate SSH key methods** into a single method, eliminating the four identical `get_*_ssh_key_pair()` wrappers.
6. **Add retry logic with exponential backoff** for transient AWS failures, reducing flaky test failures caused by temporary API throttling.
7. **Achieve >90% unit test coverage** for the `KeyStore` class with proper mocking (no real AWS calls).
8. **Provide a singleton/shared instance pattern** so callers don't need to instantiate `KeyStore()` at every call site, while maintaining backward compatibility.
9. **Add structured logging** for credential access — log which credential was fetched, when, and by which module, enabling audit capability beyond CloudTrail.

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

### Phase 2: Secrets Manager Backend with S3 Fallback

**Importance**: Critical — core migration to AWS Secrets Manager.

**Description**: Add an AWS Secrets Manager backend to `KeyStore` while keeping the S3 backend as a fallback. A configuration flag (`SCT_KEYSTORE_BACKEND`) controls which backend is used. This enables gradual rollout: teams can switch to Secrets Manager individually and roll back to S3 if issues arise.

**Dependencies**: Phase 1 (tests ensure no regressions)

**Deliverables**:
- New `_get_from_secrets_manager(secret_name)` method using `boto3.client("secretsmanager").get_secret_value()`
- `SCT_KEYSTORE_BACKEND` environment variable: `"s3"` (default, backward compatible) or `"secretsmanager"`
- `get_file_contents()` dispatches to the configured backend
- Binary secrets (SSH keys) stored as `SecretBinary`, JSON secrets stored as `SecretString`
- Secrets Manager secret names use a configurable prefix (default: `sct/keystore/`) to namespace secrets and avoid collisions, e.g. `sct/keystore/scylla_test_id_ed25519`
- `SCT_KEYSTORE_SM_PREFIX` environment variable for the prefix (default: `sct/keystore/`)
- Update unit tests to cover both backends

**Definition of Done**:
- [ ] `SCT_KEYSTORE_BACKEND=secretsmanager` reads from Secrets Manager
- [ ] `SCT_KEYSTORE_BACKEND=s3` (or unset) reads from S3 (existing behavior)
- [ ] Binary and JSON secrets round-trip correctly through Secrets Manager
- [ ] Unit tests cover both backends using `moto`'s `@mock_aws` for Secrets Manager
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 3: In-Memory Caching with TTL

**Importance**: High — the single most impactful performance improvement.

**Description**: Add a thread-safe in-memory cache to `get_file_contents()` with a configurable TTL (default: 5 minutes). Since `get_json()` and all credential getters call `get_file_contents()`, this provides caching across the entire API surface without changing any caller. The cache works identically for both S3 and Secrets Manager backends.

**Dependencies**: Phase 1

**Deliverables**:
- Thread-safe cache using `threading.Lock` and a dict of `{key: (value, expiry_time)}`
- `cache_ttl_seconds` parameter on `KeyStore.__init__()` (default 300)
- `clear_cache()` method for explicit invalidation
- `get_file_contents(file_name, bypass_cache=False)` parameter for force-refresh

**Definition of Done**:
- [ ] Repeated calls to same credential return cached value (verified by mock call count)
- [ ] Cache expires after TTL
- [ ] `bypass_cache=True` forces API fetch
- [ ] `clear_cache()` empties all cached entries
- [ ] Thread-safe under concurrent access (tested with `ThreadPoolExecutor`)
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 4: Structured Credential Access Logging

**Importance**: Medium — provides application-level audit capability complementing CloudTrail.

**Description**: Add `logging.getLogger(__name__)` to `KeyStore` and log each credential access with structured fields: credential name, caller module, cache hit/miss, fetch duration. CloudTrail provides infrastructure-level auditing; this provides application-level traceability.

**Dependencies**: Phase 3 (cache hit/miss info available)

**Deliverables**:
- `LOGGER` at module level in `keystore.py`
- Log at `DEBUG` level for cache hits, `INFO` level for API fetches
- Include caller info via `inspect.stack()` or module parameter
- Log warnings for slow fetches (>2 seconds)
- Log which backend was used (S3 vs Secrets Manager)

**Definition of Done**:
- [ ] `DEBUG` log for every cache hit: `"KeyStore cache hit for '%s' (caller: %s)"`
- [ ] `INFO` log for every API fetch: `"KeyStore fetched '%s' from %s in %.2fs (caller: %s)"`
- [ ] `WARNING` log for slow fetches: `"KeyStore slow fetch for '%s': %.2fs"`
- [ ] No logging at `INFO` or higher for normal cache-hit operation (avoid log spam)
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 5: Consolidate SSH Key Methods and Add Retry Logic

**Importance**: Medium — reduces code duplication and improves resilience.

**Description**:

1. **SSH key consolidation**: Replace the four identical methods (`get_ec2_ssh_key_pair`, `get_gce_ssh_key_pair`, `get_azure_ssh_key_pair`, `get_oci_ssh_key_pair`) with a single `get_ssh_key_pair()` call. Keep the old methods as thin wrappers that call the new method for backward compatibility, and add deprecation warnings.

2. **Retry logic**: Add `@retrying` decorator (from `sdcm/utils/decorators.py`) or `tenacity` retry to `get_file_contents()` for transient AWS failures. For Secrets Manager: `ThrottlingException`, `InternalServiceError`. For S3: `SlowDown`, `InternalError`, `ServiceUnavailable`.

**Dependencies**: Phase 1

**Deliverables**:
- `get_ssh_key_pair()` becomes the canonical method (already exists, takes `name` param)
- `get_ec2_ssh_key_pair()` etc. become one-liners calling `get_ssh_key_pair("scylla_test_id_ed25519")` (already the case — add deprecation warning)
- Retry with exponential backoff on transient errors (3 attempts, 1s/2s/4s delays)
- Unit tests for retry behavior using mocked errors

**Definition of Done**:
- [ ] Deprecation warnings added to backend-specific SSH methods
- [ ] Transient errors are retried up to 3 times
- [ ] Non-transient errors (`ResourceNotFoundException`, `NoSuchKey`, `AccessDenied`) are raised immediately
- [ ] Unit tests verify retry behavior
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 6: Shared Instance Pattern

**Importance**: Low — quality-of-life improvement, reduces boilerplate across codebase.

**Description**: Add a module-level function `get_keystore()` that returns a shared `KeyStore` instance (using `threading.Lock` for thread-safe initialization). This avoids the pattern of `KeyStore()` instantiation at every call site. Existing `KeyStore()` direct instantiation continues to work.

**Dependencies**: Phases 2, 3 (configurable backend and caching must be in place)

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

### Phase 7: Migration Script and Runbook

**Importance**: High — enables the actual cutover from S3 to Secrets Manager.

**Description**: Create a one-time migration script that reads all secrets from the S3 bucket and creates corresponding secrets in AWS Secrets Manager. Include a runbook documenting the migration procedure, rollback steps, and validation checks.

**Dependencies**: Phase 2 (Secrets Manager backend implemented)

**Deliverables**:
- Migration script `scripts/migrate_keystore_to_secrets_manager.py` that:
  - Lists all objects in the S3 keystore bucket
  - Creates each as a Secrets Manager secret (binary → `SecretBinary`, JSON → `SecretString`)
  - Applies the configurable prefix (`sct/keystore/`)
  - Validates round-trip: reads back each secret and compares with S3 original
  - Dry-run mode that only reports what would be created
  - Idempotent: skips secrets that already exist (with option to overwrite)
- IAM policy template (JSON) for Secrets Manager access, scoped to `sct/keystore/*` resource ARN
- Runbook in the PR description covering:
  1. Pre-migration: verify AWS credentials, dry-run the script
  2. Migration: run script, validate all secrets readable
  3. Cutover: set `SCT_KEYSTORE_BACKEND=secretsmanager` in Jenkins environment
  4. Validation: run a docker-backend test, verify credential access logs
  5. Rollback: unset `SCT_KEYSTORE_BACKEND` (reverts to S3)

**Definition of Done**:
- [ ] Migration script handles all secret types (binary and JSON)
- [ ] Dry-run mode works without creating any secrets
- [ ] Round-trip validation passes for all secrets
- [ ] IAM policy template restricts access to `sct/keystore/*`
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 8: Documentation Update

**Importance**: Medium — ensures the changes are discoverable and usable.

**Dependencies**: Phases 1-7

**Deliverables**:
- Update `AGENTS.md` environment variables section with:
  - `SCT_KEYSTORE_BACKEND` — Backend for credential storage (`s3` or `secretsmanager`)
  - `SCT_KEYSTORE_SM_PREFIX` — Secrets Manager secret name prefix (default: `sct/keystore/`)
- Add docstrings to all `KeyStore` methods following Google format
- Update the keystore section in repository documentation
- Archive this plan to `docs/plans/archive/`

**Definition of Done**:
- [ ] All public methods have Google-format docstrings
- [ ] New environment variables documented in `AGENTS.md`
- [ ] Passes `uv run sct.py pre-commit`

## 5. Testing Requirements

### Unit Tests (Phase 1, expanded in subsequent phases)

| Test Area | Method | What to Verify |
|-----------|--------|---------------|
| Basic fetch (S3) | `get_file_contents` | Returns S3 object body |
| Basic fetch (SM) | `get_file_contents` | Returns Secrets Manager `SecretString`/`SecretBinary` |
| JSON parse | `get_json` | Returns parsed dict from JSON |
| Missing key (S3) | `get_file_contents` | Raises `ClientError` with `NoSuchKey` |
| Missing secret (SM) | `get_file_contents` | Raises `ClientError` with `ResourceNotFoundException` |
| Invalid JSON | `get_json` | Raises `json.JSONDecodeError` |
| SSH key pair | `get_ssh_key_pair` | Returns `SSHKey` namedtuple with correct fields |
| OCI local config | `get_oci_credentials` | Falls back to local config when env var set |
| OCI local parse | `_parse_local_oci_config` | Parses INI config correctly |
| Argus per-provider | `get_argus_rest_credentials_per_provider` | Tries provider-specific first, falls back |
| ETag check | `get_obj_if_needed` | Skips download when ETag matches |
| Sync parallel | `sync` | Downloads all keys in parallel |
| S3 ETag calc | `calculate_s3_etag` | Correct single-part and multi-part ETags |
| Cache hit | `get_file_contents` (Phase 3) | Second call returns cached, no API call |
| Cache TTL | `get_file_contents` (Phase 3) | Cache expires after TTL |
| Cache bypass | `get_file_contents(bypass_cache=True)` | Forces API fetch |
| Retry transient (S3) | `get_file_contents` (Phase 5) | Retries on `SlowDown`, succeeds |
| Retry transient (SM) | `get_file_contents` (Phase 5) | Retries on `ThrottlingException`, succeeds |
| No retry permanent | `get_file_contents` (Phase 5) | No retry on `NoSuchKey`/`ResourceNotFoundException` |
| Shared instance | `get_keystore()` (Phase 6) | Returns same instance |
| Backend switching | `SCT_KEYSTORE_BACKEND` | S3 vs Secrets Manager dispatch |
| Migration script | `migrate_keystore_to_secrets_manager.py` | Dry-run, create, validate round-trip |

### Integration Tests (Existing)

- `unit_tests/test_sync.py` — already tests `sync()` with real S3. Remains valid for S3 backend. A new integration test for Secrets Manager backend is optional (CloudTrail validation is sufficient).

### Manual Testing

- Run a full SCT test with `--backend docker` and `SCT_KEYSTORE_BACKEND=secretsmanager` to verify credentials are fetched from Secrets Manager.
- Set `SCT_KEYSTORE_BACKEND=secretsmanager` with a non-existent secret prefix and verify clear error message.
- Run migration script in dry-run mode against the real S3 bucket to verify secret discovery.

## 6. Success Criteria

1. **Secrets Manager integration**: All credentials readable from AWS Secrets Manager with `SCT_KEYSTORE_BACKEND=secretsmanager`, with KMS encryption and CloudTrail audit logging.
2. **S3 fallback**: Setting `SCT_KEYSTORE_BACKEND=s3` (or leaving it unset) continues to use the existing S3 bucket — zero disruption during migration.
3. **API call reduction**: A typical docker backend test run makes ≤5 API calls (down from 40+), verified by counting `INFO`-level fetch logs.
4. **Unit test coverage**: `sdcm/keystore.py` has >90% line coverage as reported by `pytest --cov=sdcm.keystore`.
5. **No behavioral regressions**: All existing unit and integration tests pass without modification (except adding mocks where `KeyStore` was previously unmocked).
6. **Audit logging**: CloudTrail records every `GetSecretValue` call. Application logs show cache hit/miss for every credential access.
7. **Resilient fetches**: Transient AWS errors are retried transparently; `ThrottlingException`/`SlowDown` errors no longer cause test failures.
8. **Migration tooling**: One-command migration from S3 to Secrets Manager with dry-run, validation, and rollback capability.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Secrets Manager API throttling under high concurrency | Medium | Medium | In-memory caching (Phase 3) reduces API calls by 80%+. Default Secrets Manager quota is 10,000 requests/second — far above SCT's needs. Retry logic (Phase 5) handles transient throttling. |
| Cache serves stale credentials after rotation | Low | High | Default 5-min TTL is short enough for test runs; `bypass_cache=True` and `clear_cache()` available for force-refresh. Secrets Manager versioning allows explicit version pinning if needed. |
| Migration script misses secrets in S3 bucket | Low | High | Script lists all objects in bucket. Dry-run mode shows what will be migrated. Round-trip validation compares original and migrated values. |
| Cost increase from Secrets Manager API calls | Low | Low | $0.05 per 10,000 API calls. With caching, a test run makes ~5 calls. Even 1,000 test runs/month = $0.025. Secret storage: 20 × $0.40 = $8/month. |
| Shared instance causes issues with parallel test execution | Medium | Medium | `get_keystore()` is optional; direct `KeyStore()` instantiation still works. Cache is per-instance. |
| Breaking change if callers depend on fresh S3 data per call | Low | Medium | Cache bypass parameter available; existing `KeyStore()` instantiation creates new instance with fresh cache. |
| S3 `sync()` method doesn't apply to Secrets Manager | Low | Low | `sync()` and `get_obj_if_needed()` remain S3-only (they download files to disk for SSH key distribution). These methods bypass the backend dispatch and always use S3 directly, since their purpose is local file caching, not credential retrieval. |
| Retry logic masks permanent failures | Low | Medium | Only retry transient error codes (`ThrottlingException`, `InternalServiceError`, `SlowDown`); raise immediately for `ResourceNotFoundException`, `AccessDenied` |
| `moto` doesn't fully replicate Secrets Manager behavior | Medium | Low | Use `moto` for unit tests but validate with real Secrets Manager via migration script's round-trip check |
| Deprecation warnings for SSH methods are noisy | Low | Low | Use `warnings.warn(..., DeprecationWarning, stacklevel=2)` which is suppressed by default in production; visible in tests with `-W default` |
