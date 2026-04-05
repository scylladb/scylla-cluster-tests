---
status: draft
domain: framework
created: 2026-03-15
last_updated: 2026-04-05
owner: fruch
---
# KeyStore Improvements Plan

## 1. Problem Statement

SCT stores all credentials and secrets in a single shared S3 bucket (`scylla-qa-keystore`) accessed via the `KeyStore` class. While functional, this approach has accumulated significant technical debt and operational limitations:

- **No encryption at rest with managed keys**: S3 objects use default encryption, not a dedicated secrets management service with automatic KMS integration, per-secret access policies, and audit logging.
- **No access control**: All credentials live in one flat bucket with no per-team or per-role access restrictions. Anyone with AWS access to the bucket can read every credential.
- **No audit trail**: There is no logging or tracking of which credential was accessed, by whom, or when. S3 access logs are bucket-level, not per-object with caller identity.
- **No automated rotation**: Keys and secrets are rotated manually with no enforcement or alerting for stale credentials.
- **Hardcoded bucket name**: `KEYSTORE_S3_BUCKET = "scylla-qa-keystore"` is a module-level constant, making it impossible to use different keystores for different environments (staging, production, development).
- **Shared SSH key**: All cloud backends (EC2, GCE, Azure, OCI) use the same `scylla_test_id_ed25519` key pair, returned by four identical methods.
- **No error handling strategy**: S3 failures surface as raw `ClientError` exceptions with no retry logic, graceful degradation, or actionable error messages.
- **No unit test coverage**: There are zero unit tests for `KeyStore` itself. The only test (`unit_tests/test_sync.py`) is an integration test that requires real AWS credentials.
- **Thread safety concerns**: While `BOTO3_CLIENT_CREATION_LOCK` protects client creation, the `s3` property (resource) has no lock, and concurrent `get_file_contents` calls could race.
- **No caching**: Every `KeyStore()` call creates a new boto3 client and fetches credentials from S3, even when the same credential was fetched seconds ago. In a single test run, `KeyStore()` is instantiated 40+ times across modules, each making separate S3 API calls.

### Architectural Context: Hub-and-Spoke Secrets Management

This plan covers **SCT's role as a consumer** within the organization's Hub-and-Spoke secrets architecture:

- **Hub** (managed by infra team): Centrally manages credential generation, automated rotation across AWS/GCP/Azure/OCI, and pushes updated values to AWS Secrets Manager. The Hub handles the overlapping secrets (grace period) pattern — minting new keys while leaving old ones active for a configurable period (e.g., 7 days) to avoid breaking long-running SCT tests.
- **Spoke** (AWS Secrets Manager): The delivery layer. Stores all secrets with strict prefixing (`sct/`, `jenkins/`, `dtest/`) and IAM policies that scope access per consumer.
- **Consumer** (SCT — this plan): Fetches secrets dynamically at runtime. Never stores credentials in its internal databases or local filesystems beyond the test run. Never manages rotation — that is the Hub's responsibility.

### Why AWS Secrets Manager

SCT runs on multiple cloud platforms (AWS, GCE, Azure) and fetches multi-cloud credentials from AWS Secrets Manager. On GCP-hosted runners, authentication uses **GCP Workload Identity Federation (WIF)**: the GCP Service Account attached to the SCT runner is mapped to an AWS IAM Role, enabling secretless cross-cloud access with no static AWS credentials on disk. On other platforms, existing AWS credential mechanisms (profiles, environment variables) are used.

AWS Secrets Manager is the organizational standard for the delivery layer because:

- **Secretless authentication from GCP**: WIF eliminates the "chicken-and-egg" problem — no static AWS credentials needed on GCP-hosted runners.
- **$8/month cost**: ~20 secrets x $0.40/secret/month. Negligible compared to daily cloud provisioning costs.
- **Per-secret IAM policies**: SCT's IAM role is scoped to `sct/*` secrets only. It cannot access `jenkins/*` or `dtest/*` secrets.
- **Native audit trail**: CloudTrail logs every `GetSecretValue` call with caller identity, timestamp, and source IP.
- **High availability**: If the Hub goes down for maintenance, SCT continues to run using the currently valid secrets in Secrets Manager.
- **Same SDK**: Replace `boto3.client("s3").get_object()` with `boto3.client("secretsmanager").get_secret_value()` — minimal code change.
- **Grace period for long-running tests**: The Hub's overlapping secrets pattern ensures that multi-day SCT test runs are never broken by mid-flight credential revocation.

Alternatives evaluated and rejected:
- **GCP Secret Manager / Azure Key Vault**: Does not align with the organizational Hub-and-Spoke architecture where AWS SM is the delivery layer.
- **Okta Privileged Access**: PAM-oriented, no Python SDK for secrets retrieval, requires separate API token bootstrap.
- **Doppler**: Env-var-only injection doesn't fit SCT's `get_json()` pattern; flat org structure.
- **Akeyless**: Enterprise pricing, opaque costs, overkill for ~20 static secrets.
- **HashiCorp Vault / Infisical**: Being evaluated as Hub options by infra team. SCT is agnostic to the Hub implementation — it only reads from AWS SM.

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

1. **Migrate credential retrieval from S3 to AWS Secrets Manager** — SCT reads all secrets from Secrets Manager (populated and rotated by the central Hub) with KMS encryption, per-consumer IAM policies, and CloudTrail audit logging.
2. **Add `cached_property` caching** to eliminate redundant API calls within a test run — target: reduce API calls by 80%+ for a typical test execution. Cache persists for the `KeyStore` instance lifetime using Python's `functools.cached_property`.
3. **Maintain backward compatibility** — existing `KeyStore()` API surface (all public methods) must continue to work unchanged. Callers should not need modification.
4. **Support S3 fallback during migration** — a configuration flag allows reading from S3 (old) or Secrets Manager (new) to enable gradual rollout without a big-bang cutover.
5. **Consolidate duplicate SSH key methods** into a single method, eliminating the four identical `get_*_ssh_key_pair()` wrappers.
6. **Add retry logic with exponential backoff** for transient AWS failures, reducing flaky test failures caused by temporary API throttling.
7. **Achieve >90% unit test coverage** for the `KeyStore` class with proper mocking (no real AWS calls).
8. **Provide a singleton/shared instance pattern** so callers don't need to instantiate `KeyStore()` at every call site, while maintaining backward compatibility.
9. **Add structured logging** for credential access — log which credential was fetched, when, and by which module, enabling audit capability beyond CloudTrail.
10. **Define rotation requirements for the Hub** — tiered rotation frequencies (90/180/365 days) based on credential sensitivity, aligned with NIST 800-53 and PCI DSS standards, with a 7-day grace period for long-running SCT tests.
11. **Document all secrets** with standardized tags in Secrets Manager and detailed knowledge base pages in Confluence, covering recreation procedures, access scope, and dependency mapping.

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

### Phase 2: Interface Consolidation (SSH Key Methods)

**Importance**: High — cleans up the API surface before changing the backend implementation.

**Description**: Consolidate the duplicate SSH key methods and clean up the `KeyStore` interface. The principle is: change the interface first, then change the implementation. This makes the subsequent backend migration (Phase 5) simpler because there are fewer methods to adapt.

The four identical methods (`get_ec2_ssh_key_pair`, `get_gce_ssh_key_pair`, `get_azure_ssh_key_pair`, `get_oci_ssh_key_pair`) already delegate to `get_ssh_key_pair("scylla_test_id_ed25519")`. Add deprecation warnings to guide callers toward the canonical `get_ssh_key_pair()` method.

**Dependencies**: Phase 1 (tests ensure no regressions)

**Deliverables**:
- Deprecation warnings on `get_ec2_ssh_key_pair()`, `get_gce_ssh_key_pair()`, `get_azure_ssh_key_pair()`, `get_oci_ssh_key_pair()` using `warnings.warn(..., DeprecationWarning, stacklevel=2)`
- Update callers where straightforward to use `get_ssh_key_pair()` directly
- Unit tests verifying deprecation warnings are emitted
- Unit tests verifying `get_ssh_key_pair()` returns identical results

**Definition of Done**:
- [ ] Deprecation warnings emitted when backend-specific SSH methods are called
- [ ] `get_ssh_key_pair()` is the documented canonical method
- [ ] All existing tests pass without modification
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 3: Simple Caching with Instance-Level Dict

**Importance**: High — delivers immediate performance value independent of backend migration.

**Description**: Add caching to eliminate redundant API calls within a test run. Since `get_file_contents(file_name)` takes a parameter, use a thread-safe `dict` keyed by file name rather than `cached_property` (which only works for parameterless properties). This is simpler to implement and maintain than a TTL-based cache. Credentials do not change during a test run, so expiration adds complexity with no benefit.

Since `get_json()` and all credential getters call `get_file_contents()`, caching at the `get_file_contents()` level provides caching across the entire API surface without changing any caller.

Also add `tenacity` retry to `get_file_contents()` for transient AWS failures. For S3: `SlowDown`, `InternalError`, `ServiceUnavailable`. For Secrets Manager (when enabled later): `ThrottlingException`, `InternalServiceError`. Retry with exponential backoff (3 attempts, 1s/2s/4s delays). Non-transient errors (`NoSuchKey`, `ResourceNotFoundException`, `AccessDenied`) are raised immediately.

**Note on credential refresh/rotation**: Cache lives for the `KeyStore` instance lifetime. Periodic credential refresh between test runs is handled naturally — each test run creates a new `KeyStore` instance with a fresh cache. For the rare case of mid-run refresh, a `clear_cache()` method is available. A formal process for periodic credential rotation cadence should be planned as part of the Hub team coordination (see Section 8).

**Dependencies**: Phase 1 (tests ensure no regressions), Phase 2 (clean interface)

**Deliverables**:
- Thread-safe cache in `get_file_contents()` using `threading.Lock` and `dict`
- Cache lives as long as the `KeyStore` instance (no TTL)
- `clear_cache()` method and `bypass_cache=False` parameter on `get_file_contents()`
- `tenacity` retry decorator on `get_file_contents()` with transient error classification
- Unit tests for caching behavior and retry logic

**Definition of Done**:
- [ ] Repeated calls to same credential return cached value (verified by mock call count)
- [ ] `bypass_cache=True` forces API fetch
- [ ] `clear_cache()` empties all cached entries
- [ ] Thread-safe under concurrent access (tested with `ThreadPoolExecutor`)
- [ ] Transient errors are retried up to 3 times with exponential backoff
- [ ] Non-transient errors (`NoSuchKey`, `AccessDenied`) are raised immediately
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 4: Migration Script — Populate Secrets Manager

**Importance**: Critical — secrets must be populated before switching the backend.

**Description**: Create a migration script that populates AWS Secrets Manager with all secrets from the S3 bucket, and a validation script that verifies all secrets are present and readable. The migration must happen BEFORE switching the backend (Phase 5) — without all secrets populated in Secrets Manager, the new backend cannot be used.

The actual ongoing secret management is performed by the infra team (or the Hub's migration tooling) — this script handles the initial one-time population and provides ongoing validation.

**Dependencies**: Phase 1 (tests), infra team has set up AWS Secrets Manager and IAM policies

**Deliverables**:
- Migration script `scripts/migrate_keystore_to_sm.py` that:
  - Reads all secrets from the S3 bucket (`scylla-qa-keystore`)
  - Writes each to Secrets Manager with the `sct/` prefix
  - Binary secrets (SSH keys) stored as `SecretBinary`, JSON secrets stored as `SecretString`
  - Applies the standard tag schema (see Section 9)
  - Reports success/failure per secret
- Validation script `scripts/validate_keystore_secrets.py` that:
  - Reads the list of expected secrets from a manifest (derived from the Secrets Inventory in Section 2)
  - Attempts to fetch each secret via `boto3.client("secretsmanager").get_secret_value()`
  - Verifies binary secrets (SSH keys) are decodable and JSON secrets are parseable
  - Optionally compares against S3 originals to confirm content matches
  - Reports pass/fail per secret with clear error messages
  - Exit code 0 only if all secrets are readable
- IAM policy template (JSON) for SCT's AWS role, scoped to read-only on `sct/*` secrets:
  - `secretsmanager:GetSecretValue` and `secretsmanager:DescribeSecret` on `arn:aws:secretsmanager:*:*:secret:sct/*`
  - No write, delete, or rotation permissions (those belong to the Hub)

**Definition of Done**:
- [ ] Migration script successfully populates all secrets from S3 to Secrets Manager
- [ ] Validation script checks all expected secrets from the manifest
- [ ] Clear error reporting for missing or malformed secrets
- [ ] IAM policy template grants read-only access to `sct/*` only
- [ ] All secrets verified readable after migration
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 5: Secrets Manager Backend with S3 Fallback

**Importance**: Critical — core migration to AWS Secrets Manager.

**Description**: Add an AWS Secrets Manager backend to `KeyStore` while keeping the S3 backend as a fallback. A configuration flag (`SCT_KEYSTORE_BACKEND`) controls which backend is used. This enables gradual rollout: teams can switch to Secrets Manager individually and roll back to S3 if issues arise.

This phase depends on Phase 4 having already populated all secrets in Secrets Manager. Without all secrets present, the new backend would fail on first use.

Authentication to AWS Secrets Manager uses the standard boto3 credential chain. On GCP-hosted runners, this is provided via **Workload Identity Federation (WIF)**: the GCP Service Account assumes an AWS IAM Role scoped to `sct/*` secrets. On developer machines, existing AWS profiles (via Okta) continue to work. No SCT code changes are needed for authentication — boto3 handles it transparently.

The `sync()` and `get_obj_if_needed()` methods (used for SSH key distribution to disk) must also support Secrets Manager. When the backend is `secretsmanager`, `sync()` fetches secrets via `get_file_contents()` (which dispatches to Secrets Manager) and writes them to disk, replacing the S3-based download path. ETag-based caching is not available for Secrets Manager, so `sync()` always writes the file when using this backend.

**Dependencies**: Phase 3 (caching and retry apply to both backends), Phase 4 (secrets must already be populated)

**Deliverables**:
- New `_get_from_secrets_manager(secret_name)` method using `boto3.client("secretsmanager").get_secret_value()`
- `SCT_KEYSTORE_BACKEND` environment variable: `"s3"` (default, backward compatible) or `"secretsmanager"`
- `get_file_contents()` dispatches to the configured backend
- Secrets Manager secret names use a configurable prefix (default: `sct/`) to namespace secrets and avoid collisions, e.g. `sct/scylla_test_id_ed25519`
- `SCT_KEYSTORE_SM_PREFIX` environment variable for the prefix (default: `sct/`)
- `sync()` updated to work with both backends — fetches from Secrets Manager when configured, writes to disk as before
- Cutover runbook in the PR description covering:
  1. Pre-cutover: run validation script (Phase 4) to confirm all `sct/*` secrets are populated
  2. Cutover: set `SCT_KEYSTORE_BACKEND=secretsmanager` in the SCT runner environment
  3. Validation: run a docker-backend test, verify credential access logs
  4. Rollback: unset `SCT_KEYSTORE_BACKEND` (reverts to S3)
- Update unit tests to cover both backends

**Definition of Done**:
- [ ] `SCT_KEYSTORE_BACKEND=secretsmanager` reads from Secrets Manager
- [ ] `SCT_KEYSTORE_BACKEND=s3` (or unset) reads from S3 (existing behavior)
- [ ] Binary and JSON secrets round-trip correctly through Secrets Manager
- [ ] `sync()` writes SSH keys to disk correctly from both backends
- [ ] Caching and retry logic work identically for both backends
- [ ] Unit tests cover both backends using `moto`'s `@mock_aws` for Secrets Manager
- [ ] Cutover runbook includes rollback procedure
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 6: Access Logging

**Importance**: Medium — enables audit capability beyond CloudTrail.

**Description**: Add structured logging for credential access. Add `logging.getLogger(__name__)` and log each credential access with: credential name, caller module, cache hit/miss, fetch duration, backend used. `DEBUG` for cache hits, `INFO` for API fetches, `WARNING` for slow fetches (>2 seconds).

**Dependencies**: Phase 3 (caching), Phase 5 (backend switching)

**Deliverables**:
- `LOGGER` with structured log messages at appropriate levels
- Unit tests verifying log output at correct levels

**Definition of Done**:
- [ ] `DEBUG` log for cache hits, `INFO` for API fetches, `WARNING` for slow fetches
- [ ] Log messages include credential name, backend, and cache hit/miss
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 7: Shared Instance Pattern

**Importance**: Medium — reduces redundant instantiation across 41 call sites.

**Description**: Add a module-level `get_keystore()` function that returns a shared `KeyStore` instance (lazy, thread-safe initialization). When callers share a single instance, the cache is shared too — maximizing cache hits. Existing `KeyStore()` direct instantiation continues to work. Migrating call sites is deferred to a follow-up effort.

**Dependencies**: Phase 3 (caching must be in place for shared instance to be useful)

**Deliverables**:
- `get_keystore() -> KeyStore` module-level function (lazy singleton)
- Thread-safe initialization using `threading.Lock`
- Unit tests for singleton behavior

**Definition of Done**:
- [ ] `get_keystore()` returns the same instance across calls
- [ ] Thread-safe under concurrent access
- [ ] Existing `KeyStore()` direct instantiation still works
- [ ] Passes `uv run sct.py pre-commit`

---

### Phase 8: Documentation Update

**Importance**: Medium — ensures the changes are discoverable and usable.

**Dependencies**: Phases 1-7

**Deliverables**:
- Update `AGENTS.md` environment variables section with:
  - `SCT_KEYSTORE_BACKEND` — Backend for credential storage (`s3` or `secretsmanager`)
  - `SCT_KEYSTORE_SM_PREFIX` — Secrets Manager secret name prefix (default: `sct/`)
- Add docstrings to all `KeyStore` methods following Google format
- Update the keystore section in repository documentation
- Create initial Confluence page for secret knowledge base (see Section 9)
- Archive this plan to `docs/plans/archive/`

**Definition of Done**:
- [ ] All public methods have Google-format docstrings
- [ ] New environment variables documented in `AGENTS.md`
- [ ] Confluence page created with secret knowledge base
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
| Deprecation warnings | `get_ec2_ssh_key_pair` etc. (Phase 2) | Emits `DeprecationWarning` |
| Cache hit | `get_file_contents` (Phase 3) | Second call returns cached, no API call |
| Cache persists | `get_file_contents` (Phase 3) | Cache lives for instance lifetime |
| Cache bypass | `get_file_contents(bypass_cache=True)` (Phase 3) | Forces API fetch |
| Retry transient (S3) | `get_file_contents` (Phase 3) | Retries on `SlowDown`, succeeds |
| Retry transient (SM) | `get_file_contents` (Phase 5) | Retries on `ThrottlingException`, succeeds |
| No retry permanent | `get_file_contents` (Phase 3) | No retry on `NoSuchKey`/`ResourceNotFoundException` |
| Migration script | `migrate_keystore_to_sm.py` (Phase 4) | All secrets populated in Secrets Manager |
| Validation script | `validate_keystore_secrets.py` (Phase 4) | All expected secrets readable, clear error on missing |
| Backend switching | `SCT_KEYSTORE_BACKEND` (Phase 5) | S3 vs Secrets Manager dispatch |
| Sync with SM backend | `sync()` (Phase 5) | Writes SSH keys to disk from Secrets Manager |
| Access logging | `get_file_contents` (Phase 6) | Correct log levels for cache hit/miss/slow |
| Shared instance | `get_keystore()` (Phase 7) | Returns same instance |

### Integration Tests (Existing)

- `unit_tests/test_sync.py` — already tests `sync()` with real S3. Remains valid for S3 backend. A new integration test for Secrets Manager backend is optional (CloudTrail validation is sufficient).

### Manual Testing

- Run a full SCT test with `--backend docker` and `SCT_KEYSTORE_BACKEND=secretsmanager` to verify credentials are fetched from Secrets Manager.
- Set `SCT_KEYSTORE_BACKEND=secretsmanager` with a non-existent secret prefix and verify clear error message.
- Run validation script against real Secrets Manager to confirm all `sct/*` secrets are present.
- Test WIF authentication from a GCP-hosted runner by running the validation script without static AWS credentials.

## 6. Success Criteria

1. **Secrets Manager integration**: All credentials readable from AWS Secrets Manager with `SCT_KEYSTORE_BACKEND=secretsmanager`, with KMS encryption and CloudTrail audit logging.
2. **S3 fallback**: Setting `SCT_KEYSTORE_BACKEND=s3` (or leaving it unset) continues to use the existing S3 bucket — zero disruption during migration.
3. **API call reduction**: A typical docker backend test run makes <=5 API calls (down from 40+), verified by counting `INFO`-level fetch logs.
4. **Unit test coverage**: `sdcm/keystore.py` has >90% line coverage as reported by `pytest --cov=sdcm.keystore`.
5. **No behavioral regressions**: All existing unit and integration tests pass without modification (except adding mocks where `KeyStore` was previously unmocked).
6. **Audit logging**: CloudTrail records every `GetSecretValue` call. Application logs show cache hit/miss for every credential access.
7. **Resilient fetches**: Transient AWS errors are retried transparently; `ThrottlingException`/`SlowDown` errors no longer cause test failures.
8. **Validation tooling**: Validation script confirms all expected `sct/*` secrets are present and readable in Secrets Manager. Cutover runbook with rollback procedure documented.
9. **Secret metadata**: Every secret in Secrets Manager has the full standard tag schema applied. Every secret has a corresponding Confluence knowledge base page.
10. **Rotation policy**: Documented rotation schedule with tiered frequencies. `rotation_due` tags set on all secrets. Process documented and tested.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Secrets Manager API throttling under high concurrency | Medium | Medium | In-memory caching (Phase 3) reduces API calls by 80%+. Default Secrets Manager quota is 10,000 requests/second — far above SCT's needs. Retry logic (Phase 3) handles transient throttling. |
| Cache serves stale credentials after secret refresh | Low | Low | Credentials do not change during a test run. Cache lives for the `KeyStore` instance lifetime. For the rare case of mid-run refresh: `bypass_cache=True` and `clear_cache()` are available. Between runs, a new `KeyStore` instance starts with a fresh cache. |
| Hub fails to populate all SCT secrets | Low | High | Validation script (Phase 4) checks all expected secrets from a manifest before cutover. CI can run this periodically to detect missing secrets early. |
| Cost increase from Secrets Manager API calls | Low | Low | $0.05 per 10,000 API calls. With caching, a test run makes ~5 calls. Even 1,000 test runs/month = $0.025. Secret storage: 20 x $0.40 = $8/month. |
| Shared instance causes issues with parallel test execution | Medium | Medium | `get_keystore()` is optional; direct `KeyStore()` instantiation still works. Cache is per-instance. |
| Breaking change if callers depend on fresh S3 data per call | Low | Medium | Cache bypass parameter available; existing `KeyStore()` instantiation creates new instance with fresh cache. |
| `sync()` loses ETag caching with Secrets Manager | Low | Low | When backend is `secretsmanager`, `sync()` always writes the file (no ETag available). This is acceptable because `sync()` runs once at test startup and the files are small (<2KB each). The in-memory cache in `get_file_contents()` still prevents redundant API calls. |
| Retry logic masks permanent failures | Low | Medium | Only retry transient error codes (`ThrottlingException`, `InternalServiceError`, `SlowDown`); raise immediately for `ResourceNotFoundException`, `AccessDenied` |
| `moto` doesn't fully replicate Secrets Manager behavior | Medium | Low | Use `moto` for unit tests but validate with real Secrets Manager via migration script's round-trip check |
| Deprecation warnings for SSH methods are noisy | Low | Low | Use `warnings.warn(..., DeprecationWarning, stacklevel=2)` which is suppressed by default in production; visible in tests with `-W default` |
| Secret metadata becomes stale in Confluence/tags | Medium | Medium | Schedule quarterly review as part of rotation process (see Section 8). Assign owner per-secret who is responsible for keeping metadata current. |
| GCP WIF to AWS IAM role trust fails | Low | High | S3 fallback (`SCT_KEYSTORE_BACKEND=s3`) remains available until WIF is proven stable. Developers can still use Okta-to-AWS profiles locally. WIF configuration is managed by infra team. |
| Hub rotates a secret and SCT validation fails | Low | High | Grace period (7 days) means old credential is still active. SCT can continue using old credential while the issue is investigated. Validation script alerts SCT team immediately after rotation. |

## 8. Key Refresh and Rotation

### Ownership

**SCT does not own secret rotation.** The central Hub (managed by the infra team) is responsible for generating new credentials, pushing them to AWS Secrets Manager, and revoking old ones. This section documents:
1. The rotation schedule that SCT's secrets should follow (communicated to the Hub team).
2. SCT's requirements from the rotation process (grace period, no mid-flight breakage).
3. How SCT validates that rotated secrets work.

### Industry Standards

| Standard | Recommended Rotation Frequency | Notes |
|----------|-------------------------------|-------|
| **NIST 800-53 Rev. 5** | Based on risk assessment; 90 days is common baseline | Mandates rotation schedules exist; frequency depends on sensitivity |
| **PCI DSS v4.0.1** | 90 days for passwords/keys | Sections 8.3.9, 8.6.3 |
| **CIS Benchmarks** | 90 days | AWS Foundational Security Best Practices |
| **SOC 2 Type II** | Requires documented rotation policy | Frequency based on organizational risk assessment |
| **AWS Security Hub** | Flags secrets not rotated within 90 days | Default control: `SecretsManager.4` |

### Recommended Rotation Schedule for SCT Secrets

Not all secrets carry equal risk. Rotation frequency should match the sensitivity and blast radius of each credential. These tiers should be communicated to the Hub team and configured in their rotation engine:

| Tier | Rotation Frequency | Secrets | Rationale |
|------|-------------------|---------|-----------|
| **Tier 1 — High sensitivity** | **90 days** | Cloud credentials (`azure.json`, `gcp-sct-project-1.json`, `oci.json`), SSH keys (`scylla_test_id_ed25519`), Docker Hub (`docker.json`) | Direct access to cloud infrastructure or container registries. Compromise enables resource provisioning, data access, or supply chain attacks. |
| **Tier 2 — Medium sensitivity** | **180 days** | API credentials (`argus_rest_credentials.json`, `scylladb_jira.json`, `scylladb_upload.json`), database credentials (`housekeeping-db.json`), KMS configs (`azure_kms_config.json`, `gcp_kms_config.json`) | Access to internal services. Compromise is contained within SCT's operational scope. |
| **Tier 3 — Low sensitivity** | **365 days** | Email config (`email_config.json`), LDAP config (`ldap_ms_ad.json`), QA users (`qa_users.json`), backup blob (`backup_azure_blob.json`) | Read-only or low-privilege access. Limited blast radius. |

### SCT's Requirements from the Rotation Process

SCT tests can run for **multiple days** (longevity tests, performance regressions). The Hub's rotation process must use the **overlapping secrets (grace period) pattern** to avoid breaking in-flight tests:

1. **Phase 1 — Mint and Sync**: The Hub generates a new credential in the source system and pushes it to AWS Secrets Manager. New SCT runs starting from this point fetch the new credential. The old credential remains active in the source system.

2. **Phase 2 — Deferred Revocation**: After a grace period (recommended: **7 days** for SCT), the Hub revokes the old credential in the source system. By this time, all SCT test runs that started before the rotation have completed.

**Why 7 days**: The longest SCT tests (longevity, large-scale performance) run for up to 5 days. A 7-day grace period provides a 2-day safety margin.

**SCT caching behavior during rotation**: SCT's in-memory cache persists for the `KeyStore` instance lifetime (the duration of a single test run). A test run that started before rotation will continue using the old credential from its cache. The next test run creates a new `KeyStore` instance and fetches the newly rotated credential. This is exactly the behavior the grace period pattern requires.

### Post-Rotation Validation from SCT Side

After the Hub rotates a secret, the SCT team should validate the new credential works:

```bash
# Run the validation script to check all secrets are readable
python scripts/validate_keystore_secrets.py

# Run a quick smoke test with the new credentials
SCT_KEYSTORE_BACKEND=secretsmanager uv run sct.py run-test \
  longevity_test.LongevityTest.test_custom_time \
  --backend docker --config test-cases/PR-provision-test.yaml
```

The validation script (Phase 4) can be integrated into CI to run periodically, alerting the SCT team if any expected secret is missing or malformed after a rotation.

## 9. Secret Documentation and Metadata

### What to Store in Secrets Manager (Tags and Description)

AWS Secrets Manager supports a `Description` field (free-form text) and up to 50 tags (key-value pairs) per secret. Use these to document operational metadata that is useful when viewing secrets in the AWS Console or via CLI. **Do not store sensitive information in tags or descriptions** — they are not encrypted.

#### Standard Tag Schema

Every secret in Secrets Manager must have these tags:

| Tag Key | Example Value | Purpose |
|---------|---------------|---------|
| `team` | `sct` | Which team owns this secret |
| `environment` | `production` | Environment scope (`production`, `staging`, `development`) |
| `secret_type` | `cloud_credential` | Category: `cloud_credential`, `ssh_key`, `api_token`, `database`, `config`, `service_account` |
| `rotation_tier` | `tier1` | Rotation frequency tier from Section 8 (`tier1`, `tier2`, `tier3`) |
| `last_rotated` | `2026-03-15` | Date of last rotation (ISO 8601) |
| `rotation_due` | `2026-06-15` | Next rotation due date (ISO 8601) |
| `rotated_by` | `jsmith` | Who last rotated this secret |
| `owner` | `@infra-team` | Primary contact responsible for this secret |
| `source_system` | `gcp-console` | Where the credential was generated (e.g., `gcp-console`, `azure-portal`, `aws-iam`, `jira-admin`) |
| `other_locations` | `jenkins-credentials` | Where else this credential exists and would need updating on rotation (e.g., `jenkins-credentials`, `github-secrets`, `developer-laptops`) |
| `confluence_page` | `SCT/KeyStore/azure-credentials` | Link to the Confluence knowledge base page for this secret |

#### Description Field

Use the `Description` field for a one-line summary of what the secret is for:

```
Azure service principal credentials for SCT cloud provisioning (subscription: scylla-qa)
```

### What to Store in Confluence (Sensitive Knowledge Base)

Tags and descriptions in Secrets Manager should **not** contain sensitive operational knowledge. For detailed documentation that includes access scope, recreation procedures, and institutional knowledge, maintain a Confluence page per secret (or per secret group).

Each Confluence page should document:

| Section | Content | Example |
|---------|---------|---------|
| **Purpose** | What this credential is used for in SCT | "GCP service account used by SCT to provision Scylla clusters on GCE. Used in `sdcm/utils/gce_utils.py` and `sdcm/cluster_k8s/gke.py`." |
| **Access Scope** | What permissions/roles this credential grants | "Roles: `compute.admin`, `storage.admin` on project `gcp-sct-project-1`" |
| **Who Has Access** | IAM roles/users that can read this secret | "IAM roles: `sct-runner-role` (via GCP WIF), `sct-developer-role` (via Okta). Hub service account has write access for rotation." |
| **How to Recreate** | Step-by-step procedure to generate a new credential | "1. Go to GCP Console > IAM > Service Accounts. 2. Select `sct-provisioner@gcp-sct-project-1`. 3. Keys > Add Key > JSON. 4. Upload to Secrets Manager." |
| **Where Else It Exists** | All locations where this credential is stored or cached | "1. AWS Secrets Manager (`sct/gcp-sct-project-1.json`). 2. Jenkins credentials store (`gcp-sct-project-1`). 3. May be cached on developer laptops in `~/.sct/`." |
| **Dependencies** | What breaks if this credential is revoked | "All GCE-backend test runs. GKE cluster provisioning. GCP KMS encryption tests." |
| **Rotation History** | Log of past rotations | "2026-03-15: Rotated by @jsmith (quarterly). 2025-12-10: Rotated by @jdoe (emergency — key leaked in logs)." |
| **Emergency Contact** | Who to contact if this credential stops working | "@infra-team in #sct-infra Slack channel" |

### Why Split Between Secrets Manager and Confluence

- **Secrets Manager tags**: Machine-readable, queryable, visible in AWS Console. Good for: "when was this last rotated?", "who owns this?", "is rotation overdue?" Can be used by automation (CloudWatch alarms on `rotation_due`).
- **Confluence pages**: Human-readable, detailed, versioned. Good for: "how do I recreate this if it's compromised?", "what breaks if I revoke it?", "who else has a copy?" Contains institutional knowledge that doesn't fit in 255-character tag values.
