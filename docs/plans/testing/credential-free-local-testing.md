---
status: draft
domain: testing
created: 2026-10-12
last_updated: 2026-10-13
owner: fruch
---

# Credential-Free Local Testing

## 1. Problem Statement

SCT treats "has Scylla QA AWS credentials" as a precondition for almost everything, even when the
work at hand never touches AWS. This locks out:

- **Outside contributors** (community PRs, partner teams) who will never get Okta access to the QA
  AWS account.
- **Other ScyllaDB teams** (core, tooling, drivers) who want to run a Docker or kind-based SCT test
  on a laptop without being on-boarded to the QA account.
- **Our own offline/local loops**: minicloud with an already-warm image cache, and
  `k8s-local-kind`, still fail before they start when credentials are missing or expired.

Measured on master (2026-10-12) with every cloud credential hidden (`AWS_CONFIG_FILE`,
`AWS_SHARED_CREDENTIALS_FILE` pointed at nothing, empty `HOME`, no gcloud/azure config):

| Workflow | Result without credentials |
|----------|----------------------------|
| `pytest unit_tests/unit -m "not integration"` | **7754 passed, 31 skipped, 0 failed** in 80s |
| `pytest unit_tests/integration/test_run_cqlsh.py` (Docker-only) | **3 passed** in 36s |
| `sct.py integration-tests` | fails at CLI start-up: `OKTA isn't configured` |
| `sct.py conf -b docker test-cases/PR-provision-test-docker.yaml` | fails at CLI start-up: `OKTA isn't configured` + internal Notion link |
| `sct.py run-test` on `docker` / `k8s-local-kind` | fails at CLI start-up (same gate) |
| k8s-local-kind namespace creation | would fail: unconditional Docker Hub secret fetch from KeyStore |
| minicloud with a warm AMI cache | fails: S3/SSM/STS passthrough to real AWS |

The pain is not that the code *needs* AWS - for unit tests, Docker integration tests and the
Docker backend it demonstrably does not. The pain is that credential checks are **eager and
global** instead of **lazy and per-need**.

## 2. Current State

### The global start-up gate

`sct.py:cli` (the click group callback) runs for every subcommand except a hard-coded allow-list
(`unit-tests`, `pre-commit`, `conf-docs`, `lint-pipelines`, `trigger-matrix*`, ...). For all other
commands it unconditionally:

1. Calls `sdcm/utils/aws_okta.py:try_auth_with_okta` - real STS `GetCallerIdentity`, asserts the
   QA account id, and raises with an internal Notion link if no Okta config exists.
2. Calls `KeyStore().sync(["scylla_test_id_ed25519"], ~/.ssh)` - fetches the shared SSH key.
3. Calls `sdcm/utils/docker_utils.py:docker_hub_login` (no-op outside Jenkins).

This gate fires regardless of `cluster_backend`, so `integration-tests`, `run-test`, `run-pytest`,
`provision-resources` and `conf` all hard-require AWS.

### KeyStore

`sdcm/keystore.py:KeyStore` already has pluggable backends selected by `SCT_KEYSTORE_BACKEND`
(`secretsmanager` default, `s3` legacy) with a process-wide cache. There is no local-file backend;
only `get_oci_credentials` (`SCT_OCI_CONFIG_PATH`, `~/.oci/config`) and `get_baremetal_config`
(`./<name>.json`) have local fallbacks. ~97 `KeyStore(` call sites across ~38 files; most are
backend-specific (GCE, Azure, OCI, LDAP, KMS, Argus-in-Jenkins, email, cloud REST).

### Backend-agnostic credential consumers

- `defaults/*.yaml` set `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` for every backend;
  `sdcm/sct_config` `check_required_files` asserts it exists. The Docker backend never uses it for
  SSH (`DockerCmdRunner`), it only has to exist.
- `sdcm/cluster_k8s/__init__.py:KubernetesCluster.create_namespace` always calls
  `create_docker_hub_auth_secret`, which reads `docker.json` from KeyStore - including on
  `k8s-local-kind`.
- `sdcm/utils/common.py:get_s3_scylla_repos_mapping` / `find_scylla_repo` and
  `sdcm/utils/version_utils.py` repo lookups use a **signed** `boto3` S3 client against the public
  `downloads.scylladb.com` bucket; sibling helpers already use `UNSIGNED`.
- Log upload (`LogCollector` -> `cloudius-jenkins-test`) is gated by `collect_logs`, default
  `false`. Argus uses a replay-only client when `JOB_NAME` is unset. Neither blocks local runs.

### Tests

- `unit_tests/unit/conftest.py:mock_cloud_services` (session, autouse) already isolates unit tests:
  temp `HOME`, dummy SSH key, patched KeyStore and AMI/repo lookups. `Jenkinsfile` runs the unit
  stage with blanked `AWS_*` - so unit tests are *already* credential-free, but nothing outside
  Jenkins guards that contract.
- `unit_tests/integration/` (41 files, all `@pytest.mark.integration`) mixes three kinds:
  Docker-only (~25 files: stress threads, cqlsh, cluster, events, grafana, kafka, vector store),
  moto-server (`test_aws_services.py`, `test_provisioning.py`), and real-cloud
  (`test_config*.py`, `test_base_version.py`, `test_find_ami_equivalent.py`, `test_sync.py`,
  `test_s3_remote_uploader.py`, `test_catalog_generator.py`, ...). Only OCI has a credential skip;
  there is no AWS/GCE/Azure skip, and no marker says which cloud a test needs.
- `skills/writing-integration-tests/SKILL.md` already prescribes per-cloud credential skips, but
  the test files have not adopted it.

### Local backends

- **Docker**: needs nothing from AWS past the start-up gate.
- **k8s-local-kind** (`sdcm/cluster_k8s/mini_k8s.py`): needs nothing past the gate except the Docker
  Hub secret above; backups use local minio.
- **minicloud** (`sdcm/utils/minicloud/`, `docs/minicloud.md`): emulates EC2/GCE compute but
  passes S3 (`scylla-qa-keystore`, `cloudius-jenkins-test`, `downloads.scylladb.com`),
  SecretsManager, SSM, STS and `DescribeImages` through to real AWS. AMI disks are built once via
  the EBS direct API and cached in `~/.cache/minicloud/amis`; a cache hit still validates the AMI
  against real AWS.

### CI

- `Jenkinsfile` unit stage blanks `AWS_*`; integration stage runs on an AWS runner via
  `docker/env/hydra.sh --execute-on-runner`, which refuses to start without AWS credentials (fine -
  that path *is* AWS).
- `.github/workflows/` has no test job at all; fork PRs get no automated signal until a maintainer
  triggers Jenkins.

### Related plans

- `docs/plans/keystore-improvements.md` (implemented) - Secrets Manager backend this plan extends.
- `docs/plans/sct-config-validation-and-lazy-images.md` (draft) - moves cloud lookups out of
  `SCTConfiguration.__init__`; complementary, not a prerequisite.
- `docs/plans/infrastructure/ssh-key-decoupling.md` (draft) - runner-image key baking; unrelated
  mechanism, same "stop assuming the shared key" direction.
- `docs/plans/archive/minicloud-local-testing.md` - minicloud origin plan.

## 3. Goals

1. **Unit tests stay credential-free, enforced outside Jenkins**: a GitHub Actions job with zero
   secrets runs the full unit suite on every PR, including fork PRs.
2. **`sct.py integration-tests` runs without credentials** and executes every Docker-only
   integration test; tests needing a cloud are *skipped with a reason naming the missing
   credential*, never errored. Target: ≥ 25 integration files pass, 0 error, on a credential-less
   laptop.
3. **`sct.py run-test` / `conf` on `docker` and `k8s-local-kind` start without AWS credentials**,
   e.g. `test-cases/PR-provision-test-docker.yaml` completes end to end.
4. **Credential checks are lazy**: AWS auth happens the first time AWS is actually needed; a
   missing credential fails with a message naming *which secret* and *which feature* needs it -
   never an internal-only link.
5. **minicloud with a warm cache runs without AWS credentials** for released AWS AMIs and GCE
   images (scope bounded by Phase 7's investigation).
6. **Zero behaviour change for credentialed users and Jenkins**: same secrets backend, same Okta
   flow, same SSH key, when credentials are present.

Non-goals: provisioning real cloud resources without credentials; replacing Secrets Manager;
distributing Scylla QA secrets to outsiders.

## 4. Implementation Phases

Phases are ordered by dependency. Phases 2-4 unblock Docker; 5-6 unblock kind; 7 is minicloud;
8 locks the contract in CI; 9 documents it.

### Phase 1: Plan (this PR)

**Importance**: Critical
**Description**: This document, registered in `MASTER.md` and `progress.json`.

**Definition of Done**:
- [ ] Plan merged as a docs-only PR

### Phase 2: Credential profile - "do I have AWS?" in one place

**Importance**: Critical
**PR**: [#16447](https://github.com/scylladb/scylla-cluster-tests/pull/16447) (together with Phase 3)
**Description**: One small helper answers "are AWS credentials available?" (boto3 credential
chain resolves, without network calls) and "does this run need them?" (derived from
`cluster_backend`: `docker`, `k8s-local-kind`, and minicloud-offline do not; every real cloud
does). Everything below consults it instead of re-deriving.

**Deliverables**: helper in `sdcm/utils/` with unit tests covering each backend and the
creds-present / creds-absent / creds-expired cases.

**Definition of Done**:
- [ ] Helper returns "not needed" for `docker` and `k8s-local-kind`, "needed" for `aws`, `gce`,
      `azure`, `oci`, `k8s-eks`, `k8s-gke`, `xcloud`, baremetal
- [ ] No AWS API call when deciding availability (boto3's credential chain may still probe
      instance metadata on a host without credentials)
- [ ] Unit tests pass

### Phase 3: Lazy CLI start-up gate

**Importance**: Critical
**PR**: [#16447](https://github.com/scylladb/scylla-cluster-tests/pull/16447)
**Description**: Replace the hard-coded allow-list in `sct.py:cli` with a per-command decision.
Commands that resolve a config (`run-test`, `run-pytest`, `conf`, `integration-tests`,
`provision-resources`) decide for themselves once their `--backend` is known (the click group
callback runs before the subcommand's options are parsed), and
run Okta auth and the shared-key sync **only when the profile says AWS is needed**, or when
credentials are present (keeps today's behaviour for staff). Otherwise:

- skip Okta entirely;
- if `~/.ssh/scylla_test_id_ed25519` is missing, generate a local ed25519 key pair at that path
  (it is only used to satisfy `check_required_files` on local backends) and log that it did so.

The Okta error message stops pointing at an internal page and states what to do for both staff
and outsiders.

**Definition of Done**:
- [ ] `sct.py conf -b docker test-cases/PR-provision-test-docker.yaml` succeeds with creds hidden
- [ ] Same command with creds present behaves exactly as today (Okta + key sync run)
- [ ] `sct.py conf -b aws ...` with creds hidden fails fast with an actionable message
- [ ] An existing `~/.ssh/scylla_test_id_ed25519` is never overwritten

### Phase 4: Integration-test credential marker

**Importance**: Critical
**PR**: [#16453](https://github.com/scylladb/scylla-cluster-tests/pull/16453)
**Description**: Add a `requires_aws` marker (registered in `unit_tests/pytest.ini`). A
`unit_tests/integration/conftest.py` hook skips a marked test when the Phase 2 helper finds no AWS
credentials, with reason `"needs AWS credentials for the SCT AWS account"`. In CI (`JOB_NAME` set)
the same condition **fails** the test instead, so a broken credential setup can't quietly hide a
whole class of tests.

Which tests get the marker is decided by measurement, not inspection: run the integration suite
with every credential hidden and mark what fails for lack of credentials. Tests that read cached
data from S3 can *pass by accident* without credentials (an unreadable cache reads as "empty"), so
they are marked too. Moto-server tests must not need real credentials at all: fix them rather than
mark them.

One marker is enough: SCT reads its GCE, Azure and OCI credentials from KeyStore, which itself
needs AWS, and every measured credential failure was AWS. Tests that use a cloud SDK directly
(OCI utils, catalog generator) keep their existing skips. `try/except ClientError → skip` blocks
stay too; they cover API errors, not missing credentials.

`sct.py integration-tests` no longer needs Okta (Phase 3) and accepts
`-m "integration and not requires_aws"` filtering. The `need_network` marker stays orthogonal
(public internet ≠ credentials).

**Definition of Done**:
- [ ] Integration suite with creds hidden: every cloud-dependent test skips with a credential
      reason, 0 credential failures (measured on the marked files: 43 passed, 81 skipped)
- [ ] Same run with creds present: no test skipped by the marker
- [ ] With `JOB_NAME` set and creds hidden, marked tests fail
- [ ] A lint/collection check fails if a test calls real boto3 without the marker -
      **Needs Investigation**: whether a cheap collection-time heuristic is reliable, or
      whether this stays a review checklist item in the `writing-integration-tests` skill

### Phase 5: Unsigned reads of public buckets

**Importance**: Important
**PR**: [#16454](https://github.com/scylladb/scylla-cluster-tests/pull/16454)
**Description**: Repo/version lookups against the public `downloads.scylladb.com` bucket
(`get_s3_scylla_repos_mapping`, `find_scylla_repo`, `get_branched_repo` and siblings) use
unsigned S3 requests, like the helpers next to them already do. Removes a credential dependency
from config resolution for every backend and from minicloud's `downloads` passthrough.

**Definition of Done**:
- [ ] Repo lookup integration tests pass with creds hidden and drop `requires_aws` (32 tests:
      repo lookups, base versions, baremetal and OCI config)
- [ ] No change in resolved URLs vs. master for a fixed set of versions

### Phase 6: Optional Docker Hub secret on local k8s

**Importance**: Important
**PR**: [#16468](https://github.com/scylladb/scylla-cluster-tests/pull/16468)
**Description**: `KubernetesCluster.create_namespace` creates the Docker Hub auth secret only when
credentials are obtainable; on `k8s-local-kind` without them it logs once and relies on
anonymous Docker Hub pulls (subject to the anonymous rate limit, acceptable for a laptop).
The Helm charts keep referencing the secret: kubelet reports the missing pull secret and
pulls anonymously.

**Definition of Done**:
- [ ] A pod referencing the missing pull secret on a kind cluster pulls anonymously and starts
- [ ] `k8s-local-kind` functional smoke test (`test-cases/scylla-operator/functional.yaml`
      with a narrowed test selection - **Needs Investigation**: cheapest selection that still
      creates a namespace) passes with creds hidden
- [ ] EKS/GKE paths unchanged (secret still created)

### Phase 7: KeyStore `local` backend + minicloud offline mode

**Importance**: Important
**Description**: Add `SCT_KEYSTORE_BACKEND=local`: secrets are files under a directory
(`~/.sct/keystore/` by default, overridable), same names as the Secrets Manager keys. A missing
file raises an error naming the secret and the feature that asked for it. This lets outsiders
supply *their own* values for the few secrets a local run might touch (e.g. their own Docker Hub
token) and lets minicloud stop passing `scylla-qa-keystore` through.

For minicloud, define an **offline mode** used when the AMI/image is already in
`~/.cache/minicloud`: no S3 passthrough buckets, `collect_logs` off, KeyStore `local`, and no
`DescribeImages`/SSM/STS calls to real AWS for cached images.

**Needs Investigation** (likely a separate plan in the minicloud repo per one-concern-per-plan):
- Which passthrough calls minicloud makes on a cache hit, and whether `RunInstances` AMI validation
  can be skipped when the disk is cached.
- How an outsider *obtains* a warm cache without AWS: export/import a cache bundle, build from a
  public Scylla image (GCE public images, Docker image → qcow2), or a published cache artifact.
  Until answered, Goal 5 is limited to hosts whose cache was warmed once with credentials.

**Definition of Done**:
- [ ] `local` backend with unit tests (hit, miss → actionable error, cache key isolation from
      Secrets Manager)
- [ ] A minicloud `PR-provision-test` run on a warm cache completes with creds hidden
- [ ] Default backend and behaviour unchanged when `SCT_KEYSTORE_BACKEND` is unset

### Phase 8: Credential-free CI guard

**Importance**: Critical
**Description**: A GitHub Actions workflow with **no secrets** that runs on every PR (fork PRs
included, `pull_request` trigger - not `pull_request_target`):
- full unit suite (`sct.py unit-tests`);
- `sct.py integration-tests -m "integration and not requires_aws"` on the runner's Docker.

This is the contract test for Goals 1-2: anything that sneaks a credential dependency into these
paths turns the job red. Jenkins stages stay as they are.

**Definition of Done**:
- [ ] Workflow green on master; deliberately adding a real KeyStore call to a unit test turns it
      red
- [ ] Runtime under 30 minutes - **Needs Investigation**: GitHub-hosted runner disk/CPU for the
      Docker Scylla fixtures; fall back to unit-only + a small Docker subset if too slow
- [ ] No `secrets.*` referenced in the workflow (CodeQL clean)

### Phase 9: Documentation

**Importance**: Critical
**Description**: New `docs/running-without-credentials.md` with the capability matrix (what works
with no credentials, with your own credentials, with Scylla QA credentials), linked from
`README.md` and `docs/contrib.md`; README stops presenting Okta as a universal prerequisite.
Update `docs/minicloud.md` (offline mode), `skills/writing-integration-tests/SKILL.md` (markers
replace ad-hoc env-var skips), and `AGENTS.md` testing section.

**Definition of Done**:
- [ ] A person with no Scylla accounts can follow the doc from clone to a passing Docker
      `run-test` (validated by someone outside the QA team)
- [ ] Skill and AGENTS.md reference the markers

## 5. Testing Requirements

### Unit

- Phase 2 helper: matrix of backends × credential states, with boto3 credential resolution mocked.
- Phase 3: click `CliRunner` tests for `conf`/`run-test` with and without creds; key generation
  creates a valid key and never overwrites an existing one.
- Phase 7: `local` KeyStore hit/miss/error message; cache isolation between backends.

### Integration

- Phase 4: the full integration suite run twice - creds hidden and creds present - and the pass
  sets diffed. The credentialed run must match master exactly.
- Phase 5: repo lookup tests run unsigned.
- Phase 6/7: one kind run and one minicloud warm-cache run with creds hidden.

### Manual

- Fresh VM / container with no `~/.aws`, `~/.okta_aws_login_config`, gcloud or azure config:
  clone, `uv sync`, unit tests, integration tests, Docker `run-test`, kind `run-test`.
- Staff laptop with Okta: every existing workflow unchanged.
- One Jenkins PR build with `test-integration` label: unchanged results.

## 6. Success Criteria

Completing all Definition of Done items constitutes success. Plan-level:

1. The "without credentials" column of the Problem Statement table is green for every row except
   minicloud cold-cache (tracked by Phase 7's investigation).
2. The Phase 8 workflow has been green on master for two consecutive weeks of normal merge
   traffic - proving the contract holds without anyone actively defending it.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Lazy gate lets a cloud run start, then fail late on an expired token | Medium | Medium | Phase 2 marks every real cloud backend "needed" → gate still runs up front for them; only local backends skip it |
| Marker skip hides a real regression in Jenkins (creds misconfigured → whole class silently skipped) | Medium | High | In Jenkins (`JOB_NAME` set) a `requires_*` skip becomes a failure, not a skip |
| Generated local SSH key confused with the shared key | Low | Medium | Generated only when the file is absent and the profile says AWS not needed; logged loudly; never overwrites |
| Unsigned S3 reads hit throttling that signed reads did not | Low | Low | Same public bucket other helpers already read unsigned; retry policy unchanged |
| GitHub-hosted runners too small for Docker Scylla fixtures | Medium | Medium | Start with unit + a curated Docker subset; grow as runtime allows |
| `pull_request` workflow abused by fork PRs | Low | Medium | No secrets, read-only token, no `pull_request_target` (see existing CodeQL rule) |
| minicloud offline mode needs emulator-side changes | High | Medium | Scoped as Needs Investigation; Phases 2-6 deliver value without it |

Rollback: every phase is gated on "credentials absent **and** local backend". Reverting a phase
restores today's eager behaviour; credentialed users never exercise the new paths.
