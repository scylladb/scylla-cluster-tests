# Credential-free local testing

## Problem

SCT treats the Scylla QA AWS credentials as a precondition for almost every
command, also when the work never touches AWS. The credential checks are
eager and global: `sct.py` runs the Okta check and the shared SSH key sync at
start-up for every command outside a short fixed list, for every
`cluster_backend`.

Unit tests, Docker-only integration tests, and the Docker backend do not need
AWS. They still fail before they start when the credentials are missing or
expired. A few more paths read the SCT KeyStore or AWS with no check of
whether the run needs it:

- The `k8s-local-kind` backend reads the Docker Hub secret from the KeyStore
  each time it creates a namespace.
- The repository and version lookups read the public
  `downloads.scylladb.com` bucket with signed requests.
- Every backend requires the shared SSH key file to exist, also Docker and
  kind, which never SSH with it.
- minicloud passes S3, Secrets Manager, SSM, STS, and `DescribeImages`
  through to real AWS, also when the image is in the local cache.

No test outside Jenkins guards the credential-free state of the unit tests.
The integration tests do not say which of them need a cloud: only OCI has a
credential skip.

## Who it affects

- Outside contributors (community pull requests, partner teams), who will
  never get Okta access to the QA AWS account. Their fork pull requests get
  no automated signal until a maintainer starts Jenkins.
- Other ScyllaDB teams (core, tooling, drivers), who want to run a Docker or
  kind SCT test on a laptop with no QA account.
- The QA team on local loops: minicloud with a warm image cache, and
  `k8s-local-kind`, with expired or absent credentials.

## Evidence

Measured on master on 2026-10-12 with every cloud credential hidden
(`AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` pointed at nothing, an
empty `HOME`, no gcloud or Azure configuration):

| Workflow | Result without credentials |
|----------|----------------------------|
| `pytest unit_tests/unit -m "not integration"` | 7754 passed, 31 skipped, 0 failed in 80s |
| `pytest unit_tests/integration/test_run_cqlsh.py` (Docker only) | 3 passed in 36s |
| `sct.py integration-tests` | fails at CLI start-up: `OKTA isn't configured` |
| `sct.py conf -b docker test-cases/PR-provision-test-docker.yaml` | fails at CLI start-up: `OKTA isn't configured`, with a link to an internal page |
| `sct.py run-test` on `docker` or `k8s-local-kind` | fails at CLI start-up (same check) |
| `k8s-local-kind` namespace creation | reads the Docker Hub secret from the KeyStore, with no condition |
| minicloud with a warm AMI cache | fails: S3, SSM, and STS go through to real AWS |

The code does not need AWS for these paths. The checks run too early and for
every backend.

## Definition of done

1. The unit tests stay credential-free, and a check outside Jenkins enforces
   it: a job with no secrets runs the full unit suite on each pull request,
   fork pull requests included.
2. `sct.py integration-tests` runs with no credentials. Each Docker-only
   integration test runs. Each test that needs a cloud skips with a reason
   that names the missing credential. No test errors. Target: 25 or more
   integration files pass on a laptop with no credentials.
3. `sct.py run-test` and `sct.py conf` on `docker` and `k8s-local-kind`
   start with no AWS credentials.
   `test-cases/PR-provision-test-docker.yaml` completes end to end.
4. SCT checks a credential the first time it needs it. A missing credential
   fails with a message that names the secret and the feature that needs it,
   and that a person outside ScyllaDB can act on. The message has no link to
   an internal page.
5. minicloud with a warm image cache runs with no AWS credentials, for
   released AWS AMIs and GCE images.
6. Credentialed users and Jenkins see no change: the same secrets backend,
   the same Okta flow, and the same SSH key, when the credentials are there.
7. On a CI job, a test that needs credentials and does not find them fails.
   It does not skip.
8. With all of the above, every row of the Evidence table passes with no
   credentials, except minicloud with a cold cache.

## Out of scope

- Provisioning real cloud resources with no credentials.
- Replacing Secrets Manager as the default KeyStore backend.
- Giving Scylla QA secrets to people outside the QA account.
- Where SCT keeps the SSH keys of the cloud backends, and the writes to
  `~/.ssh` for those backends. SCT-1292 tracks it.
- `k8s-local-kind` with no passwordless sudo. SCT-1293 tracks it.
- minicloud changes inside the minicloud repository: an offline switch,
  local image metadata, local SSM and GCE OAuth stubs, and cache export and
  import. They get their own plan in that repository.
