---
name: writing-integration-tests
description: >-
  Guides writing and debugging integration tests for the SCT framework
  that interact with real external services. Use when creating tests
  requiring Docker, AWS, GCE, Azure, OCI, or Kubernetes backends.
  Covers service labeling, credential skip patterns, Docker Scylla
  fixtures, resource cleanup, and common pitfalls.
---

# Writing Integration Tests for SCT

Write integration tests that clearly label their external service dependencies and clean up after themselves.

## Essential Principles

### Every Integration Test Must Be Marked

**All integration tests MUST have `@pytest.mark.integration`.**

SCT separates test execution: `sct.py unit-tests` runs with `-m "not integration"`, and `sct.py integration-tests` runs with `-m "integration"`. A test without the marker runs as a unit test, where external services are unavailable and the test will fail.

### External Services Must Be Labeled

**Every integration test must declare which external services it requires.**

Use one or more of these approaches:
1. **Module docstring** listing service requirements
2. **`pytest.mark.skipif`** with credential checks and clear `reason` messages
3. **Comments** on individual test functions

This enables developers to know which tests they can run locally and which require cloud credentials.

### Service Categories

| Label | Services | Credential Check |
|-------|----------|-----------------|
| **docker** | Scylla container, Vector Store, Kafka | Docker daemon running |
| **aws** | EC2, S3, IAM, KMS, SSM, CloudFormation | `AWS_ACCESS_KEY_ID` or `AWS_PROFILE` |
| **gce** | Compute Engine, GCS, KMS | `GOOGLE_APPLICATION_CREDENTIALS` |
| **azure** | VMs, Storage, KMS, Resource Groups | `AZURE_SUBSCRIPTION_ID` |
| **oci** | Compute, Identity, Network | `OCI_CONFIG_FILE` |
| **kubernetes** | EKS, GKE, local Kind cluster | `KUBECONFIG` or `sct.py integration-tests` setup |

### Resources Must Be Cleaned Up

**Every resource created during a test must be destroyed, even if the test fails.**

Use `yield` fixtures with teardown blocks, context managers, or `try/finally`. Leaked cloud resources cost real money and break subsequent test runs.

## When to Use

- Writing tests that need a real Scylla Docker container (CQL, Alternator, stress tools)
- Testing cloud provisioning logic against real or mocked AWS/GCE/Azure/OCI APIs
- Validating Kubernetes operator behavior with a real K8s cluster
- Testing stress tool threads (cassandra-stress, latte, YCSB) against live endpoints
- Writing tests that verify network behavior, SSL/TLS, or multi-node clustering

## When NOT to Use

- Testing pure logic that can be validated with mock data — use the `writing-unit-tests` skill
- Testing configuration parsing or value transformation — unit test with `monkeypatch`
- Testing error handling for known error formats — unit test with fake responses
- Writing end-to-end longevity or performance tests — those are separate test types in `*_test.py`

## Quick Reference: Integration Test Fixtures

### Docker Scylla Fixtures (from `conftest.py`)

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `docker_scylla` | function | Single Scylla container with CQL + Alternator ports |
| `docker_scylla_2` | function | Second Scylla node seeded from `docker_scylla` |
| `docker_vector_store` | function | Vector Store container connected to `docker_scylla` |

**Configuration via marker:**
```python
@pytest.mark.integration
@pytest.mark.docker_scylla_args(
    ssl=True,
    scylla_docker_image="scylladb/scylla:2025.2.0",
    docker_network="my-network",
)
def test_with_ssl(docker_scylla, params):
    ...
```

### SCT Configuration Fixture

```python
@pytest.mark.integration
@pytest.mark.sct_config(files="test-cases/my-test.yaml")
def test_with_config(docker_scylla, params):
    # params is loaded from the specified config file
    ...
```

### Events Fixtures

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `events` | module | Shared event system (efficient for many tests) |
| `events_function_scope` | function | Per-test event system (better isolation) |

## Quick Reference: Service-Specific Patterns

### Docker Tests (Service: docker)

```python
@pytest.mark.integration
def test_cql_query(docker_scylla):
    """Test CQL connectivity to Scylla container.

    External services: Docker (Scylla container)
    """
    session = docker_scylla.cql_connection()
    result = session.execute("SELECT cluster_name FROM system.local")
    assert result.one()
```

### AWS Tests (Service: aws)

For tests that can use mocked AWS, use `moto`:
```python
import boto3
from moto import mock_aws

@pytest.mark.integration
@mock_aws
def test_s3_operations():
    """Test S3 storage operations with mocked AWS.

    External services: AWS S3 (mocked via moto)
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
```

For tests that need real AWS:
```python
import os
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("AWS_ACCESS_KEY_ID"),
        reason="AWS credentials not configured — set AWS_ACCESS_KEY_ID",
    ),
]
```

### OCI Tests (Service: oci)

```python
import os
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("OCI_CONFIG_FILE"),
        reason="OCI credentials not configured — set OCI_CONFIG_FILE",
    ),
]

def test_oci_instances():
    """Test OCI compute instance listing.

    External services: OCI Compute, OCI Identity
    """
    ...
```

### Kubernetes Tests (Service: kubernetes)

Kubernetes tests require the `sct.py integration-tests` runner which sets up a local Kind cluster:
```python
@pytest.mark.integration
def test_k8s_operator(k8s_cluster):
    """Test K8s operator deployment.

    External services: Kubernetes (local Kind cluster)
    Prerequisites: Run via `sct.py integration-tests` (sets up LocalKindCluster)
    """
    ...
```

## Debugging Integration Tests

### Container Fails to Start

1. **Check Docker daemon:** `docker ps` — is Docker running?
2. **Check image availability:** `docker pull scylladb/scylla-nightly:latest`
3. **Check port conflicts:** `docker ps` — is another Scylla container using the same ports?
4. **Check container logs:** After failure, run `docker logs <container_id>`

### Test Hangs Waiting for Service

1. **Increase timeout** in `wait.wait_for` calls.
2. **Check container health:** `docker exec <id> nodetool status`
3. **Run with `-s` flag** to see stdout during waits:
   ```bash
   uv run python -m pytest unit_tests/test_module.py -v -s -m integration -n0
   ```

### Test Passes Locally but Fails in CI

1. **Missing credentials.** CI may not have the same cloud credentials. Add `skipif` guards.
2. **Docker version differences.** CI may use a different Docker version. Check `docker --version`.
3. **Network restrictions.** CI may block outgoing connections. Use `moto` for AWS tests when possible.

### Resource Leaks After Test Failure

1. **Check for leftover containers:** `docker ps -a | grep scylla`
2. **Clean up manually:** `docker rm -f <container_id>`
3. **Fix the test:** Ensure cleanup is in a fixture's `yield` teardown or `finally` block.

## Running Tests

```bash
# Run all integration tests (sets up K8s prerequisites)
uv run sct.py integration-tests

# Run a specific integration test file
uv run sct.py integration-tests -t test_your_module.py

# Run a specific test with verbose output (no parallel)
uv run python -m pytest unit_tests/test_your_module.py::test_function -v -s -m integration -n0

# Run integration tests with more parallel workers
uv run sct.py integration-tests -n 8
```

## Reference Index

| File | Content |
|------|---------|
| [common-pitfalls.md](references/common-pitfalls.md) | Integration-specific pitfalls and anti-patterns with before/after fixes |

| Workflow | Purpose |
|----------|---------|
| [write-an-integration-test.md](workflows/write-an-integration-test.md) | 4-phase process for writing a new integration test |

## Success Criteria

A well-written SCT integration test:

- [ ] Has `@pytest.mark.integration` on every test function or at module level
- [ ] Documents external service dependencies (docstring, skipif, or comments)
- [ ] Skips cleanly with a clear reason when credentials are missing
- [ ] Cleans up all created resources (containers, cloud resources, temp files)
- [ ] Uses `docker_scylla` fixture for Scylla container tests (not custom Docker setup)
- [ ] Sets timeouts on operations that could hang
- [ ] Uses `xdist_group` when tests share expensive resources
- [ ] Has all imports at the top of the file
- [ ] Follows Google docstring format
- [ ] Passes `uv run sct.py pre-commit` checks
