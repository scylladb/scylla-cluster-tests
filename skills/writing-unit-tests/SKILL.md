---
name: writing-unit-tests
description: >-
  Guides writing and debugging unit tests for the SCT framework using
  pytest conventions. Use when creating new test files in unit_tests/,
  adding test cases, mocking external services, setting up fixtures, or
  reviewing test coverage. Covers network-blocking patterns, FakeRemoter,
  moto for AWS mocking, monkeypatch, and common pitfalls.
---

# Writing Unit Tests for SCT

Write isolated, fast unit tests that never contact external services.

## Essential Principles

### External Services Must Be Blocked

**Unit tests must never make real network calls — not to AWS, GCE, Azure, Docker registries, or any external endpoint.**

The `conftest.py` autouse `fake_remoter` fixture blocks SSH connections automatically. But HTTP-based services (boto3, requests, REST APIs) are NOT auto-blocked. You must mock them explicitly using `unittest.mock.patch`, `monkeypatch`, or `moto`.

If a test is slow or flaky, the first suspect is an unmocked network call.

### Use pytest, Not unittest.TestCase

**All tests use pytest functions, fixtures, and `assert` — never `unittest.TestCase`.**

`unittest.TestCase` breaks pytest's fixture injection, `autouse` fixtures, parametrize, and parallel execution. SCT requires pytest-native style throughout `unit_tests/`.

### Tests Must Be Isolated and Parallel-Safe

**Every test must pass independently, in any order, and in parallel.**

SCT runs tests with `pytest-xdist` (`-n2` by default) and `pytest-random-order`. Never rely on test execution order, shared mutable state, or global side effects. Use fixtures for setup/teardown, `monkeypatch` for environment variables, and `tmp_path` for file-based tests.

### Mock at the Boundary, Not the Logic

**Mock external dependencies (network, file system, cloud APIs) — not internal SCT logic.**

Mocking internal functions makes tests brittle and hides bugs. Mock at the outermost boundary: the HTTP call, the SSH command, the cloud SDK client. This tests the actual logic while isolating from infrastructure.

## When to Use

- Creating a new test file in `unit_tests/`
- Adding test cases to an existing unit test module
- Mocking AWS, GCE, Azure, or other cloud services in tests
- Setting up pytest fixtures for SCT components
- Debugging a unit test that is failing, slow, or flaky
- Converting unittest-style tests to pytest style

## When NOT to Use

- Writing integration tests that need Docker or real services — use the `writing-integration-tests` skill
- Running or configuring CI pipelines — edit Jenkins pipeline files directly
- Writing functional tests for K8s operators — see `functional_tests/`
- Fixing production code bugs — edit the source in `sdcm/` directly

## Quick Reference: Test Infrastructure

### Autouse Fixtures (Always Active)

These fixtures from `unit_tests/conftest.py` run automatically for every test:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `fake_remoter` | function | Blocks real SSH; sets `FakeRemoter` as default remoter |
| `fake_provisioner` | session | Registers `FakeProvisioner` for cloud provisioning |
| `fake_region_definition_builder` | session | Registers `FakeDefinitionBuilder` for regions |
| `fixture_cleanup_continuous_events_registry` | function | Cleans up event registry between tests |

### On-Demand Fixtures

Request these by name in your test function signature:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `params` | function | SCT configuration with `SCT_CLUSTER_BACKEND=docker` |
| `events` | module | Event system with mocked devices |
| `events_function_scope` | function | Event system per-test (cleaner isolation) |
| `prom_address` | session | Prometheus metrics server address |
| `monkeypatch` | function | Pytest built-in for patching env vars and attributes |
| `tmp_path` | function | Pytest built-in temporary directory |

### Test Markers

| Marker | Purpose | Unit Test Usage |
|--------|---------|-----------------|
| `@pytest.mark.integration` | Marks integration tests | **Do NOT use** — unit tests must NOT have this |
| `@pytest.mark.sct_config(files="...")` | Loads specific SCT config | Use when testing config-dependent code |
| `@pytest.mark.parametrize` | Test multiple inputs | Use freely for data-driven tests |

## Quick Reference: Mocking Patterns

### Pattern 1: monkeypatch for Environment Variables

```python
def test_config(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-123")
    config = SCTConfiguration()
    assert config.get("cluster_backend") == "aws"
```

### Pattern 2: unittest.mock.patch for Functions

```python
from unittest.mock import patch, MagicMock

def test_s3_download():
    with patch("sdcm.utils.common._s3_download_file") as mock_dl:
        mock_dl.return_value = "/tmp/file.tar.gz"
        result = download_from_cloud("s3://bucket/file.tar.gz")
        assert result == "/tmp/file.tar.gz"
```

### Pattern 3: moto for Full AWS Service Mocking

```python
import boto3
from moto import mock_aws

@mock_aws
def test_ec2_provisioning():
    ec2 = boto3.client("ec2", region_name="us-east-1")
    ec2.run_instances(ImageId="ami-12345", MinCount=1, MaxCount=1)
    instances = ec2.describe_instances()
    assert len(instances["Reservations"]) == 1
```

### Pattern 4: FakeRemoter for Remote Commands

```python
import re
from invoke import Result

def test_node_command(fake_remoter):
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN 10.0.0.1", exited=0),
        re.compile(r"cat /etc/scylla/scylla.yaml"): Result(stdout="cluster_name: test", exited=0),
    }
    # Code that calls node.remoter.run("nodetool status") will get the fake result
```

### Pattern 5: monkeypatch for Attribute Replacement

```python
def test_custom_behavior(monkeypatch):
    monkeypatch.setattr("sdcm.utils.common.S3Storage.download_file", lambda *a, **kw: "/fake/path")
    result = some_function_that_downloads()
    assert result == "/fake/path"
```

## Debugging Unit Tests

### Test Hangs or Is Slow

1. **Unmocked network call.** Add `-s` flag to see stdout and check for connection attempts:
   ```bash
   uv run python -m pytest unit_tests/test_module.py::test_function -v -s
   ```
2. **Unmocked `wait.wait_for` loop.** If code uses SCT's `wait_for`, mock it or reduce the timeout.

### Test Passes Alone but Fails in Parallel

1. **Shared global state.** Use fixtures instead of module-level variables.
2. **Unpinned environment variables.** Use `monkeypatch.setenv` not `os.environ`.
3. **Conflicting `FakeRemoter.result_map`.** The result_map is a class attribute — set it per test, not globally.

### FakeRemoter Raises ValueError

The error `No fake result specified for command: <cmd>` means the code under test runs a remote command that `FakeRemoter.result_map` doesn't know about. Add the missing command pattern to `result_map`.

### Test Fails With Import Errors

SCT has many dependencies. If a test fails with `ModuleNotFoundError`, ensure:
1. Your virtualenv is active: `uv sync`
2. The import is at the top of the file, not inline

## Running Tests

```bash
# Run all unit tests (excludes integration)
uv run sct.py unit-tests

# Run a specific test file
uv run sct.py unit-tests -t test_config.py

# Run a specific test function with verbose output
uv run python -m pytest unit_tests/test_config.py::test_function_name -v -s

# Run with parallel execution disabled (for debugging)
uv run python -m pytest unit_tests/test_config.py -v -s -n0
```

## Reference Index

| File | Content |
|------|---------|
| [common-pitfalls.md](references/common-pitfalls.md) | Detailed pitfalls and anti-patterns with before/after fixes |

| Workflow | Purpose |
|----------|---------|
| [write-a-unit-test.md](workflows/write-a-unit-test.md) | 4-phase process for writing a new unit test |

## Success Criteria

A well-written SCT unit test:

- [ ] Lives in `unit_tests/` with a `test_*.py` filename
- [ ] Uses pytest style (`assert`, fixtures, `@pytest.mark.parametrize`) — not unittest
- [ ] Does NOT have `@pytest.mark.integration` marker
- [ ] Makes zero real network calls (all external services mocked)
- [ ] Uses `monkeypatch` for environment variables, not `os.environ`
- [ ] Uses `tmp_path` for temporary files, not hardcoded paths
- [ ] Passes in isolation, in parallel, and in random order
- [ ] Has all imports at the top of the file
- [ ] Follows Google docstring format for test docstrings
- [ ] Passes `uv run sct.py pre-commit` checks
