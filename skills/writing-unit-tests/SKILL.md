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
All new unit tests go in `unit_tests/unit/` (not the root `unit_tests/` directory).

## Essential Principles

### External Services Must Be Blocked

**Unit tests must never make real network calls — not to AWS, GCE, Azure, Docker registries, or any external endpoint.**

The `unit_tests/unit/conftest.py` autouse `fake_remoter` fixture blocks SSH connections automatically. But HTTP-based services (boto3, requests, REST APIs) are NOT auto-blocked. You must mock them explicitly using `unittest.mock.patch`, `monkeypatch`, or `moto`.

If a test is slow or flaky, the first suspect is an unmocked network call.

### Use pytest, Not unittest.TestCase

**All tests use pytest functions, fixtures, and `assert` — never `unittest.TestCase` or `class Test*`.**

`unittest.TestCase` breaks pytest's fixture injection, `autouse` fixtures, parametrize, and parallel execution. SCT requires pytest-native style throughout `unit_tests/unit/`. This means **no test classes at all** — use flat module-level `def test_*` functions and group related tests with comment blocks (see AP-4).

**Never duplicate test infrastructure** — fake objects, base classes, runner stubs, and fixture setup code. Before adding anything new, check `fake_cluster.py`, `unit_tests/unit/nemesis/__init__.py`, `execute_nemesis/__init__.py`, and `unit_tests/unit/conftest.py`. Add concrete subclasses or attributes to existing structures; only create a new class hierarchy when the registry under test must be isolated from existing subclasses, and document the reason (see AP-5).

### Tests Must Be Isolated and Parallel-Safe

**Every test must pass independently, in any order, and in parallel.**

SCT runs tests with `pytest-xdist` (`-n2` by default) and `pytest-random-order`. Never rely on test execution order, shared mutable state, or global side effects. Use fixtures for setup/teardown, `monkeypatch` for environment variables, and `tmp_path` for file-based tests.

**Special care for `Singleton` classes:** SCT has classes with `metaclass=Singleton` (e.g. `NodeLoadInfoServices`, `AdaptiveTimeoutStore` subclasses) that persist mutable state across tests on the same worker process. Add an `autouse` fixture that clears the cache in teardown (post-`yield` only — never pre-yield). See pitfall P-16 for details.

### Mock at the Boundary, Not the Logic

**Mock external dependencies (network, file system, cloud APIs) — not internal SCT logic.**

Mocking internal functions makes tests brittle and hides bugs. Mock at the outermost boundary: the HTTP call, the SSH command, the cloud SDK client. This tests the actual logic while isolating from infrastructure.

**Never reimplement the code under test in a fake class.** If you find yourself copying a method body from `sdcm/` into a `FakeFoo` helper in your test, stop — you are testing the copy, not the real code. Always instantiate the real class and mock only its external I/O (network, file system, cloud APIs). See anti-pattern AP-6 for details.

### No Inline Classes in Fixtures or Tests

**Define helper classes at module level, not inside fixtures or test functions.**

Inline classes (defined inside a function or fixture) are harder to read, cannot be reused, and make diffs confusing. Define helper classes at module level and instantiate them in fixtures. This keeps test code flat and scannable.

### Use Events Fixtures for Event System Tests

**Use `events_function_scope` fixture when tests publish or read SCT events — never manage `EventsUtilsMixin` manually.**

The `events_function_scope` fixture (from `unit_tests/conftest.py`) creates a fully isolated events system per test — fresh temp directory, events device, and registry patcher. This prevents event leakage between tests. Access the raw events log via `events_fixture.get_raw_events_log()` and the events logger via `events_fixture.get_events_logger()`. Use `events` (module scope) only when many tests share expensive event setup and you are certain there is no cross-test interference.

## When to Use

- Creating a new test file in `unit_tests/unit/`
- Adding test cases to an existing unit test module in `unit_tests/unit/`
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

These fixtures from `unit_tests/unit/conftest.py` run automatically for every unit test:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `fake_remoter` | function | Blocks real SSH; sets `FakeRemoter` as default remoter (returns the **class**, not an instance) |
| `fake_provisioner` | session | Registers `FakeProvisioner` for cloud provisioning |
| `fake_region_definition_builder` | session | Registers `FakeDefinitionBuilder` for regions |
| `fixture_cleanup_continuous_events_registry` | function | Cleans up event registry between tests |

**Important:** AWS, GCE, and Azure HTTP calls are **NOT** auto-blocked. You must mock them per-test using `unittest.mock.patch`, `patch.object`, `monkeypatch`, or `moto`. Common functions to patch include `convert_name_to_ami_if_needed`, `find_scylla_repo`, `get_arch_from_instance_type`, and `KeyStore` methods. Use `patch.object(KeyStore, "method_name", ...)` for `KeyStore` since it's imported via `from sdcm.keystore import KeyStore` in 20+ modules.

### On-Demand Fixtures (from parent `unit_tests/conftest.py`)

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
| `@pytest.mark.parametrize` | Test multiple inputs | Use freely for data-driven tests; always use `pytest.param(id=...)` for human-readable names |

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
    # fake_remoter is the FakeRemoter CLASS (not an instance).
    # Setting result_map here sets a class attribute, affecting all instances.
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

## Test Naming Convention

Use the pattern `test_<function>_<scenario>_<expected>`:

```python
# Good — describes behavior, condition, and expectation
def test_parse_version_invalid_string_returns_none(): ...
def test_config_missing_backend_raises_value_error(): ...
def test_health_check_single_node_failure_does_not_block_others(): ...

# Bad — generic, unclear what's being tested
def test_parse(): ...
def test_config_1(): ...
def test_it_works(): ...
```

## Running Tests

```bash
# Run all unit tests (excludes integration)
uv run sct.py unit-tests

# Run a specific test file
uv run sct.py unit-tests -t unit/test_config.py

# Run a specific test function with verbose output
uv run python -m pytest unit_tests/unit/test_config.py::test_function_name -v -s

# Run with parallel execution disabled (for debugging)
uv run python -m pytest unit_tests/unit/test_config.py -v -s -n0

# Run with coverage report
uv run python -m pytest unit_tests/unit/ --cov=sdcm --cov-report=term-missing

# Run specific file with coverage for a single module
uv run python -m pytest unit_tests/unit/test_config.py --cov=sdcm.sct_config --cov-report=term-missing
```

## Reference Index

| File | Content |
|------|---------|
| [common-pitfalls.md](references/common-pitfalls.md) | Pitfalls P-1 through P-16 with before/after fixes |
| [anti-patterns.md](references/anti-patterns.md) | Anti-patterns AP-1 through AP-6 with before/after fixes |

| Workflow | Purpose |
|----------|---------|
| [write-a-unit-test.md](workflows/write-a-unit-test.md) | 4-phase process for writing a new unit test |

## Success Criteria

A well-written SCT unit test:

- [ ] Lives in `unit_tests/unit/` with a `test_*.py` filename
- [ ] Uses pytest style (`assert`, fixtures, `@pytest.mark.parametrize`) — not unittest
- [ ] Does NOT have `@pytest.mark.integration` marker
- [ ] Makes zero real network calls (all external services mocked)
- [ ] Uses `monkeypatch` for environment variables, not `os.environ`
- [ ] Uses `tmp_path` for temporary files, not hardcoded paths
- [ ] Passes in isolation, in parallel, and in random order
- [ ] Has all imports at the top of the file
- [ ] Follows Google docstring format for test docstrings
- [ ] Passes `uv run sct.py pre-commit` checks
