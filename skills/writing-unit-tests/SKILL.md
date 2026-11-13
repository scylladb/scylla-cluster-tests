---
name: writing-unit-tests
description: >-
  Guides writing and debugging pytest unit tests for the SCT framework.
  Use when creating or reviewing tests in unit_tests/, mocking external
  services, or setting up fixtures. Triggers: "write a unit test", "add
  a test for", "mock boto3", "fix a flaky test", "convert unittest to
  pytest". Covers mocking, fixtures, and test-quality checks.
---

# Writing Unit Tests for SCT

Write isolated, fast unit tests that never contact external services.
All new unit tests go in `unit_tests/unit/` (not the root `unit_tests/` directory).

## Essential Principles

### Mock Every External Service

**Unit tests make no real network calls: not to AWS, GCE, Azure, Docker registries, or any other endpoint.**

The `unit_tests/unit/conftest.py` autouse `fake_remoter` fixture blocks SSH connections. HTTP-based services (boto3, requests, REST APIs) are not blocked automatically, so mock them with `unittest.mock.patch`, `monkeypatch`, or `moto`. If a test is slow or flaky, look for an unmocked network call first.

### pytest Functions, Fixtures, and Parametrize

**Tests are flat module-level `def test_*` functions that use fixtures and plain `assert`.** pytest fixture injection, `autouse` fixtures, parametrize, and `pytest-xdist` all depend on this style.

- **One parametrized test per behavior.** Cases that differ only in input and expected value are `pytest.param(id=...)` entries of one test. That includes cases with different expected errors, and new cases for an existing test. Several inputs in one test become parameters, so each case reports on its own (AP-2).
- **Setup lives in fixtures.** Code that builds the object under test, its mocks, or its patches is a fixture or fixture factory. Patches every test needs are `autouse`. When setup is longer than the act and assert lines, move it into a fixture named for the scenario (P-15, P-21).
- **Reuse existing infrastructure.** Check `unit_tests/lib/`, `unit_tests/unit/nemesis/__init__.py`, `fake_cluster.py`, and the `conftest.py` files before writing a fake (AP-5).

### Every Test Runs Alone

**Every test passes on its own, in any order, and in parallel.**

`sct.py unit-tests` runs them with `pytest-xdist` (`-n4`). Check order independence locally with `--random-order`.

- State comes from fixtures, environment variables from `monkeypatch`, and files from `tmp_path`.
- Variables the shell or CI exports that switch a code path are removed in an `autouse` fixture (P-8).
- Classes with `metaclass=Singleton` (`NodeLoadInfoServices`, `AdaptiveTimeoutStore` subclasses) are cleared by an `autouse` fixture before and after `yield` (P-16).
- Timing never decides a result: no wall-clock asserts, fixed waits patched, retry loops on a fake clock (P-22).

### Exercise Real Code, Mock Only Its I/O

**Construct the real `sdcm` class and mock only its external boundary: the network, the file system, the remoter, the cloud SDK client.**

Then the code under test is the code that runs. Build the subject through its constructor, not `__new__` or methods bound onto a mock, and keep production logic out of the test, including its expected values. Build inputs from real SCT types (events, configs, `invoke.Result`) and plain data, and give external systems canned return values. A stand-in mock gets `spec=RealClass` (AP-6, AP-15, P-14).

### Assert Behavior, Exactly

**Each assert checks something the code under test produced, in this case.**

- Assert through the public entry point: return values, raised errors, commands run, events published (AP-1, AP-14).
- A mock call that is the effect gets `assert_called_once_with(...)` with every argument (AP-13).
- Errors are checked with `pytest.raises(SpecificError, match="...")`, never `pytest.raises(Exception)` (AP-13).
- Assert the complete result: the whole command string, every field the test put in, exact counts, and `assert_not_called()` for the path not taken (AP-13).
- Declarations are not behavior: constants, nemesis flags, registry discovery, `__all__`, static yaml, and signatures are not test subjects (AP-8).
- Every claim in the test name has an assert (P-17).
- Before calling a test done, break the code under test and confirm the test fails (P-18).

### Test What the Change Adds, Once

**Every branch, guard, and changed argument the diff adds gets a case. A bug fix gets cases that fail on the pre-fix code, on an assert about the bug.**

Test through the caller path, not only the helpers (AP-17, P-18). Before adding a test, search `unit_tests/` and the linters for one that already covers the behavior. After a change, delete tests it made redundant and rename tests whose asserts outgrew their names (AP-18).

### Inputs From the Test, Expected Values From Intent

**Inputs live in the test or in `unit_tests/test_data/`. Expected values come from what the code should do.**

A bug found in a test run gets the real log lines or command output from that run. Expected values come from the issue and the intended behavior, never from copying the code's current output (AP-9, AP-16).

### Test Non-Python Code With Its Own Runtime

**Bash, Groovy, and other non-Python code is checked by running it.** In order of preference: run it with its own runtime or test tool; test the Python decision around it (the function that picks values or branches, or how Python handles command output); otherwise run it in a real job, link the run in the PR, and write no unit test (AP-7).

### Public Names and a Readable Layout

**Test modules read top to bottom: imports, constants, fixtures, data builders, tests.**

- No leading underscore on fixtures, helpers, constants, or new production functions. If a test calls it, it is public (AP-12, P-19).
- Shared fixtures go in `conftest.py`. One subject per module; a module with many groups becomes a package (P-19).
- Helper classes are defined at module level and instantiated in fixtures.
- Every test and fixture has a docstring with the goal of the check. Explanations go there, not in comments next to asserts.

### Events Fixtures for Event System Tests

**Tests that publish or read SCT events use the `events_function_scope` fixture.**

`events_function_scope` (from `unit_tests/conftest.py`) creates a fully isolated events system per test: fresh temp directory, events device, and registry patcher. Read the raw events log with `events_function_scope.get_raw_events_log()` and the events logger with `events_function_scope.get_events_logger()`. Use `events` (module scope) when many tests share expensive event setup and don't interact. When every test in a package needs it, make it `autouse` in that package's `conftest.py`.

## When to Use

- Creating a new test file in `unit_tests/unit/`
- Adding test cases to an existing unit test module in `unit_tests/unit/`
- Mocking AWS, GCE, Azure, or other cloud services in tests
- Setting up pytest fixtures for SCT components
- Debugging a unit test that is failing, slow, or flaky
- Converting unittest-style tests to pytest style

## When NOT to Use

- The test can only pass with a running Docker daemon, real cloud credentials, or a live endpoint: that is an integration test, so use the `writing-integration-tests` skill. If every external call can be mocked, it is a unit test. Files in `tmp_path` and a subprocess that runs a local binary or one of our scripts still make a unit test (#14752). A binary that the hydra image lacks gets a `pytest.skip` when `shutil.which` doesn't find it.
- Running or configuring CI pipelines: edit Jenkins pipeline files directly
- Writing functional tests for K8s operators: see `functional_tests/`
- Fixing production code bugs: edit the source in `sdcm/` directly

## Quick Reference: Test Infrastructure

### Autouse Fixtures (Always Active)

These fixtures run automatically for every unit test:

| Fixture | Defined in | Scope | Purpose |
|---------|------------|-------|---------|
| `fake_remoter` | `unit_tests/unit/conftest.py` | function | Blocks real SSH; sets `FakeRemoter` as default remoter (returns the **class**, not an instance) |
| `fake_provisioner` | `unit_tests/conftest.py` | session | Registers `FakeProvisioner` for cloud provisioning |
| `fake_region_definition_builder` | `unit_tests/conftest.py` | session | Registers `FakeDefinitionBuilder` for regions |
| `fixture_cleanup_continuous_events_registry` | `unit_tests/conftest.py` | function | Cleans up event registry between tests |

**AWS, GCE, and Azure HTTP calls are not blocked.** Mock them per test with `unittest.mock.patch`, `patch.object`, `monkeypatch`, or `moto`. Functions often patched: `convert_name_to_ami_if_needed`, `find_scylla_repo`, `get_arch_from_instance_type`, and `KeyStore` methods. Use `patch.object(KeyStore, "method_name", ...)` for `KeyStore`, which 20+ modules import by name (P-11).

### On-Demand Fixtures (from parent `unit_tests/conftest.py`)

Request these by name in your test function signature:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `params` | function | SCT configuration with `SCT_CLUSTER_BACKEND=docker` |
| `events` | module | Event system with mocked devices |
| `events_function_scope` | function | Event system per test (cleaner isolation) |
| `test_data_dir` | function | Path to `unit_tests/test_data/` |
| `prom_address` | session | Prometheus metrics server address |
| `monkeypatch` | function | Pytest built-in for patching env vars and attributes |
| `tmp_path` | function | Pytest built-in temporary directory |

For nemesis tests, `unit_tests/unit/nemesis/monkey/conftest.py` provides `base_runner`, a `TestRunner` with two data nodes and deterministic `random.choice`. Extend it with a module-level `runner` fixture.

### Test Markers

| Marker | Purpose | Unit Test Usage |
|--------|---------|-----------------|
| `@pytest.mark.sct_config(files="...")` | Loads specific SCT config | Use when testing config-dependent code |
| `@pytest.mark.parametrize` | Test multiple inputs | One test per behavior, a `pytest.param(id=...)` per case |
| `@pytest.mark.integration` | Marks integration tests | Belongs to the `writing-integration-tests` skill; unit tests carry no marker |

## Quick Reference: Mocking Patterns

### Pattern 1: monkeypatch for Environment Variables

```python
def test_config_backend_from_env(monkeypatch):
    """SCT_CLUSTER_BACKEND selects the backend."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-123")

    assert SCTConfiguration().get("cluster_backend") == "aws"
```

### Pattern 2: patch the Client, Assert the Exact Call

```python
def test_s3_download_dir_downloads_each_file(tmp_path):
    """Each object under the prefix lands at its relative path; directory markers are skipped."""
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": [{"Key": "logs/a.log"}, {"Key": "logs/sub/"}]}

    with patch("boto3.client", return_value=client):
        s3_download_dir("bucket", "logs", str(tmp_path))

    client.download_file.assert_called_once_with("bucket", "logs/a.log", str(tmp_path / "a.log"))
```

### Pattern 3: moto for Full AWS Service Mocking

```python
@mock_aws
def test_ec2_provisioning_creates_one_instance():
    """run_instances against moto registers a single reservation."""
    ec2 = boto3.client("ec2", region_name="us-east-1")
    ec2.run_instances(ImageId="ami-12345", MinCount=1, MaxCount=1)

    assert len(ec2.describe_instances()["Reservations"]) == 1
```

### Pattern 4: FakeRemoter for Remote Commands

```python
def test_node_reads_status_and_config(fake_remoter):
    """Remote commands get their output from FakeRemoter.result_map."""
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN 10.0.0.1", exited=0),
        re.compile(r"cat /etc/scylla/scylla.yaml"): Result(stdout="cluster_name: test", exited=0),
    }
    ...
```

`fake_remoter` is the `FakeRemoter` class, and `result_map` is a class attribute, so set it inside each test.

### Pattern 5: Real Objects as Inputs

```python
def test_format_error_raises_on_error_event():
    """An error event raises NemesisSubTestFailure naming its step."""
    event = TestStepEvent(step="Run stress", publish_event=False)
    event.add_error(["something went wrong"])

    with pytest.raises(NemesisSubTestFailure, match="Step: Run stress"):
        SlaMonkeyBase.format_error_for_sla_test_and_raise([None, event])
```

## Debugging Unit Tests

### Test Hangs or Is Slow

1. **Unmocked network call.** Add `-s` to see stdout and look for connection attempts:
   ```bash
   uv run python -m pytest unit_tests/unit/test_module.py::test_function -v -s
   ```
2. **Unmocked `wait.wait_for` loop.** If code uses SCT's `wait_for`, patch it to call the function once, or lower the timeout.
3. **Fixed waits and retries.** Patch `time.sleep` where the code calls it, in an `autouse` fixture, and run retry loops on a fake clock. Find the slow tests with `--durations=10` (P-22).
4. **Real objects that connect.** Point them at a refused local address (`127.0.0.1:1`), not an unroutable one (AP-6).

### Test Passes Alone but Fails in Parallel

1. **Shared global state.** Move it into fixtures (P-6).
2. **Environment variables.** Set them with `monkeypatch.setenv`, and remove variables the shell or CI exports (P-8).
3. **`FakeRemoter.result_map`.** It is a class attribute; set it per test.
4. **Singleton caches.** Clear them in an `autouse` fixture before and after `yield` (P-16).
5. **Timing.** A count or duration that depends on machine speed (P-22).

### FakeRemoter Raises ValueError

`No fake result specified for command: <cmd>` means the code under test runs a command that `FakeRemoter.result_map` has no entry for. Add the command pattern to `result_map` (P-4).

### Test Fails With Import Errors

On `ModuleNotFoundError`:
1. Sync the virtualenv: `uv sync`
2. Put the import at the top of the file (P-3)

## Test Naming

Name tests `test_<function>_<scenario>_<expected>`:

```python
def test_comparable_scylla_version_unparsable_string_raises_value_error(): ...
def test_config_missing_backend_raises_value_error(): ...
def test_health_check_single_node_failure_does_not_block_others(): ...
```

Name fixtures for what they provide (`restore_snapshot_runner`, `stub_common_helpers`), and use whole words for variables (`tester`, `db_node`).

Every test function and fixture gets a docstring (Google format) that says what the test checks and why it matters. The name says *what*; the docstring says the goal. Notes that would sit next to an assert belong in the docstring:

```python
def test_stop_releases_the_pool_worker(logger):
    """stop() must retire the pool worker; a live one would block interpreter shutdown."""
```

## Converting unittest-Style Tests

A conversion leaves no classes behind (#13952):

- `setUp` and `setup_method` become fixtures. One that every test in the class uses becomes `autouse`.
- `tearDown` becomes the code after `yield` in that fixture.
- `self.assertEqual(a, b)` becomes `assert a == b`, and `self.assertRaises` becomes `pytest.raises(..., match=...)`.
- Methods become module-level `def test_*` functions. A class that grouped one subject becomes its own module, with its fixtures in a shared `conftest.py` when several modules need them.
- Methods that differ only in literals become one parametrized test (AP-2).

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
| [common-pitfalls.md](references/common-pitfalls.md) | Mocking, isolation, timing, and structure rules P-1 through P-22 |
| [anti-patterns.md](references/anti-patterns.md) | Test design rules AP-1 through AP-18: what to test and what to assert |

| Workflow | Purpose |
|----------|---------|
| [write-a-unit-test.md](workflows/write-a-unit-test.md) | 4-phase process for writing a new unit test |

The workflow in brief:
1. **Identify**: list every behavior the diff adds, find the existing tests, list external dependencies, and confirm a unit test is the right tool (AP-7, AP-8, AP-17, AP-18).
2. **Set up**: reuse existing fixtures, put new setup in fixtures, and mock only at the external boundary.
3. **Write**: one parametrized test per behavior, with docstrings, exact asserts, and edge cases.
4. **Verify**: run the file, run bug-fix cases against the pre-fix code, mutation-check each test, check isolation, speed, and layout, and run pre-commit.

## Success Criteria

A well-written SCT unit test:

- [ ] Lives in `unit_tests/unit/` with a `test_*.py` filename, in a module about one subject
- [ ] Is a module-level pytest function with fixtures and plain `assert`
- [ ] Is one parametrized test per behavior, with a `pytest.param(id=...)` per case, not near-duplicate functions (AP-2)
- [ ] Gets its setup from fixtures; its body is mostly act and assert (P-15, P-21)
- [ ] Makes zero real network calls (all external services mocked)
- [ ] Exercises real `sdcm` classes, built through their constructors, with real SCT types and plain data as inputs, and no copy of production logic (AP-6, AP-15)
- [ ] Asserts on behavior the code produced in this case, not on constants, flags, discovery, or arranged preconditions (AP-8, AP-14)
- [ ] Asserts mock interactions with `assert_called_once_with(...)` and every argument, and errors with `pytest.raises(SpecificError, match=...)` (AP-13)
- [ ] Asserts what the test name claims (P-17)
- [ ] Covers every branch and changed argument the diff adds, and duplicates no existing test (AP-17, AP-18)
- [ ] Fails when the code under test is broken (mutation-checked); bug-fix cases fail on the pre-fix code (P-18)
- [ ] Takes milliseconds and never depends on timing (P-22)
- [ ] Asserts on Python behavior; non-Python code is checked by running it (AP-7)
- [ ] Takes inputs from the test or `unit_tests/test_data/`, with real log lines for bug fixes and expected values from intent (AP-9, AP-16)
- [ ] Uses public names only: no leading underscore in the test module or in new production code (AP-12, P-19)
- [ ] Has fixtures and helpers above the first test, or in `conftest.py` (P-19)
- [ ] Uses `monkeypatch` for environment variables and `tmp_path` for files
- [ ] Passes in isolation, in parallel, and in random order
- [ ] Has all imports at the top of the file
- [ ] Has a docstring on every test and fixture that says what behavior it checks (Google format)
- [ ] Passes `uv run sct.py pre-commit` checks
