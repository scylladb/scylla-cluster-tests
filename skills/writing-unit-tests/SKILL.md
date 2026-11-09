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

### Exercise Real Code, Mock Only Its I/O

**Construct the real `sdcm` class and mock only its external boundary: the network, the file system, the remoter, the cloud SDK client.**

Then the code under test is the code that runs. Unit tests make no real network calls. The autouse `fake_remoter` fixture blocks SSH, but HTTP-based services (boto3, requests, REST APIs) are not blocked, so mock them. Build inputs from real SCT types and plain data, and give a stand-in mock `spec=RealClass` (UT-10, UT-12, UT-13).

### One Parametrized Test per Behavior, Setup in Fixtures

**Tests are flat module-level `def test_*` functions. Cases are `pytest.param(id=...)` entries, and mocks, patches, and collaborators come from fixtures.**

Fixture injection, `autouse`, parametrize, and `pytest-xdist` depend on this style, and these are the two most frequent review requests on SCT unit tests. The test body is the act and its asserts (UT-5, UT-6, UT-7).

### Assert Behavior, Exactly

**Each assert checks something the code under test produced, in this case.**

An exact assert catches a wrong argument and shows the difference, where a loose one passes for the wrong reason. Assert the full call with `assert_called_once_with(...)`, errors with `pytest.raises(SpecificError, match="...")`, and the complete result. Declarations (constants, nemesis flags, registry discovery, static yaml) are not behavior (UT-3, UT-21, UT-22, UT-23).

### Test What the Change Adds, Once

**Every branch, guard, and changed argument the diff adds gets a case. A bug fix gets cases that fail on the pre-fix code.**

A test that cannot fail proves nothing, so break the code under test and confirm each new test fails. Before adding a test, search `unit_tests/` and the linters for one that already covers the behavior (UT-1, UT-2, UT-25).

### Every Test Runs Alone

**Every test passes on its own, in any order, and in parallel.**

`sct.py unit-tests` runs them with `pytest-xdist` (`-n4`). Check order independence locally with `--random-order`. State comes from fixtures, environment variables from `monkeypatch`, and files from `tmp_path`, and timing never decides a result (UT-16, UT-17, UT-18).

## When to Use

- Creating a new test file in `unit_tests/unit/`
- Adding test cases to an existing unit test module in `unit_tests/unit/`
- Mocking AWS, GCE, Azure, or other cloud services in tests
- Setting up pytest fixtures for SCT components
- Debugging a unit test that is failing, slow, or flaky
- Converting unittest-style tests to pytest style

## When NOT to Use

- The test can only pass with a running Docker daemon, real cloud credentials, or a live endpoint: that is an integration test, so use the `writing-integration-tests` skill. If every external call can be mocked, it is a unit test, and so is a test that writes files in `tmp_path` or runs a local binary (UT-16).
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

**AWS, GCE, and Azure HTTP calls are not blocked.** Mock them per test with `unittest.mock.patch`, `patch.object`, `monkeypatch`, or `moto`. Functions often patched: `convert_name_to_ami_if_needed`, `find_scylla_repo`, `get_arch_from_instance_type`, and `KeyStore` methods. Use `patch.object(KeyStore, "method_name", ...)` for `KeyStore`, which 20+ modules import by name (UT-14).

### On-Demand Fixtures (from parent `unit_tests/conftest.py`)

Request these by name in your test function signature:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `params` | function | SCT configuration with `SCT_CLUSTER_BACKEND=docker` |
| `events` | module | Event system with mocked devices |
| `events_function_scope` | function | Event system per test (cleaner isolation) |
| `test_data_dir` | session | Path to `unit_tests/test_data/` |
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

## Workflow

Follow [write-a-unit-test.md](workflows/write-a-unit-test.md), and read the reference for a phase before starting it.

| Phase | What happens | Read first |
|-------|--------------|------------|
| 1. Identify | List every behavior the diff adds, find the existing tests, confirm a unit test is the right tool | [what-to-test.md](references/what-to-test.md) |
| 2. Set up | Reuse fixtures, put new setup in fixtures, mock at the external boundary | [fixtures-and-layout.md](references/fixtures-and-layout.md), [mocking-and-isolation.md](references/mocking-and-isolation.md) |
| 3. Write | One parametrized test per behavior, with docstrings and exact asserts | [asserts-and-inputs.md](references/asserts-and-inputs.md) |
| 4. Verify | Run the file, mutation-check each test, check isolation and speed, run pre-commit | Success Criteria below |

| Other task | Read |
|------------|------|
| Debug a failing, slow, or flaky test | Debugging below, then [mocking-and-isolation.md](references/mocking-and-isolation.md) |
| Convert unittest-style tests | "Converting unittest-Style Tests" in [fixtures-and-layout.md](references/fixtures-and-layout.md) |
| Review tests someone else wrote | Success Criteria below, then the rule each failed item names |

## Debugging Unit Tests

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Hangs or is slow | An unmocked network call, an unmocked `wait.wait_for` loop, a fixed `time.sleep`, or a real object waiting on an unroutable address | Run with `-s` and `--durations=10`. Mock the call, patch `wait_for` to call the function once, patch the sleep (UT-10, UT-18) |
| Passes alone, fails in parallel | Shared global state, a variable the shell or CI exports, `FakeRemoter.result_map` (a class attribute), a Singleton cache, or a timing assert | Move the state into fixtures (UT-15, UT-16, UT-17, UT-18) |
| `ValueError: No fake result specified for command` | The code runs a command that `FakeRemoter.result_map` has no entry for | Add the command pattern (UT-15) |
| `ModuleNotFoundError` | A stale virtualenv, or an import inside a function | Run `uv sync`, and put the import at the top of the file (UT-9) |

## Running Tests

```bash
# Run all unit tests (excludes integration)
uv run sct.py unit-tests

# Run a specific test file
uv run sct.py unit-tests -t unit/test_config.py

# Run one test with output, without parallel workers
uv run python -m pytest unit_tests/unit/test_config.py::test_function_name -v -s -n0

# Coverage for a single module
uv run python -m pytest unit_tests/unit/test_config.py --cov=sdcm.sct_config --cov-report=term-missing
```

## Reference Index

| File | Content |
|------|---------|
| [what-to-test.md](references/what-to-test.md) | UT-1 to UT-4: cover the change, test each behavior once, declarations, non-Python code |
| [fixtures-and-layout.md](references/fixtures-and-layout.md) | UT-5 to UT-9: parametrize, fixtures, file layout, names and docstrings, imports, unittest conversion |
| [mocking-and-isolation.md](references/mocking-and-isolation.md) | UT-10 to UT-20: real classes, shared fakes, patching, FakeRemoter, environment, Singletons, timing |
| [asserts-and-inputs.md](references/asserts-and-inputs.md) | UT-21 to UT-25: exact asserts, inputs and expected values, mutation checks |

| Workflow | Purpose |
|----------|---------|
| [write-a-unit-test.md](workflows/write-a-unit-test.md) | 4-phase process for writing a new unit test |

## Success Criteria

This checklist is the one-line form of every rule. The workflow's Phase 4 and the `code-review` skill point here instead of restating it. `.coderabbit.yaml` carries a short copy for the bot, so change both together.

A well-written SCT unit test:

- [ ] Lives in `unit_tests/unit/` with a `test_*.py` filename, in a module about one subject (UT-7)
- [ ] Is a module-level pytest function with plain `assert`, with fixtures and helpers above the first test or in `conftest.py` (UT-7)
- [ ] Is one parametrized test per behavior, with a `pytest.param(id=...)` per case, not near-duplicate functions (UT-5)
- [ ] Gets its mocks, patches, and collaborators from fixtures; its body is act and assert (UT-6)
- [ ] Makes zero real network calls (UT-10)
- [ ] Exercises real `sdcm` classes, built through their constructors, with real SCT types and plain data as inputs, and no copy of production logic (UT-10, UT-12)
- [ ] Asserts on behavior the code produced in this case, not on constants, flags, discovery, or arranged preconditions (UT-3, UT-22)
- [ ] Asserts mock interactions with `assert_called_once_with(...)` and every argument, and errors with `pytest.raises(SpecificError, match=...)` (UT-21)
- [ ] Asserts what the test name claims (UT-23)
- [ ] Covers every branch and changed argument the diff adds, and duplicates no existing test (UT-1, UT-2)
- [ ] Fails when the code under test is broken (mutation-checked); bug-fix cases fail on the pre-fix code (UT-25)
- [ ] Takes milliseconds and never depends on timing (UT-18)
- [ ] Asserts on Python behavior; non-Python code is checked by running it (UT-4)
- [ ] Takes inputs from the test or `unit_tests/test_data/`, with real log lines for bug fixes and expected values from intent (UT-24)
- [ ] Uses public names only: no leading underscore in the test module or in new production code (UT-8)
- [ ] Has a docstring on every test and fixture that says what behavior it checks (Google format) (UT-8)
- [ ] Has all imports at the top of the file (UT-9)
- [ ] Uses `monkeypatch` for environment variables and `tmp_path` for files (UT-16)
- [ ] Passes in isolation, in parallel, and in random order (UT-16, UT-17)
- [ ] Passes `uv run sct.py pre-commit` checks
