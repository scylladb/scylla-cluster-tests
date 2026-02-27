# Writing a Unit Test

A 4-phase process for creating a new unit test in the SCT repository.

---

## Phase 1: Identify What to Test

**Entry:** You have a function, method, or class that needs test coverage.

**Actions:**

1. **Locate the source code.** Find the module under `sdcm/` that contains the code to test. Note all public functions and their expected inputs/outputs.

2. **Check for existing tests.** Search `unit_tests/` for tests covering the same module:
   ```bash
   grep -rl "from sdcm.module_name import" unit_tests/
   ```

3. **Identify external dependencies.** List any network calls, file I/O, cloud API calls, or remote commands. These must be mocked in the unit test.

4. **Decide the test file name.** Follow the pattern `unit_tests/test_<module_name>.py`. If an existing file covers the module, add tests to it.

**Exit:** You know what to test, what to mock, and where to put the test.

---

## Phase 2: Set Up Fixtures and Mocks

**Entry:** Phase 1 complete. Dependencies identified.

**Actions:**

1. **Use existing fixtures when available.** Check `unit_tests/conftest.py` for shared fixtures:
   - `params` — SCT configuration (auto-sets `SCT_CLUSTER_BACKEND=docker`)
   - `events` / `events_function_scope` — Event system setup
   - `fake_remoter` — Blocks real SSH (autouse, always active)
   - `prom_address` — Prometheus metrics server
   - `docker_scylla` — Real Scylla container (integration tests only)

2. **Mock external services at the boundary.** Use `unittest.mock.patch` or `monkeypatch`:
   ```python
   from unittest.mock import patch, MagicMock

   def test_something(monkeypatch):
       monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
       with patch("boto3.client") as mock_client:
           mock_client.return_value.describe_instances.return_value = {...}
           # test code
   ```

3. **Use moto for AWS-heavy tests.** If the function under test makes multiple AWS API calls, `moto` provides full mock AWS services:
   ```python
   from moto import mock_aws

   @mock_aws
   def test_aws_provisioning():
       ec2 = boto3.client("ec2", region_name="us-east-1")
       # All AWS calls are intercepted
   ```

4. **Set up FakeRemoter result_map for remote commands:**
   ```python
   import re
   from invoke import Result

   def test_remote_cmd(fake_remoter):
       fake_remoter.result_map = {
           re.compile(r"nodetool status"): Result(stdout="UN 10.0.0.1", exited=0),
       }
   ```

5. **Use `tmp_path` for file-based tests.** Avoid hardcoded paths:
   ```python
   def test_config_file(tmp_path):
       config = tmp_path / "config.yaml"
       config.write_text("key: value")
   ```

**Exit:** All mocks and fixtures are identified and coded.

---

## Phase 3: Write Test Functions

**Entry:** Phase 2 complete. Fixtures and mocks ready.

**Actions:**

1. **Follow the Arrange-Act-Assert pattern:**
   ```python
   def test_parse_version():
       # Arrange
       version_string = "5.2.1-0.20230101.abc123"

       # Act
       result = parse_version(version_string)

       # Assert
       assert result.major == 5
       assert result.minor == 2
   ```

2. **Use `@pytest.mark.parametrize` for multiple cases:**
   ```python
   @pytest.mark.parametrize("input_val,expected", [
       ("5.2.1", (5, 2, 1)),
       ("2024.1.0", (2024, 1, 0)),
       ("invalid", None),
   ])
   def test_parse_version(input_val, expected):
       assert parse_version(input_val) == expected
   ```

3. **Test error conditions with `pytest.raises`:**
   ```python
   def test_invalid_config():
       with pytest.raises(ValueError, match="Invalid backend"):
           SCTConfiguration(cluster_backend="nonexistent")
   ```

4. **Use `assert` directly — not `self.assertEqual`:**
   ```python
   # Good
   assert result == expected
   assert "key" in data
   assert len(items) == 3

   # Bad (unittest style)
   self.assertEqual(result, expected)
   ```

5. **Keep tests focused.** One behavior per test function. Test name should describe the expected behavior: `test_parse_version_returns_none_for_invalid_input`.

**Exit:** All test functions written with clear assertions.

---

## Phase 4: Run and Validate

**Entry:** Phase 3 complete. Test functions written.

**Actions:**

1. **Run the specific test file:**
   ```bash
   uv run sct.py unit-tests -t test_your_module.py
   ```

2. **Run with verbose output for debugging:**
   ```bash
   uv run python -m pytest unit_tests/test_your_module.py -v -s
   ```

3. **Check for accidental network access.** If any test hangs or is slow, it may be making real network calls. Add mocking.

4. **Verify test isolation.** Run the test file multiple times and in different order:
   ```bash
   uv run python -m pytest unit_tests/test_your_module.py -v -p random-order
   ```

5. **Run pre-commit checks:**
   ```bash
   uv run sct.py pre-commit
   ```

6. **Verify the test is excluded from integration runs.** Do NOT add `@pytest.mark.integration` — unit tests run with `-m "not integration"` by default.

**Exit:** All tests pass, no external service calls, pre-commit clean.
