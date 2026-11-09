# Writing a Unit Test

A 4-phase process for creating a new unit test in the SCT repository. Each phase names the reference to read first; the rule IDs (UT-n) point into those files.

---

## Phase 1: Identify What to Test

**Entry:** You have a function, method, or class that needs test coverage.

**Read first:** `references/what-to-test.md` (UT-1 to UT-4).

**Actions:**

1. **Locate the source code.** Find the module under `sdcm/` that contains the code to test. Note its public functions and their expected inputs and outputs.

2. **List every behavior the change adds or changes.** Each new branch, guard, parser path, and changed call-site argument gets a case, reached through the caller path (UT-1). Logic in a module no test can import moves to `sdcm/` first.

3. **Find existing tests for the same behavior.** Search all of `unit_tests/`, not only the obvious file, and check whether a linter already covers it. An existing test may already cover the code, or need only one more `pytest.param` (#13924, #15851). Note tests the change makes redundant or misnamed; they are deleted or renamed (UT-2):
   ```bash
   grep -rl "from sdcm.module_name import" unit_tests/
   grep -rn "function_name" unit_tests/
   ```

4. **Identify external dependencies.** List the network calls, file I/O, cloud API calls, remote commands, fixed waits, and environment variables the code reads. These are mocked, patched, or set by the test.

5. **Choose the test module.** Add to an existing module when the new tests cover the same subject and share its fixtures. Start a new module when they need nothing from the existing file (#16633). A module that already holds many groups becomes a package with one module per group and a shared `conftest.py` (#15857). New modules go in `unit_tests/unit/`, mirroring the production package: `unit_tests/unit/<package>/test_<module>.py` (UT-7).

6. **Confirm a unit test is the right tool.** There must be Python behavior to test.
   - Bash, Groovy, and other non-Python code (a generated script, `vars/*.groovy`, a `*.jenkinsfile`, a `*.sh` file) is checked by running it or through the Python decision around it (UT-4).
   - Declarations such as constants, nemesis flags, registry discovery, and `__all__` are not test subjects (UT-3).
   - Code that needs a simulator or `__wrapped__` to reach is extracted first, or left to integration tests (UT-12).

7. **For a bug fix, collect the evidence.** Save the log lines or command output from the failed run. They become the test input (UT-24).

**Exit:** You know every behavior to test, what to mock, and which module the tests go in.

---

## Phase 2: Set Up Fixtures and Mocks

**Entry:** Phase 1 complete. Dependencies identified.

**Read first:** `references/fixtures-and-layout.md` (UT-5 to UT-9) and `references/mocking-and-isolation.md` (UT-10 to UT-20).

**Actions:**

1. **Start from existing fixtures.**
   - `params`: SCT configuration (sets `SCT_CLUSTER_BACKEND=docker`)
   - `events` / `events_function_scope`: event system setup
   - `fake_remoter`: blocks real SSH (autouse; returns the FakeRemoter **class**)
   - `test_data_dir`: path to `unit_tests/test_data/`
   - `base_runner` (nemesis tests): `TestRunner` with two data nodes and deterministic `random.choice`

2. **Write new setup as fixtures.** Mocks, patches, and the collaborators the object under test needs are fixtures at the top of the module. Each gets a public name that says what it provides, and a docstring (UT-6, UT-7):
   ```python
   @pytest.fixture()
   def runner(base_runner):
       """``base_runner`` with a single non-system table for modify-table tests."""
       base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
       return base_runner
   ```
   - A variation per test uses a fixture factory, so the test passes only what changes.
   - Patches every test in the module needs are an `autouse` fixture.
   - A fixture every module in a package needs is `autouse` in the package's `conftest.py`.

3. **Mock external services at the boundary** with `unittest.mock.patch` or `monkeypatch`, and return real types (UT-10, UT-13):
   ```python
   @pytest.fixture
   def ec2_client():
       """EC2 client answering describe_instances with one running instance."""
       client = MagicMock()
       client.describe_instances.return_value = {"Reservations": [{"Instances": [{"State": {"Name": "running"}}]}]}
       with patch("boto3.client", return_value=client):
           yield client
   ```

4. **Use moto for AWS-heavy code.** When the code makes many AWS calls, `@mock_aws` provides full in-memory services.

5. **Give FakeRemoter a result for every command**, in a fixture (UT-15):
   ```python
   fake_remoter.result_map = {
       re.compile(r"nodetool status"): Result(stdout="UN 10.0.0.1", exited=0),
   }
   ```

6. **Write files under `tmp_path`, and set environment variables with `monkeypatch.setenv`.** Remove variables the shell or CI may export when they switch a code path (UT-16).

7. **Patch fixed waits in an `autouse` fixture**, and run retry loops on a fake clock (UT-18).

8. **Build the subject through its constructor**, with mocked collaborators. A stand-in mock gets `spec=RealClass` (UT-10, UT-13).

**Exit:** All setup is in fixtures, and every external call has a mock.

---

## Phase 3: Write Test Functions

**Entry:** Phase 2 complete. Fixtures and mocks ready.

**Read first:** `references/asserts-and-inputs.md` (UT-21 to UT-25).

**Actions:**

1. **Write one test per behavior, parametrized over its cases.** Put the expected value, and the expected exception, in the parameters (UT-5):
   ```python
   @pytest.mark.parametrize("version_string,expected", [
       pytest.param("5.1", (5, 1, 0, "", ""), id="major-minor"),
       pytest.param("2022.1~rc8", (2022, 1, 0, "rc8", ""), id="tilde-rc"),
       pytest.param("2022.1.3-0.20220922.539a55e35", (2022, 1, 3, "dev-0.20220922", "539a55e35"), id="dev-build"),
   ])
   def test_comparable_scylla_version_parts(version_string, expected):
       """Release, rc, and dev build strings split into major, minor, patch, pre-release, and build."""
       version = ComparableScyllaVersion(version_string)

       assert (version.v_major, version.v_minor, version.v_patch, version.v_pre_release, version.v_build) == expected
   ```

2. **Keep the body to act and assert.** The docstring states the goal. Setup comes from fixtures (UT-6):
   ```python
   def test_comparable_scylla_version_keeps_dev_build_parts():
       """A dev build string keeps its version, pre-release tag, and commit."""
       version = ComparableScyllaVersion("5.2.0-dev-0.20230109.08b3a9c786d9")

       assert str(version) == "5.2.0-dev-0.20230109+08b3a9c786d9"
   ```

3. **Assert exact calls when a mock call is the effect** (UT-21):
   ```python
   monkey.runner.cluster.get_non_system_ks_cf_list.assert_called_once_with(
       db_node=monkey.runner.target_node,
       filter_out_table_with_counter=True,
       filter_out_mv=True,
   )
   ```

4. **Test error conditions with the concrete exception and `match`** (UT-21):
   ```python
   def test_comparable_scylla_version_unparsable_string_raises():
       """A string without major.minor is rejected."""
       with pytest.raises(ValueError, match="Cannot parse provided '2023.dev' scylla_version"):
           ComparableScyllaVersion("2023.dev")
   ```

5. **Make each assert about this case** (UT-22). Check values the code produced, not preconditions the test arranged or facts that hold for every case. Assert the complete result: the whole string, every field the test put in, exact counts, and `assert_not_called()` for the path not taken (UT-21). Expected numbers are worked-out literals, not the formula under test re-run (UT-24).

6. **Cover the edges as parameters:** empty inputs (`""`, `[]`, `{}`, `None`), boundary values, malformed data, and error paths.

7. **Name tests `test_<subject>_<scenario>_<expected>`**, and give every test and fixture a docstring (UT-8).

**Exit:** One test per behavior, each case a `pytest.param`, every assert tied to the case.

---

## Phase 4: Run and Validate

**Entry:** Phase 3 complete. Test functions written.

**Actions:**

1. **Run the test file:**
   ```bash
   uv run sct.py unit-tests -t unit/test_your_module.py
   uv run python -m pytest unit_tests/unit/test_your_module.py -v -s
   ```

2. **Check speed and network access.** A test that hangs or runs slowly is likely making a real call or waiting on a real sleep. Find it with `--durations=10`, then mock the call or patch the wait (UT-18).

3. **Check isolation.** Run the file in random order and in parallel:
   ```bash
   uv run python -m pytest unit_tests/unit/test_your_module.py -v --random-order -n2
   ```

4. **Mutation-check each new test.** Break the code under test the way the test claims to catch (delete the stub, invert the branch, return early), confirm the test fails, then revert. For a bug fix, save the fix as a patch, then run every new case against the pre-fix code: each must fail on an assert about the bug, not on an `AttributeError` (UT-25).

5. **Check every claim in the test name has an assert** (UT-23).

6. **Review the file against the layout and naming rules:**
   ```bash
   grep -nE '^\s*(def|class) _[A-Za-z]|^_[A-Za-z]' unit_tests/unit/test_your_module.py   # prints nothing
   grep -nE '^def test_|^@pytest.fixture' unit_tests/unit/test_your_module.py           # fixtures listed first
   ```
   Then go through the Success Criteria in `SKILL.md`. It is the full list, one line per rule.

7. **Run pre-commit:**
   ```bash
   uv run sct.py pre-commit
   ```

8. **Leave the test unmarked.** Unit tests run with `-m "not integration"`.

**Exit:** All tests pass in milliseconds, each fails under its mutation, bug-fix cases fail on the pre-fix code, no external calls, layout and naming clean, pre-commit clean.
