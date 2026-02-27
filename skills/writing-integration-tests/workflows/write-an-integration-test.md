# Writing an Integration Test

A 4-phase process for creating a new integration test in the SCT repository.

---

## Phase 1: Determine If Integration Testing Is Needed

**Entry:** You have a feature or component that may need testing against real external services.

**Actions:**

1. **Ask: can this be tested with mocking?** If the logic can be validated with fake inputs and mocked outputs, write a unit test instead. Integration tests are slower and more fragile.

2. **Identify the external services required.** Common SCT integration test backends:

   | Service | When Needed | Fixture/Setup |
   |---------|-------------|---------------|
   | **Docker (Scylla)** | CQL queries, stress tools, Alternator | `docker_scylla` fixture |
   | **Docker (Vector Store)** | Vector search operations | `docker_vector_store` fixture |
   | **AWS (EC2, S3, IAM)** | Provisioning, AMI operations, storage | `moto` library or real AWS credentials |
   | **OCI** | Oracle Cloud integration | OCI credentials + `pytest.mark.skipif` |
   | **Kubernetes** | K8s operator tests | `LocalKindCluster` setup via `sct.py integration-tests` |
   | **GCE** | Google Cloud operations | GCE credentials |
   | **Azure** | Azure provisioning | Azure credentials |

3. **Check if an existing test file covers your area.** Search `unit_tests/` for integration tests on the same module:
   ```bash
   grep -rl "@pytest.mark.integration" unit_tests/ | xargs grep "module_name"
   ```

4. **Choose the test file location.** Integration tests live in `unit_tests/` alongside unit tests, distinguished by the `@pytest.mark.integration` marker.

**Exit:** Decision made: integration test is needed. External services identified and documented.

---

## Phase 2: Set Up Fixtures and Service Access

**Entry:** Phase 1 complete. External service requirements known.

**Actions:**

1. **For Docker Scylla tests**, use the built-in `docker_scylla` fixture:
   ```python
   @pytest.mark.integration
   @pytest.mark.docker_scylla_args(ssl=True)
   def test_cql_over_ssl(docker_scylla, params):
       node = docker_scylla
       # node.cql_connection(), node.run(), etc.
   ```

2. **For cloud credential-dependent tests**, add skip conditions:
   ```python
   pytestmark = [
       pytest.mark.integration,
       pytest.mark.skipif(
           not os.environ.get("AWS_ACCESS_KEY_ID"),
           reason="AWS credentials not configured",
       ),
   ]
   ```

3. **For tests sharing expensive resources**, use `xdist_group`:
   ```python
   @pytest.mark.integration
   @pytest.mark.xdist_group("my-test-group")
   def test_step_1(docker_scylla):
       ...
   ```

4. **Create cleanup fixtures for any resources you create:**
   ```python
   @pytest.fixture
   def my_resource():
       resource = create_resource()
       yield resource
       resource.cleanup()  # Always runs, even on failure
   ```

5. **Document external service requirements** in the test module docstring:
   ```python
   """Integration tests for S3 storage operations.

   External services: AWS S3 (mocked via moto) or real AWS credentials
   Prerequisites: None (moto handles mocking)
   """
   ```

**Exit:** Fixtures, skip conditions, and cleanup handlers are in place.

---

## Phase 3: Write Test Functions

**Entry:** Phase 2 complete. Infrastructure ready.

**Actions:**

1. **Add `@pytest.mark.integration` to every test** (or use module-level `pytestmark`):
   ```python
   @pytest.mark.integration
   def test_stress_tool(docker_scylla, params):
       ...
   ```

2. **Follow the Arrange-Act-Assert pattern:**
   ```python
   @pytest.mark.integration
   def test_cql_insert(docker_scylla):
       # Arrange
       session = docker_scylla.cql_connection()
       session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

       # Act
       session.execute("INSERT INTO test.data (id, value) VALUES (1, 'hello')")
       result = session.execute("SELECT value FROM test.data WHERE id = 1")

       # Assert
       assert result.one().value == "hello"
   ```

3. **Set timeouts for operations that could hang:**
   ```python
   from sdcm import wait

   wait.wait_for(
       func=check_condition,
       step=2,
       text="Waiting for operation",
       timeout=120,
       throw_exc=True,
   )
   ```

4. **Use parametrize for testing multiple configurations:**
   ```python
   @pytest.mark.integration
   @pytest.mark.parametrize("consistency", ["ONE", "QUORUM", "ALL"])
   def test_read_consistency(docker_scylla, consistency):
       ...
   ```

**Exit:** All test functions written with proper markers, timeouts, and assertions.

---

## Phase 4: Run and Validate

**Entry:** Phase 3 complete. Tests written.

**Actions:**

1. **Run integration tests:**
   ```bash
   uv run sct.py integration-tests -t test_your_module.py
   ```

2. **Run a single test with verbose output:**
   ```bash
   uv run python -m pytest unit_tests/test_your_module.py::test_function -v -s -m integration
   ```

3. **Check Docker container cleanup.** After test run:
   ```bash
   docker ps -a | grep scylla
   # Should show no leftover test containers
   ```

4. **Verify skip conditions work.** Remove the required credentials/env vars and confirm the test skips with a clear reason:
   ```bash
   unset AWS_ACCESS_KEY_ID
   uv run python -m pytest unit_tests/test_your_module.py -v -m integration
   # Should show: SKIPPED (reason: AWS credentials not configured)
   ```

5. **Run pre-commit checks:**
   ```bash
   uv run sct.py pre-commit
   ```

**Exit:** All tests pass with services available, skip cleanly without them, no resource leaks.
