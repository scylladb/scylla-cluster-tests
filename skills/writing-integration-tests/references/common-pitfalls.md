# Integration Test Pitfalls and Anti-Patterns

Common mistakes when writing SCT integration tests, with before/after fixes.

---

## Pitfalls

### P-1: Missing the @pytest.mark.integration Marker

Every integration test MUST have `@pytest.mark.integration`. Without it, the test runs as a unit test — where it will fail because external services are unavailable.

❌ **Bad:**
```python
def test_cql_query(docker_scylla):
    # Missing marker — runs during unit-tests and fails
    session = docker_scylla.cql_connection()
    session.execute("SELECT * FROM system.local")
```

✅ **Good:**
```python
@pytest.mark.integration
def test_cql_query(docker_scylla):
    session = docker_scylla.cql_connection()
    session.execute("SELECT * FROM system.local")
```

**For entire modules, use `pytestmark`:**
```python
import pytest

pytestmark = [pytest.mark.integration]

def test_cql_query(docker_scylla):
    # All tests in this module are integration tests
    ...
```

---

### P-2: Not Labeling External Service Dependencies

Integration tests must clearly indicate which external services they require. This helps CI systems, reviewers, and developers understand what infrastructure is needed.

❌ **Bad:**
```python
@pytest.mark.integration
def test_something():
    # What does this need? AWS? Docker? K8s? Unknown until it fails.
    result = do_cloud_thing()
```

✅ **Good — use docstrings or comments:**
```python
@pytest.mark.integration
def test_ec2_provisioning():
    """Test EC2 instance provisioning.

    External services: AWS EC2, AWS S3
    """
    ...
```

✅ **Good — use skip conditions for missing credentials:**
```python
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("OCI_CONFIG_FILE"),
        reason="OCI credentials not configured",
    ),
]
```

---

### P-3: Not Cleaning Up External Resources

Integration tests that create cloud resources (instances, buckets, containers) MUST clean them up, even if the test fails.

❌ **Bad:**
```python
@pytest.mark.integration
def test_create_bucket():
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket-12345")
    # Test assertions...
    # If test fails, bucket is leaked!
```

✅ **Good:**
```python
@pytest.fixture
def s3_bucket():
    s3 = boto3.client("s3")
    bucket_name = f"test-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup always runs, even on failure
    s3.delete_bucket(Bucket=bucket_name)

@pytest.mark.integration
def test_bucket_operations(s3_bucket):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key="test.txt", Body=b"hello")
```

---

### P-4: Hardcoding Docker Image Versions

Use `@pytest.mark.docker_scylla_args` to configure Docker images, not hardcoded values in test code.

❌ **Bad:**
```python
@pytest.mark.integration
def test_with_scylla():
    container = start_container("scylladb/scylla:5.2.1")  # hardcoded
```

✅ **Good:**
```python
@pytest.mark.integration
@pytest.mark.docker_scylla_args(scylla_docker_image="scylladb/scylla:5.2.1")
def test_with_scylla(docker_scylla):
    # docker_scylla fixture handles container lifecycle
    ...
```

---

### P-5: Not Waiting for Services to Be Ready

Docker containers and cloud services need time to start. Always wait for readiness before interacting.

❌ **Bad:**
```python
@pytest.mark.integration
def test_immediate_query(docker_scylla):
    # May fail if Scylla isn't ready yet
    session = connect_cql(docker_scylla.ip_address)
    session.execute("SELECT * FROM system.local")
```

✅ **Good:**
The `docker_scylla` fixture already waits for CQL and Alternator ports. If you start your own services, use `wait.wait_for`:
```python
from sdcm import wait

@pytest.mark.integration
def test_custom_service():
    container = start_my_service()
    wait.wait_for(
        func=lambda: container.is_port_used(port=8080, service_name="my-service"),
        step=1,
        text="Waiting for service to be ready",
        timeout=60,
        throw_exc=True,
    )
    # Now safe to interact
```

---

### P-6: Mixing Unit and Integration Test Logic

If a test can be written as a unit test with mocking, it should be. Only use integration tests when you genuinely need a real service.

❌ **Bad:**
```python
@pytest.mark.integration
def test_parse_cql_output(docker_scylla):
    # Starts a real Scylla container just to test parsing!
    output = docker_scylla.run("nodetool status")
    parsed = parse_nodetool_output(output.stdout)
    assert parsed["status"] == "UN"
```

✅ **Good:**
```python
# Unit test — no container needed
def test_parse_cql_output():
    sample_output = "UN  10.0.0.1  123.45 KB  256  100.0%  abc123  rack1"
    parsed = parse_nodetool_output(sample_output)
    assert parsed["status"] == "UN"
```

---

### P-7: Not Using xdist_group for Resource-Sharing Tests

Tests that share a Docker container or expensive resource should use `@pytest.mark.xdist_group` to ensure they run on the same worker.

❌ **Bad:**
```python
# These tests share state but may run on different workers
@pytest.mark.integration
def test_01_create_table(docker_scylla):
    docker_scylla.run("cqlsh -e 'CREATE TABLE ...'")

@pytest.mark.integration
def test_02_insert_data(docker_scylla):
    # May get a DIFFERENT docker_scylla instance in parallel!
    docker_scylla.run("cqlsh -e 'INSERT INTO ...'")
```

✅ **Good:**
```python
@pytest.mark.integration
@pytest.mark.xdist_group("scylla-crud")
def test_01_create_table(docker_scylla):
    ...

@pytest.mark.integration
@pytest.mark.xdist_group("scylla-crud")
def test_02_insert_data(docker_scylla):
    ...
```

---

## Anti-Patterns

### AP-1: Testing External Services Instead of SCT Code

Integration tests should test SCT's interaction with services, not the services themselves.

❌ **Bad:**
```python
@pytest.mark.integration
def test_scylla_starts():
    # Just tests that Scylla Docker image works — not SCT code
    container = docker.run("scylladb/scylla:latest")
    assert container.is_running()
```

✅ **Good:**
```python
@pytest.mark.integration
def test_cql_stress_thread(docker_scylla, params):
    # Tests SCT's stress thread against a real Scylla instance
    stress_thread = CqlStressThread(loader_set, cmd, node_list=[docker_scylla])
    stress_thread.run()
    assert stress_thread.get_results()
```

### AP-2: Skipping Without Explanation

Always provide a `reason` for skipped tests so developers know what's missing.

❌ **Bad:**
```python
@pytest.mark.skip
def test_azure_provisioning():
    ...
```

✅ **Good:**
```python
@pytest.mark.skipif(
    not os.environ.get("AZURE_SUBSCRIPTION_ID"),
    reason="Azure credentials not configured — set AZURE_SUBSCRIPTION_ID",
)
def test_azure_provisioning():
    ...
```

### AP-3: Ignoring Test Timeouts

Integration tests can hang if a service doesn't start. Always set timeouts.

❌ **Bad:**
```python
@pytest.mark.integration
def test_long_operation(docker_scylla):
    result = docker_scylla.run("nodetool repair")  # Could take forever
```

✅ **Good:**
```python
@pytest.mark.integration
def test_long_operation(docker_scylla):
    result = docker_scylla.run("nodetool repair", timeout=300)
```
