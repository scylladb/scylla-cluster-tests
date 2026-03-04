# Unit Test Pitfalls and Anti-Patterns

Common mistakes when writing SCT unit tests, with before/after fixes.

---

## Pitfalls

### P-1: Accidentally Contacting External Services

Unit tests must **never** make real network calls. The `fake_remoter` autouse fixture blocks SSH — but HTTP-based services (boto3, requests, REST APIs) are **NOT** auto-blocked. You must mock them explicitly using `unittest.mock.patch`, `monkeypatch`, or `moto`.

**Symptom:** Test passes locally but fails in CI, or test is slow/flaky.

❌ **Bad:**
```python
def test_fetch_ami():
    ami = boto3.client("ec2").describe_images(Owners=["self"])  # real AWS call
```

✅ **Good — mock or use moto:**
```python
from unittest.mock import patch, MagicMock

def test_fetch_ami():
    mock_client = MagicMock()
    mock_client.describe_images.return_value = {"Images": [{"ImageId": "ami-123"}]}
    with patch("boto3.client", return_value=mock_client):
        assert fetch_ami() == "ami-123"

@mock_aws  # or use moto for full AWS service mocking
def test_fetch_ami_moto():
    ec2 = boto3.client("ec2", region_name="us-east-1")
```

---

### P-2: Using unittest.TestCase Instead of pytest

SCT requires pytest-style tests. `unittest.TestCase` breaks fixture injection and autouse.

❌ **Bad:**
```python
class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = create_config()
    def test_value(self):
        self.assertEqual(self.config.get("key"), "value")
```

✅ **Good:**
```python
@pytest.fixture
def config():
    return create_config()

def test_value(config):
    assert config.get("key") == "value"
```

---

### P-3: Inline Imports in Test Code

SCT forbids inline imports. All imports must be at the top of the file.

❌ **Bad:**
```python
def test_something():
    from sdcm.utils.common import get_data_dir_path  # inline import
    path = get_data_dir_path("test_data")
```

✅ **Good:**
```python
from sdcm.utils.common import get_data_dir_path

def test_something():
    path = get_data_dir_path("test_data")
```

---

### P-4: Not Mocking FakeRemoter result_map

When testing code that runs remote commands, you must populate `FakeRemoter.result_map` with expected command patterns and responses.

**Symptom:** `ValueError: No fake result specified for command: ...`

❌ **Bad:**
```python
def test_node_status(fake_remoter):
    node = create_node()
    # Crashes because FakeRemoter has no result for "nodetool status"
    status = node.get_status()
```

✅ **Good:**
```python
import re

from invoke import Result

def test_node_status(fake_remoter):
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN  192.168.1.1", exited=0),
    }
    node = create_node()
    status = node.get_status()
    assert "UN" in status
```

---

### P-5: Missing Cleanup in Fixtures

Fixtures that create resources must clean up. Use `yield` with teardown or `tmp_path`.

❌ **Bad:**
```python
@pytest.fixture
def config_file():
    path = Path("/tmp/test_config.yaml")
    path.write_text("key: value")
    return path  # never cleaned up!
```

✅ **Good:**
```python
@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "test_config.yaml"
    path.write_text("key: value")
    return path  # tmp_path auto-cleaned by pytest
```

---

### P-6: Test Order Dependencies

Each test must be independent. SCT uses `pytest-random-order` and `pytest-xdist`.

❌ **Bad:**
```python
_shared_state = {}
def test_01_setup():
    _shared_state["node"] = create_node()
def test_02_verify():
    assert _shared_state["node"].is_up()  # fails if test_01 doesn't run first
```

✅ **Good:**
```python
@pytest.fixture
def node():
    n = create_node()
    yield n
    n.cleanup()

def test_node_is_up(node):
    assert node.is_up()
```

---

### P-7: Overly Broad Mocking

Mocking too much hides bugs. Mock at the boundary (network, file system, external service), not internal logic.

❌ **Bad:**
```python
def test_health_check():
    with patch("sdcm.cluster.BaseNode.get_status", return_value="UP"):
        with patch("sdcm.cluster.BaseNode.check_disk", return_value=True):
            with patch("sdcm.cluster.BaseNode.check_memory", return_value=True):
                # Testing nothing — everything is mocked
                assert health_check(node) is True
```

✅ **Good:**
```python
def test_health_check():
    # Only mock the network boundary
    with patch("sdcm.remote.RemoteCmdRunnerBase.run") as mock_run:
        mock_run.return_value = Result(stdout="UP", exited=0)
        assert health_check(node) is True
```

---

### P-8: Forgetting monkeypatch for Environment Variables

SCT configuration reads from environment variables. Always use `monkeypatch` to avoid polluting other tests.

❌ **Bad:**
```python
def test_config_backend():
    os.environ["SCT_CLUSTER_BACKEND"] = "aws"  # pollutes subsequent tests!
    assert SCTConfiguration().get("cluster_backend") == "aws"
```

✅ **Good:**
```python
def test_config_backend(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    assert SCTConfiguration().get("cluster_backend") == "aws"
```

---

### P-9: Fixture Scope Mismatch — monkeypatch in Session-Scoped Fixtures

`monkeypatch` is function-scoped — it **cannot** be used in session/module-scoped fixtures. Use `unittest.mock.patch` context managers instead.

❌ **Bad:**
```python
@pytest.fixture(scope="session", autouse=True)
def block_aws(monkeypatch):  # FAILS: scope mismatch
    monkeypatch.setattr("sdcm.utils.aws_utils.get_ami", lambda *a: "ami-fake")
```

✅ **Good:**
```python
@pytest.fixture(scope="session", autouse=True)
def block_aws():
    with patch("sdcm.utils.aws_utils.get_ami", return_value="ami-fake"):
        yield
```

---

### P-10: Patching Only the Source Module for `from X import func`

When code uses `from sdcm.utils.common import func`, patching only `sdcm.utils.common.func` leaves the import-site reference untouched.

❌ **Bad:**
```python
with patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"):
    config = SCTConfiguration()  # sdcm.sct_config still has the real reference
```

✅ **Good — patch both source and import site:**
```python
with (
    patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"),
    patch("sdcm.sct_config.convert_name_to_ami_if_needed", return_value="ami-fake"),
):
    config = SCTConfiguration()
```

---

### P-11: Using `patch("module.Class")` for Widely-Imported Classes

`KeyStore` is imported with `from sdcm.keystore import KeyStore` in 20+ modules. Patching `"sdcm.keystore.KeyStore"` only affects code that accesses it through that path.

❌ **Bad:**
```python
with patch("sdcm.keystore.KeyStore") as mock_ks:
    mock_ks.return_value.get_ssh_key_pair.return_value = fake_key  # other modules unaffected
```

✅ **Good — use `patch.object` to patch the class directly:**
```python
with patch.object(KeyStore, "get_ssh_key_pair", return_value=fake_key):
    ...  # works for ALL modules
```

---

### P-12: Returning MagicMock Instead of Proper Types from Mocks

`MagicMock()` auto-recurses on attribute access. Code that serializes the mock (JSON, str) gets circular references or `TypeError`.

❌ **Bad:**
```python
with patch.object(KeyStore, "get_ssh_key_pair", return_value=MagicMock()):
    provision_azure_vm()  # crashes: MagicMock is not JSON serializable
```

✅ **Good — return the real type:**
```python
from sdcm.keystore import SSHKey

mock_key = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA\n", private_key=b"dummy\n")
with patch.object(KeyStore, "get_ssh_key_pair", return_value=mock_key):
    provision_azure_vm()  # SSHKey namedtuple serializes correctly
```

---

### P-13: Module-Level Code Contacting External Services

Code at module level runs at **import time** during test collection, before fixtures are active.

❌ **Bad:**
```python
# Runs at import → triggers KeyStore() → boto3.resource("s3") → NoCredentialsError
argus_client = argus_client_factory()
```

✅ **Good — lazy initialization:**
```python
@lru_cache(maxsize=1)
def argus_client_factory():
    creds = KeyStore().get_argus_rest_credentials_per_provider()
    return partial(ArgusSCTClient, auth_token=creds["token"])
```

---

### P-14: Mock `__getattribute__` Returning `self` Breaks Attribute Chains

A catch-all `return self` in `__getattribute__` makes `obj.params.scylla_version.split(".")` return the mock at every step, eventually causing `TypeError`.

❌ **Bad:**
```python
class Monitors:
    def __getattribute__(self, item):
        if item not in "external_address":
            return self  # obj.params.scylla_version → all return self → TypeError
        return "10.0.0.1"
```

✅ **Good — handle known attributes explicitly:**
```python
class Monitors:
    def __getattribute__(self, item):
        if item == "params":
            return MagicMock(scylla_version=None)
        if item not in "external_address":
            return self
        return "10.0.0.1"
```

---

## Anti-Patterns

### AP-1: Testing Implementation, Not Behavior

Tests should verify what a function does, not how it does it.

❌ **Bad:**
```python
def test_config_loading():
    with patch("builtins.open") as mock_open:
        load_config("test.yaml")
        mock_open.assert_called_once_with("test.yaml", "r")
```

✅ **Good:**
```python
def test_config_loading(tmp_path):
    config_file = tmp_path / "test.yaml"
    config_file.write_text("cluster_backend: docker")
    config = load_config(str(config_file))
    assert config["cluster_backend"] == "docker"
```

### AP-2: Giant Test Functions

Split large tests into focused functions or use `@pytest.mark.parametrize`.

❌ **Bad:**
```python
def test_all_config_options():
    # 200 lines testing every config option
```

✅ **Good:**
```python
@pytest.mark.parametrize("option,value,expected", [
    ("cluster_backend", "aws", "aws"),
    ("cluster_backend", "docker", "docker"),
])
def test_config_option(option, value, expected, monkeypatch):
    monkeypatch.setenv(f"SCT_{option.upper()}", value)
    assert SCTConfiguration().get(option) == expected
```

### AP-3: Asserting on Mocked Return Values

If you mock a return value, don't assert on that same value — you're testing the mock, not the code.

❌ **Bad:**
```python
def test_get_nodes():
    with patch("sdcm.cluster.get_nodes", return_value=["node1"]):
        result = get_nodes()
        assert result == ["node1"]  # You're testing unittest.mock, not your code
```

✅ **Good:**
```python
def test_get_nodes():
    with patch("sdcm.cluster.get_nodes", return_value=["node1"]):
        result = process_nodes()  # Tests logic that USES get_nodes()
        assert result.count == 1
```
