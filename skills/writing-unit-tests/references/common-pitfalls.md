# Unit Test Pitfalls and Anti-Patterns

Common mistakes when writing SCT unit tests, with before/after fixes.

---

## Pitfalls

### P-1: Accidentally Contacting External Services

Unit tests must **never** make real network calls. The autouse `fake_remoter` fixture in `conftest.py` blocks SSH connections, but HTTP calls (e.g., `requests.get`, `boto3`) are not automatically blocked.

**Symptom:** Test passes locally but fails in CI, or test is slow/flaky.

❌ **Bad:**
```python
def test_fetch_ami():
    # Makes a real AWS API call
    ami = boto3.client("ec2").describe_images(Owners=["self"])
    assert ami
```

✅ **Good:**
```python
from unittest.mock import patch, MagicMock

def test_fetch_ami():
    mock_client = MagicMock()
    mock_client.describe_images.return_value = {"Images": [{"ImageId": "ami-123"}]}
    with patch("boto3.client", return_value=mock_client):
        ami = fetch_ami()
        assert ami == "ami-123"
```

**Or use moto for full AWS mocking:**
```python
from moto import mock_aws

@mock_aws
def test_fetch_ami():
    ec2 = boto3.client("ec2", region_name="us-east-1")
    # moto intercepts all AWS calls in this scope
```

---

### P-2: Using unittest.TestCase Instead of pytest

SCT convention requires pytest-style tests. `unittest.TestCase` breaks fixture injection, parametrize, and autouse fixtures.

❌ **Bad:**
```python
import unittest

class TestConfig(unittest.TestCase):
    def setUp(self):
        self.config = create_config()

    def test_value(self):
        self.assertEqual(self.config.get("key"), "value")
```

✅ **Good:**
```python
import pytest

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

Fixtures that create resources (files, processes, global state) must clean up. Use `yield` with teardown or `tmp_path`.

❌ **Bad:**
```python
@pytest.fixture
def config_file():
    path = Path("/tmp/test_config.yaml")
    path.write_text("key: value")
    return path
    # File is never cleaned up!
```

✅ **Good:**
```python
@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "test_config.yaml"
    path.write_text("key: value")
    return path
    # tmp_path is automatically cleaned up by pytest
```

---

### P-6: Test Order Dependencies

Each test must be independent. Do not rely on test execution order — SCT uses `pytest-random-order` and `pytest-xdist` for parallel execution.

❌ **Bad:**
```python
_shared_state = {}

def test_01_setup():
    _shared_state["node"] = create_node()

def test_02_verify():
    # Fails if test_01_setup doesn't run first
    assert _shared_state["node"].is_up()
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

SCT configuration reads heavily from environment variables. Always use `monkeypatch` to avoid polluting other tests.

❌ **Bad:**
```python
def test_config_backend():
    os.environ["SCT_CLUSTER_BACKEND"] = "aws"
    config = SCTConfiguration()
    assert config.get("cluster_backend") == "aws"
    # Environment is polluted for subsequent tests!
```

✅ **Good:**
```python
def test_config_backend(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    config = SCTConfiguration()
    assert config.get("cluster_backend") == "aws"
    # monkeypatch automatically restores the environment
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

Split large tests into focused test functions or use `@pytest.mark.parametrize`.

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
    ("n_db_nodes", "3", 3),
])
def test_config_option(option, value, expected, monkeypatch):
    monkeypatch.setenv(f"SCT_{option.upper()}", value)
    config = SCTConfiguration()
    assert config.get(option) == expected
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
