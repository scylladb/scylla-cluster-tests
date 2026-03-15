# Unit Test Anti-Patterns

Broader testing anti-patterns that reduce test value. See also: [common-pitfalls.md](common-pitfalls.md) for specific pitfalls (P-1 through P-15).

## AP-1: Testing Implementation, Not Behavior

❌ **Bad:**
```python
def test_config_loading():
    with patch("builtins.open") as mock_open:
        load_config("test.yaml")
        mock_open.assert_called_once_with("test.yaml", "r")
```

✅ **Good — test behavior, not how it's done:**
```python
def test_config_loading(tmp_path):
    config_file = tmp_path / "test.yaml"
    config_file.write_text("cluster_backend: docker")
    config = load_config(str(config_file))
    assert config["cluster_backend"] == "docker"
```

## AP-2: Giant Test Functions

Split large tests into focused functions or use `@pytest.mark.parametrize` with `pytest.param(id=...)` for human-readable names.

❌ **Bad:**
```python
def test_all_config_options():
    # 200 lines testing every config option
```

✅ **Good:**
```python
@pytest.mark.parametrize("option,value,expected", [
    pytest.param("cluster_backend", "aws", "aws", id="backend-aws"),
    pytest.param("cluster_backend", "docker", "docker", id="backend-docker"),
])
def test_config_option(option, value, expected, monkeypatch):
    monkeypatch.setenv(f"SCT_{option.upper()}", value)
    assert SCTConfiguration().get(option) == expected
```

## AP-3: Asserting on Mocked Return Values

❌ **Bad:**
```python
def test_get_nodes():
    with patch("sdcm.cluster.get_nodes", return_value=["node1"]):
        result = get_nodes()
        assert result == ["node1"]  # testing unittest.mock, not your code
```

✅ **Good — assert on code that USES the mocked value:**
```python
def test_get_nodes():
    with patch("sdcm.cluster.get_nodes", return_value=["node1"]):
        result = process_nodes()
        assert result.count == 1
```
