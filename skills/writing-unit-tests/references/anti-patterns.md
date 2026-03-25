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

## AP-4: Test Classes Instead of Pure Functions

Using `class Test*` to group tests adds no value in pytest and breaks fixture injection, `autouse` fixtures, and `pytest-xdist` parallel execution.

❌ **Bad:**
```python
class TestEvaluateSkip:
    def test_default_returns_empty_list(self):
        nemesis = CustomNemesisA(runner=...)
        assert nemesis.evaluate_skip() == []

    def test_skippable_excluded(self):
        ...
```

✅ **Good — flat module-level functions:**
```python
def test_evaluate_skip_default_returns_empty_list():
    nemesis = CustomNemesisA(runner=...)
    assert nemesis.evaluate_skip() == []

def test_evaluate_skip_skippable_excluded():
    ...
```

Group related tests with a comment block (e.g. `# --- filtering tests ---`) instead of a class.

## AP-5: Duplicating Test Infrastructure Instead of Reusing It

Writing new fake objects, base classes, runner stubs, or fixture setup code when equivalent infrastructure already exists in `unit_tests/` causes maintenance burden and inconsistency.

### Ignoring unit_tests/lib/

❌ **Bad — writing a new in-memory event collector from scratch:**
```python
captured_events = []

def fake_publish(event):
    captured_events.append(str(event))

with patch("sdcm.sct_events.base.SctEvent.publish", fake_publish):
    run_something()

assert any("CRITICAL" in e for e in captured_events)
```

✅ **Good — use the existing `FakeEventsDevice` from `unit_tests/lib/`:**
```python
def test_publishes_critical(events_function_scope):
    run_something()
    assert events_function_scope.get_events_by_category()["CRITICAL"]
```

Before writing new fake objects or utility classes, check `unit_tests/lib/` first. It contains ready-to-use helpers (`FakeEventsDevice`, `FakeRemoter`, `make_fake_events`, etc.) designed for reuse. Prefer reusing or slightly extending what is already there over writing a parallel implementation.

### Fixture setup inlined into test bodies

❌ **Bad — reproducing the `nemesis_runner` fixture body inside a test:**
```python
def test_run_does_not_crash_on_skipped_nemesis():
    termination_event = threading.Event()
    tester = FakeTester(params=PARAMS)
    tester.db_cluster.check_cluster_health = MagicMock()
    tester.db_cluster.test_config = MagicMock()
    runner = TestNemesisRunner(tester, termination_event, nemesis_selector="flag_a")
    ...
```

✅ **Good — accept the existing fixture and build on it:**
```python
def test_run_does_not_crash_on_skipped_nemesis(nemesis_runner):
    nemesis_runner.disruptions_list = [SkippingTestNemesis(runner=nemesis_runner)]
    ...
```

### Infrastructure exported from test files

`FakeSisyphusMonkey` lives in `test_sisyphus.py` but is imported by `test_evaluate_skip.py`. Test files are not libraries. If a fake/stub/helper is needed in more than one test file, move it to `fake_cluster.py`, `unit_tests/nemesis/__init__.py`, or a `conftest.py`.
