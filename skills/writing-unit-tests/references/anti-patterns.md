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

## AP-6: Testing a Copy of the Code Instead of the Code Itself

The most dangerous anti-pattern: the test creates a `FakeFoo` class that **re-implements** the same logic as the real `Foo`, then tests `FakeFoo`. The production code is never exercised, so any bug in `Foo` is invisible.

This often happens when an AI generates tests by reading the source and mirroring it into a fake, rather than calling the real class with mocked dependencies.

❌ **Bad — `FakeDockerCluster._create_nodes` is a verbatim copy of the real method:**
```python
class FakeDockerCluster:
    # copied from sdcm/cluster_docker.py
    def _create_nodes(self, count, rack=None, enable_auto_bootstrap=False):
        new_nodes = []
        for node_index in self._get_new_node_indexes(count):
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(node_index, rack=node_rack)
            ...
        return new_nodes

def test_round_robin_rack_assignment():
    cluster = FakeDockerCluster(racks_count=3)
    nodes = cluster._create_nodes(6)
    assert [n.rack for n in nodes] == [0, 1, 2, 0, 1, 2]  # tests the copy, not the real code
```

✅ **Good — construct the real object and mock only its external dependencies:**
```python
from unittest.mock import MagicMock, patch
from sdcm.cluster_docker import DockerCluster

@pytest.mark.parametrize("racks_count,node_count,expected_racks", [
    pytest.param(3, 6, [0, 1, 2, 0, 1, 2], id="round-robin-3-racks"),
    pytest.param(2, 5, [0, 1, 0, 1, 0],    id="round-robin-2-racks"),
    pytest.param(1, 3, [0, 0, 0],           id="single-rack"),
])
def test_create_nodes_round_robin(racks_count, node_count, expected_racks, params):
    params["simulated_racks"] = racks_count
    params["n_db_nodes"] = node_count
    cluster = DockerCluster(...)  # the REAL class

    with patch.object(cluster, "_create_node", side_effect=[MagicMock() for _ in range(node_count)]):
        cluster._create_nodes(count=node_count)

    actual_racks = [call.kwargs["rack"] for call in cluster._create_node.call_args_list]
    assert actual_racks == expected_racks
```

**How to spot it:** if you grep the test file and find the same method bodies from `sdcm/` appearing verbatim, the tests are copies. A good test constructs the real `sdcm.*` class and mocks only at the external boundary (network, file system, cloud APIs).

**Correct approach:** always instantiate the real class and mock only its external I/O. Only fall back to `MagicMock(spec=RealClass)` as a last resort when the real constructor has unavoidable heavy side effects that cannot be mocked — and document why.
