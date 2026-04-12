# Test Quality Checklist (T1 + T7)

Data-backed from 338 T1 (test quality) + 214 T7 (missing tests) review comments across 11,312 SCT PRs.

## T1: pytest Conventions

SCT requires pytest-native style throughout `unit_tests/`. The framework runs tests with
`pytest-xdist` and `pytest-random-order` — `unittest.TestCase` breaks fixture injection,
`autouse` fixtures, and parallel execution. Every violation is blocked, not just commented on.

### Rule 1: No Classes — Use Modules and Functions

**Bad** (generates review comments):

```python
# unit_tests/test_config_parsing.py
import unittest

class TestConfigParsing(unittest.TestCase):
    def setUp(self):
        self.config = SCTConfiguration()

    def test_backend_defaults_to_aws(self):
        self.assertEqual(self.config.get("cluster_backend"), "aws")

    def test_missing_backend_raises(self):
        with self.assertRaises(ValueError):
            SCTConfiguration(cluster_backend=None)
```

**Good** (passes review):

```python
# unit_tests/test_config_parsing.py
import pytest

from sdcm.sct_config import SCTConfiguration


@pytest.fixture()
def default_config(monkeypatch):
    """SCTConfiguration with docker backend defaults."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    return SCTConfiguration()


def test_backend_defaults_to_docker(default_config):
    assert default_config.get("cluster_backend") == "docker"


def test_missing_backend_raises(monkeypatch):
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    with pytest.raises(ValueError):
        SCTConfiguration()
```

> Reviewer quote: "Dont use classes, if you need tests grouping uses module + separate files"

**Why**: pytest fixtures inject dependencies cleanly, run setup/teardown reliably even when tests
fail, and work with parallel execution. `setUp` does none of these.

### Rule 2: Fixtures Replace Repeated Setup

When 3+ tests share the same setup code, extract it into a fixture.

**Bad**:

```python
def test_node_is_healthy():
    node = MockNode(ip="10.0.0.1", dc="dc1")
    node.remoter = FakeRemoter()
    result = check_node_health(node)
    assert result is True

def test_node_detects_down():
    node = MockNode(ip="10.0.0.1", dc="dc1")
    node.remoter = FakeRemoter()
    node.remoter.result_map = {...}
    result = check_node_health(node)
    assert result is False
```

**Good**:

```python
@pytest.fixture()
def healthy_node():
    """A MockNode with FakeRemoter configured for healthy responses."""
    node = MockNode(ip="10.0.0.1", dc="dc1")
    node.remoter = FakeRemoter()
    return node


def test_node_is_healthy(healthy_node):
    assert check_node_health(healthy_node) is True


def test_node_detects_down(healthy_node):
    healthy_node.remoter.result_map = {...}
    assert check_node_health(healthy_node) is False
```

> Reviewer quote: "This should be a fixture, instead of having to call it in every test"

### Rule 3: `@pytest.mark.parametrize` for Variations

When tests differ only in input/output values, merge them with parametrize.

**Bad** (3 functions testing the same behavior):

```python
def test_parse_version_enterprise():
    assert parse_scylla_version("5.4.0-enterprise") == ("5.4.0", "enterprise")

def test_parse_version_oss():
    assert parse_scylla_version("5.4.0") == ("5.4.0", "oss")

def test_parse_version_rc():
    assert parse_scylla_version("5.4.0~rc1") == ("5.4.0", "rc")
```

**Good**:

```python
@pytest.mark.parametrize("version_str,expected", [
    pytest.param("5.4.0-enterprise", ("5.4.0", "enterprise"), id="enterprise"),
    pytest.param("5.4.0", ("5.4.0", "oss"), id="oss"),
    pytest.param("5.4.0~rc1", ("5.4.0", "rc"), id="rc-candidate"),
])
def test_parse_scylla_version(version_str, expected):
    assert parse_scylla_version(version_str) == expected
```

> Reviewer quote: "can be made into one test with parametrization probably"

**Why**: Parametrized tests are more readable, easier to extend, and produce clearer failure
messages that identify which case failed.

---

## T7: Missing Unit Tests

New logic in `sdcm/`, `utils/`, or config paths almost always warrants a unit test. Reviewers
specifically look for untested parsing, validation, and transformation functions.

### What Always Needs a Test

| Code Pattern | Test Required | Reason |
|--------------|--------------|--------|
| `def parse_*(...)` — any parsing function | YES | Format bugs are silent |
| `SctField(...)` additions in `sct_config.py` | YES | Validation rules need coverage |
| New utility function in `sdcm/utils/` | YES | Utility bugs are widespread |
| Data transformation (list→string, dict→yaml) | YES | Format mismatch silently corrupts |
| New nemesis operation logic | YES | Logic bugs missed in long runs |
| Simple property accessor | usually NO | Trivial to verify by inspection |

> Reviewer quote: "there isn't a unit test asserting the parsing behavior"
> Reviewer quote: "Do you think we could unit test this as well?"

### Example: Config Parameter Parsing

New config parameter added in `sct_config.py`:

```python
# sdcm/sct_config.py
seeds_selector: str = SctField(
    description="Comma-separated list of seed node selectors.",
    ...
)
```

**Missing test** — this is what triggers a review comment:

```python
# No test for how seeds_selector is parsed or validated
```

**Required test**:

```python
# unit_tests/test_sct_config.py
import pytest

from sdcm.sct_config import SCTConfiguration


@pytest.mark.parametrize("raw,expected_list", [
    pytest.param("10.0.0.1,10.0.0.2", ["10.0.0.1", "10.0.0.2"], id="two-seeds"),
    pytest.param("10.0.0.1", ["10.0.0.1"], id="single-seed"),
    pytest.param("", [], id="empty-string"),
])
def test_seeds_selector_parsed_as_list(monkeypatch, raw, expected_list):
    monkeypatch.setenv("SCT_SEEDS_SELECTOR", raw)
    config = SCTConfiguration()
    assert config.get("seeds_selector_list") == expected_list
```

### Example: New Utility Function

```python
# sdcm/utils/version_utils.py
def normalize_scylla_version(version: str) -> str:
    """Strip pre-release suffixes for comparison."""
    return version.split("~")[0].split("-")[0]
```

**Required test**:

```python
# unit_tests/test_version_utils.py
import pytest

from sdcm.utils.version_utils import normalize_scylla_version


@pytest.mark.parametrize("raw,expected", [
    pytest.param("5.4.0", "5.4.0", id="plain"),
    pytest.param("5.4.0~rc1", "5.4.0", id="rc-suffix"),
    pytest.param("5.4.0-enterprise", "5.4.0", id="enterprise-suffix"),
    pytest.param("5.4.0~rc1-enterprise", "5.4.0", id="both-suffixes"),
])
def test_normalize_scylla_version(raw, expected):
    assert normalize_scylla_version(raw) == expected
```

### Running the Tests

```bash
# Run all unit tests
uv run sct.py unit-tests

# Run specific test file
uv run python -m pytest unit_tests/test_version_utils.py -v

# Run with coverage for specific module
uv run python -m pytest unit_tests/test_version_utils.py \
    --cov=sdcm.utils.version_utils --cov-report=term-missing
```

## Review Checklist for T1 + T7

```
T1 — Test Quality
[ ] No unittest.TestCase classes in unit_tests/
[ ] No setUp/tearDown methods — use @pytest.fixture instead
[ ] Repeated setup extracted into fixtures (3+ tests sharing setup)
[ ] Variations of same test case use @pytest.mark.parametrize
[ ] pytest.param(id=...) used for human-readable test IDs
[ ] No inline imports in test files
[ ] Test names follow test_<function>_<scenario>_<expected> pattern

T7 — Missing Tests
[ ] New parsing functions have parametrized tests covering format variations
[ ] New sct_config.py parameters have tests for validation rules
[ ] New sdcm/utils/ functions have unit tests
[ ] Data transformation logic has tests for edge cases (empty, single, multi)
[ ] Tests live in unit_tests/ and follow test_*.py naming
```
