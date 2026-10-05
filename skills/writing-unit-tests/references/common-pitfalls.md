# Unit Test Pitfalls and Anti-Patterns

Common mistakes when writing SCT unit tests, with before/after fixes.

## Pitfalls

### P-1: Accidentally Contacting External Services

Unit tests must **never** make real network calls. The `fake_remoter` autouse fixture blocks SSH — but HTTP-based services (boto3, requests, REST APIs) are **NOT** auto-blocked. Mock them with `unittest.mock.patch`, `monkeypatch`, or `moto`.

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

This includes importing utility modules inside test functions to access module-level state (e.g., resetting caches). Import the module at the top of the file instead.

❌ **Bad:**
```python
def test_something():
    from sdcm.utils.common import get_data_dir_path  # inline import
    path = get_data_dir_path("test_data")
```

❌ **Also bad — importing a module inside a test to access module state:**
```python
def test_presets():
    import utils.staging_trigger.constants as mod  # inline import
    mod._PRESETS = None
    ...
    mod._PRESETS = None
```

✅ **Good — import at the top of the file:**
```python
import utils.staging_trigger.constants as constants_mod
from sdcm.utils.common import get_data_dir_path

def test_something():
    path = get_data_dir_path("test_data")

def test_presets():
    constants_mod._PRESETS = None
    ...
    constants_mod._PRESETS = None
```

---

### P-4: Not Mocking FakeRemoter result_map

When testing code that runs remote commands, you must populate `FakeRemoter.result_map`.

❌ **Bad:**
```python
def test_node_status(fake_remoter):
    node = create_node()
    status = node.get_status()  # ValueError: No fake result specified for command
```

✅ **Good:**
```python
def test_node_status(fake_remoter):
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN  192.168.1.1", exited=0),
    }
    node = create_node()
    assert "UN" in node.get_status()
```

---

### P-5: Missing Cleanup in Fixtures

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

Mock at the boundary (network, file system, external service), not internal logic.

❌ **Bad:**
```python
def test_health_check():
    with patch("sdcm.cluster.BaseNode.get_status", return_value="UP"):
        with patch("sdcm.cluster.BaseNode.check_disk", return_value=True):
            assert health_check(node) is True  # testing nothing — all mocked
```

✅ **Good:**
```python
def test_health_check():
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

`monkeypatch` is function-scoped — use `unittest.mock.patch` context managers in session/module fixtures.

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

When code uses `from sdcm.utils.common import func`, patching only `sdcm.utils.common.func` leaves the import-site copy untouched.

❌ **Bad:**
```python
with patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"):
    config = SCTConfiguration()  # sdcm.sct_config.config still has the real reference
```

✅ **Good — patch both source and import site:**
```python
with (
    patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"),
    patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", return_value="ami-fake"),
):
    config = SCTConfiguration()
```

Note the import site is `sdcm.sct_config.config`, not `sdcm.sct_config`: the config lives in the
`sdcm/sct_config/` package and `__init__.py` deliberately does not re-export these names, so
patching `sdcm.sct_config.<name>` raises `AttributeError` rather than silently doing nothing.

This holds even for names *defined* inside the package. `config.py` does
`from sdcm.sct_config.types import _check_file_exists`, so the effective patch target is
`sdcm.sct_config.config._check_file_exists` — patching `...types._check_file_exists` replaces an
attribute no caller reads. Always patch where the name is **called**, not where it is defined.

---

### P-11: Using `patch("module.Class")` for Widely-Imported Classes

`KeyStore` is imported via `from sdcm.keystore import KeyStore` in 20+ modules. Patching `"sdcm.keystore.KeyStore"` only affects code accessing it through that path.

❌ **Bad:**
```python
with patch("sdcm.keystore.KeyStore") as mock_ks:
    mock_ks.return_value.get_ssh_key_pair.return_value = fake_key
```

✅ **Good — `patch.object` patches the class directly:**
```python
with patch.object(KeyStore, "get_ssh_key_pair", return_value=fake_key):
    ...  # works for ALL modules
```

---

### P-12: Returning MagicMock Instead of Proper Types from Mocks

`MagicMock()` auto-recurses on attribute access — serialization gets circular references or `TypeError`.

❌ **Bad:**
```python
with patch.object(KeyStore, "get_ssh_key_pair", return_value=MagicMock()):
    provision_azure_vm()  # MagicMock is not JSON serializable
```

✅ **Good — return the real type:**
```python
mock_key = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA\n", private_key=b"dummy\n")
with patch.object(KeyStore, "get_ssh_key_pair", return_value=mock_key):
    provision_azure_vm()  # SSHKey namedtuple serializes correctly
```

---

### P-13: Module-Level Code Contacting External Services

Code at module level runs at **import time** during test collection, before fixtures are active.

❌ **Bad:**
```python
argus_client = argus_client_factory()  # import time → KeyStore() → NoCredentialsError
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

### P-15: Utility Functions as Factories Instead of Fixtures

Prefer pytest fixtures (including factory fixtures) over bare helper functions. Fixtures integrate with pytest's lifecycle, cleanup, and dependency injection.

**This is especially important when the factory needs a pytest fixture as a dependency** (e.g. `events_function_scope`, `monkeypatch`, `tmp_path`). A plain helper function cannot receive fixtures via dependency injection — you end up passing them manually, or worse, working around scope issues with hacks like `events.clear()`. Converting the helper to a factory fixture lets pytest wire up dependencies automatically.

❌ **Bad — helper function that needs a fixture but can't receive one:**
```python
def _make_runner(disruptions):
    # Cannot depend on events_function_scope — it's not a fixture!
    termination_event = threading.Event()
    tester = FakeTester(params=PARAMS)
    tester.db_cluster.check_cluster_health = MagicMock()
    runner = TestNemesisRunner(tester, termination_event)
    runner.disruptions_list = disruptions
    return runner

def test_run_stops_after_skips(events_function_scope):
    runner = _make_runner(disruptions=[])  # events not wired to runner
    runner.disruptions_list = [SkippingTestNemesis(runner=runner)]
    runner.run(cycles_count=5)
```

✅ **Good — factory fixture with proper dependency injection:**
```python
# conftest.py
@pytest.fixture
def make_nemesis_runner(events_function_scope):
    def _make(disruptions=None):
        termination_event = threading.Event()
        tester = FakeTester(params=PARAMS)
        tester.db_cluster.check_cluster_health = MagicMock()
        runner = TestNemesisRunner(tester, termination_event)
        if disruptions is not None:
            runner.disruptions_list = disruptions
        return runner
    return _make

# test file
def test_run_stops_after_skips(make_nemesis_runner, events_function_scope):
    runner = make_nemesis_runner([SkippingTestNemesis(runner=runner)])
    runner.run(cycles_count=5)
```

**Same rule when tests run against several types.** If a helper branches on the type to build the object, use one parametrized fixture. Each test then just names the fixture, and the setup is in one place.

❌ **Bad — PR [#16142](https://github.com/scylladb/scylla-cluster-tests/pull/16142): a type-switching helper called from every test:**
```python
def _make_logger(logger_class, tmp_path):
    if issubclass(logger_class, HDRHistogramFileLogger):
        return logger_class(node=MagicMock(), remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr"))
    return logger_class(node=MagicMock(), target_log_file=str(tmp_path / "target.log"))


@pytest.mark.parametrize("logger_class", [_StubSSHLogger, _StubHDRLogger])
def test_stop_releases_the_pool_worker(logger_class, tmp_path):
    logger = _make_logger(logger_class, tmp_path)
    ...
```

✅ **Good — one parametrized fixture builds the right object per type:**
```python
@pytest.fixture(params=["ssh", "hdr"])
def logger(request, tmp_path):
    """One real logger of each kind that owns a log-follower pool."""
    if request.param == "hdr":
        return HDRHistogramFileLogger(node=_node(), remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr"))
    return SSHGeneralSystemdLogger(node=_node(), target_log_file=str(tmp_path / "target.log"))


def test_stop_releases_the_pool_worker(logger):
    ...
```

See also: [anti-patterns.md](anti-patterns.md) for broader testing anti-patterns.

---

### P-16: Singleton State Leaking Between Parallel Tests

SCT contains classes that use `metaclass=Singleton` (e.g. `NodeLoadInfoServices`, `AdaptiveTimeoutStore`). A `Singleton` holds shared mutable state across the entire process. When `pytest-xdist` runs tests on the same worker, a test that populates a Singleton's cache can corrupt a later test that expects a clean slate — even if the tests appear unrelated.

**Symptoms:** tests pass with `-n0` (sequential) but fail with `-n2` or higher; failures are non-deterministic; errors reference stale node names, wrong cached values, or `KeyError` from a missing key that another test was supposed to populate.

❌ **Bad — Singleton cache persists across tests:**
```python
# Test A populates the cache with a stale remoter
def test_a(fake_node):
    with adaptive_timeout(operation=Operations.DECOMMISSION, node=fake_node, ...) as timeout:
        ...
# Test B runs on the same worker; NodeLoadInfoServices still holds fake_node from test_a
# fake_node.remoter is now invalid → KeyError / wrong cached result
def test_b(fake_node, ...):
    with adaptive_timeout(operation=Operations.DECOMMISSION, node=fake_node, ...) as timeout:
        assert timeout == 7200  # fails: gets stale value from test_a's cache
```

✅ **Good — add an `autouse` fixture that clears the Singleton's mutable state after each test:**
```python
from sdcm.utils.adaptive_timeouts.load_info_store import NodeLoadInfoServices

@pytest.fixture(autouse=True)
def clear_node_load_info_services_singleton():
    """Clear the NodeLoadInfoServices Singleton cache after each test to prevent cross-test pollution."""
    yield
    NodeLoadInfoServices()._services.clear()
```

**Rules:**
- Only clear in teardown (post-`yield`); the previous test's teardown already ran before setup begins.
- Place the fixture in the test module or in `conftest.py` if multiple modules share the same Singleton.
- Prefer post-yield-only cleanup (no pre-yield clear) — redundant pre-yield clearing is a code smell that signals the teardown isn't trusted.

Also beware that `MemoryAdaptiveTimeoutStore` (and any `AdaptiveTimeoutStore` subclass) inherits `Singleton` — calling `MemoryAdaptiveTimeoutStore()` inside a test body returns the **same shared instance** that the fixture populated, but it may have been cleared or written to by a parallel test. Always read results from the fixture instance, not from a fresh `SomeStore()` call:

❌ **Bad — creates new Singleton reference, may see another test's data (or none):**
```python
metrics = MemoryAdaptiveTimeoutStore().get(operation="DECOMMISSION")
```

✅ **Good — read from the fixture instance that was passed to `adaptive_timeout`:**
```python
def test_decommission(fake_node, adaptive_timeout_store):
    with adaptive_timeout(..., stats_storage=adaptive_timeout_store) as timeout:
        ...
    metrics = adaptive_timeout_store.get(operation="DECOMMISSION")  # same instance
```

---

### P-17: The Test Name Claims Something the Asserts Don't Check

Reviewers read the name and assume it was verified. If the name says "without reaching EC2", "does not retry", or "never logs", there must be an assertion that fails when that happens. An assertion that the call simply succeeded does not cover it.

❌ **Bad — PR [#16093](https://github.com/scylladb/scylla-cluster-tests/pull/16093): nothing checks that EC2 was not reached:**
```python
def test_capacity_reservation_pipeline_lints_without_reaching_ec2(_restore_root_logger):
    env = build_env(parse_jenkinsfile(PIPELINE)) | {"SCT_TEST_ID": "11111111-2222-3333-4444-555555555555"}

    is_error, message = validate_pipeline(PIPELINE, env)

    assert not is_error, message
```

✅ **Good — record the side effect the name rules out, and assert on it:**
```python
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(pipeline, remote_connections, restore_root_logger):
    ...
    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
```

To assert "no network access", use a fixture that refuses outbound connections and records each attempt. PR #16093 adds one as `remote_connections` in `unit_tests/lint/conftest.py`: it monkeypatches `socket.socket.connect`/`connect_ex`, lets loopback through, and returns the list of refused addresses. Refusing, not just recording, keeps a missing stub from making a real call.

---

### P-18: Green for the Wrong Reason — Mutation-Check Every New Test

A passing test proves only that the assertions held. It does not prove that the code under test ran. In PR #16093 the first replacement tests passed **with every stub removed**: `build_env` always injects a placeholder image for the main cluster, so the image lookup the tests were meant to cover never ran. The fix was a test-only config that adds an oracle cluster (`db_type: mixed_scylla` + `oracle_scylla_version`), whose resolver `build_env` does not short-circuit.

✅ **Mutation check — required before calling a test done:**
1. Break the code under test in the way the test claims to catch: delete the stub entry, invert the condition, return early.
2. Run the test and confirm it **fails**, with a message that points at the break.
3. Revert the break and confirm the test passes again.

```bash
# e.g. comment out one entry in _CLOUD_API_PATCHES, then:
uv run python -m pytest unit_tests/lint/test_validator.py -n0 -q   # must FAIL
git checkout sdcm/utils/lint/validator.py                           # restore
```

If the test still passes after the break, it is testing setup, a default, or a mock (see AP-3), not the code. Say in the PR description which mutation you checked.

**Don't stub out the failure path.** A stub replaces a code path, and every bug in that path disappears with it. In PR [#16142](https://github.com/scylladb/scylla-cluster-tests/pull/16142) the test replaced `_journal_thread` entirely. So it never ran the case the reviewer found: `remote_pid` is empty, `stop()` sends `kill -9 -`, and the worker stays blocked in `journalctl -f`. For each stub, list what the real code does that the stub skips, and cover any of those that the fix depends on with its own test. That PR added `test_stop_without_remote_pid_warns_instead_of_running_bare_kill`, and its reply to the review names the mutation it checked: "with the `_shutdown_pool()` calls removed, both parametrizations fail".

---

### P-19: Fixture and Helper Class Placement

Fixtures and helper classes go at the top of the module, after imports and constants, or in `conftest.py` when more than one module uses them. The same review comment came up on PR #16093 (a fixture) and on PR #16142 (a stub class defined between tests). A fixture defined between tests is easy to miss and gets copied into the next file. Do not give fixture names a leading underscore: pytest injects them by name, so the underscore marks nothing as private and only makes the signature noisier.

❌ **Bad — PR #16093: underscore fixture defined halfway down the file:**
```python
def test_fake_oci_image_is_indexable_as_the_resolvers_expect():
    ...


@pytest.fixture
def _restore_root_logger():
    root = logging.getLogger()
    ...


def test_capacity_reservation_pipeline_lints_without_reaching_ec2(_restore_root_logger):
    ...
```

✅ **Good — public name, shared via `conftest.py`:**
```python
# unit_tests/lint/conftest.py
@pytest.fixture
def restore_root_logger():
    """`validate_pipeline` silences the root logger for good; keep that out of the other tests."""
    root = logging.getLogger()
    handlers, disabled = root.handlers, root.disabled
    yield
    root.handlers = handlers
    root.disabled = disabled
```

---

### P-20: Test-Only Safeguards in Production Code

Code that exists only to catch a missing stub, mock, or fixture during tests belongs in a test fixture, not in `sdcm/`. In production it is dead weight on every call, and it hides what the tests really depend on.

❌ **Bad — PR #16093 added a network guard to the production linter path:**
```python
# sdcm/utils/lint/validator.py
class LintNetworkAccessError(RuntimeError):
    """Linting reached the network instead of a stub in `_CLOUD_API_PATCHES`."""


@contextlib.contextmanager
def _no_remote_network():
    """Turn an unstubbed cloud call into a loud, self-explanatory failure."""
    ...

# inside validate_pipeline():
        stack.enter_context(_no_remote_network())
```

✅ **Good — production keeps only what makes it work (the stubs); detection moves to a fixture:**
```python
# unit_tests/lint/conftest.py
@pytest.fixture
def remote_connections(monkeypatch):
    """Refuse every outbound connection and record where it was headed."""
    attempts = []
    ...
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    return attempts
```

A future unstubbed lookup then fails in the unit tests, not in someone else's PR or CI run. **Not covered:** guards against real-world misuse, such as input validation or refusing to run without credentials. Those protect production and stay in `sdcm/`.
