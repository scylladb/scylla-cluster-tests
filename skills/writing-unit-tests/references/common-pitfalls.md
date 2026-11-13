# Mocking, Isolation, and Structure Rules

How to mock, isolate, and lay out SCT unit tests. Each rule shows only the shape to follow. See also: [anti-patterns.md](anti-patterns.md) for test design rules (AP-1 through AP-18).

## Rules

### P-1: Mock Every External Service

The `fake_remoter` autouse fixture blocks SSH. HTTP-based services (boto3, requests, REST APIs) are **not** blocked automatically, so mock them with `unittest.mock.patch`, `monkeypatch`, or `moto`.

```python
def test_fetch_ami_returns_first_image():
    """fetch_ami() picks the image id from the EC2 response."""
    ec2 = MagicMock()
    ec2.describe_images.return_value = {"Images": [{"ImageId": "ami-123"}]}

    with patch("boto3.client", return_value=ec2):
        assert fetch_ami() == "ami-123"
```

---

### P-2: pytest Functions and Fixtures

Setup goes in fixtures, checks are plain `assert`, and tests are module-level functions.

```python
@pytest.fixture
def config():
    """A config built from the default test values."""
    return create_config()


def test_config_reads_key(config):
    """The default test config carries the expected key."""
    assert config.get("key") == "value"
```

---

### P-3: Imports at the Top of the File

All imports go at the top of the test module, including modules whose state a test changes. Change module state with `monkeypatch.setattr(module, "name", value)` so pytest restores it.

```python
from sdcm.utils.common import get_data_dir_path


def test_data_dir_path_points_inside_repo():
    """get_data_dir_path resolves under the repository data directory."""
    assert get_data_dir_path("test_data").endswith("data_dir/test_data")
```

- **In-repo modules always import.** A path that exists is always tested; whoever deletes it deletes its tests too. No `pytest.importorskip` for SCT code (#15427).
- **Scripts outside a package** (`.github/scripts/`) run through `subprocess` in a fixture, and the test checks their external interface: output, exit code, API calls. Don't load them with `importlib.util.spec_from_file_location` or `sys.modules` (#16363, #16933). If the test needs their functions, move them into a package.
- **Nothing runs at import time**: no network call or file read to compute a module constant. Put it in a fixture, session-scoped if it is expensive (#13992, #14513).

---

### P-4: Give FakeRemoter a Result for Every Command

Code that runs remote commands needs a `FakeRemoter.result_map` entry for each command. A missing entry raises `ValueError: No fake result specified for command`.

```python
def test_node_status(fake_remoter):
    """get_status() returns the state reported by nodetool."""
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN  192.168.1.1", exited=0),
    }
    node = create_node()

    assert "UN" in node.get_status()
```

---

### P-5: Files in `tmp_path`

Write files under `tmp_path`. pytest creates a fresh directory per test and cleans it up.

```python
@pytest.fixture
def config_file(tmp_path):
    """A minimal YAML config on disk."""
    path = tmp_path / "test_config.yaml"
    path.write_text("key: value")
    return path
```

---

### P-6: Every Test Runs Alone

Each test gets its state from fixtures, so it passes alone, in any order, and in parallel.

```python
@pytest.fixture
def node():
    """A fresh node, cleaned up after the test."""
    new_node = create_node()
    yield new_node
    new_node.cleanup()


def test_node_is_up(node):
    """A newly created node reports itself up."""
    assert node.is_up()
```

---

### P-7: Mock at the Boundary

Mock the outermost external call (the remote command, the HTTP request, the SDK client) and let SCT logic run.

```python
def test_health_check_reads_remote_status(node):
    """health_check() is healthy when the node reports UP."""
    with patch("sdcm.remote.RemoteCmdRunnerBase.run", return_value=Result(stdout="UP", exited=0)):
        assert health_check(node) is True
```

---

### P-8: Environment Variables With `monkeypatch.setenv`

SCT configuration reads environment variables. `monkeypatch` restores them after the test.

```python
def test_config_backend(monkeypatch):
    """SCT_CLUSTER_BACKEND selects the backend."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")

    assert SCTConfiguration().get("cluster_backend") == "aws"
```

Tests also run in shells and CI jobs that export variables of their own. A developer shell may export minicloud endpoints. The Jenkins unit-test job sets `JOB_NAME` and `BUILD_NUMBER`. When such a variable switches a code path, remove it in an `autouse` fixture and say why in its docstring (#16020, #16044). Adapted from `test_pending_instances_timeout.py`:

```python
MINICLOUD_ENV_VARS = ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL")


@pytest.fixture(autouse=True)
def no_minicloud_env(monkeypatch):
    """Drop minicloud endpoints a developer shell may export; they route provisioning to the emulator."""
    for var in MINICLOUD_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
```

- **Code under test that writes `os.environ`**: call `monkeypatch.delenv(var, raising=False)` before the act, so monkeypatch restores the variable afterwards (#15427).
- **Subprocesses** get `stdin=subprocess.DEVNULL`, so the result is the same under `pytest -s` in a terminal and in CI (#16576).
- **Paths** are built from `tmp_path`, `test_data_dir`, or `Path(__file__)`, never relative to the working directory (#14170).

---

### P-9: `patch` Context Managers in Session and Module Fixtures

`monkeypatch` is function-scoped. Wider-scoped fixtures use `unittest.mock.patch` and `yield`.

```python
# unit_tests/unit/conftest.py, trimmed
@pytest.fixture(scope="session", autouse=True)
def mock_cloud_services():
    """Prevent unit tests from making real AWS/GCE/Azure API calls."""
    with (
        patch("sdcm.utils.aws_utils.get_arch_from_instance_type", return_value="x86_64"),
        patch.object(KeyStore, "sync", return_value=None),
    ):
        yield
```

---

### P-10: Patch Where the Name Is Called

`from X import func` copies the reference into the importing module. Patch the import site, and the source too if other callers read it there.

```python
with (
    patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"),
    patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", return_value="ami-fake"),
):
    config = SCTConfiguration()
```

The config import site is `sdcm.sct_config.config`, not `sdcm.sct_config`. The package `__init__.py` does not re-export these names, so a patch on `sdcm.sct_config.<name>` raises `AttributeError`. This also holds for names defined inside the package. `config.py` does `from sdcm.sct_config.types import _check_file_exists`, so the target is `sdcm.sct_config.config._check_file_exists`.

---

### P-11: `patch.object` for Widely Imported Classes

`KeyStore` is imported by name in 20+ modules. `patch.object` changes the class itself, so every importer sees the patch.

```python
with patch.object(KeyStore, "get_ssh_key_pair", return_value=fake_key):
    ...
```

---

### P-12: Return Real Types From Mocks

A `MagicMock` return value creates attributes on access and breaks serialization. Return the real type.

```python
ssh_key = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA\n", private_key=b"dummy\n")
with patch.object(KeyStore, "get_ssh_key_pair", return_value=ssh_key):
    provision_azure_vm()
```

A fake behaves like the boundary it replaces, or the test passes where production fails:

- **Same shape as production today.** When the PR changes what a function returns, update every fake of it (#14423).
- **No more lenient than the real thing.** A fake `send_files` that runs `cp` in a shell expands `$HOME`, while rsync leaves it literal (#15697).
- **Built from the same source.** When production builds a name and another place matches it (a process name and its `pgrep` pattern), build the input with the real builder, not a hand-written string (#16854).
- **A stub that must not be called** is `MagicMock(side_effect=AssertionError("must not resolve"))` (#16233).

---

### P-13: Lazy Initialization for Clients in Production Code

Module-level code runs at import time, during test collection, before any fixture is active. Create clients and fetch credentials on first use.

```python
# sdcm/utils/docker_utils.py
@cache
def get_docker_hub_credentials() -> dict:
    LOGGER.debug("Get Docker Hub credentials")
    return KeyStore().get_docker_hub_credentials()
```

---

### P-14: Set Fake Attributes Explicitly

Give a fake exactly the attributes the code reads: a `MagicMock` with named attributes set, or a real object (AP-15). Every attribute then has a value the test chose.

```python
@pytest.fixture
def monitors():
    """Monitor set exposing the address and Scylla version the code reads."""
    monitor_set = MagicMock()
    monitor_set.external_address = "10.0.0.1"
    monitor_set.params.scylla_version = None
    return monitor_set
```

When the fake stands in for a known class, create it with `MagicMock(spec=RealClass)`, so reading an attribute the class lacks raises `AttributeError` (#16444, #16590). `spec` knows only class-level attributes, so set instance attributes, such as the ones `__init__` assigns, explicitly as above.

---

### P-15: Setup Lives in Fixtures

Code that builds the object under test, its mocks, or its patches is a fixture. This holds even when it needs no other fixture: pytest wires dependencies, runs teardown, and lets tests compose setup by name (#13333, #13924, #15486, #15591, #16589).

- **Variations** use a fixture factory, so callers pass only what changes. From `test_manager.py`:
  ```python
  @pytest.fixture()
  def set_params(runner):
      """Factory pointing ``runner.cluster.params.get`` at the given values, honouring the caller's default."""

      def apply(**values):
          runner.cluster.params.get.side_effect = lambda key, default=None, **kwargs: values.get(key, default)

      return apply
  ```
- **Shared values** are small fixtures that other fixtures and tests request (abridged from `test_manager.py`):
  ```python
  @pytest.fixture()
  def snapshot_tag():
      """Tag of the single snapshot in ``snapshot_catalog``."""
      return "sm_20250101120000UTC"


  @pytest.fixture()
  def snapshot_catalog(snapshot_tag, restored_ks):
      """A minimal ``get_persistent_snapshots()`` catalog: a single 5 GB AWS snapshot."""
      return {"aws": {"snapshots_sizes": {5: {"snapshots": {"us-east-1": {snapshot_tag: {"keyspace_name": restored_ks}}}}}}}
  ```
- **Patches that every test in a module needs** are an `autouse` fixture:
  ```python
  @pytest.fixture(autouse=True)
  def stub_common_helpers():
      """Stub DbNodeLogger, which opens SSH sessions to every node, and make the SStable pick deterministic."""
      with patch(f"{COMMON_MODULE}.DbNodeLogger"), patch(f"{COMMON_MODULE}.random") as mock_random:
          mock_random.choice.side_effect = lambda seq: seq[0]
          yield
  ```
- **A fixture every test in a package uses** is `autouse` in that package's `conftest.py`, so modules don't repeat `pytestmark = pytest.mark.usefixtures(...)` (#15857). From `nemesis/monkey/grow_shrink/conftest.py`:
  ```python
  @pytest.fixture(autouse=True)
  def events(events):
      """Every test in this package exercises code that emits SCT events."""
      return events
  ```
- **Several types under one test** use one parametrized fixture (see the `logger` fixture in AP-6).
- **Side effects** (`side_effect` functions, retry simulations) live in a fixture factory that takes what varies, such as `first_error`, and returns the state the asserts read. Tests contain no inner functions (#13718).
- **Isolation patches** that keep the code away from a cloud go in one `autouse` fixture. Each test then lists only the patches its own case needs (#14376).
- **`autouse` only when every test in scope needs it.** A fixture one module uses is requested by name in that module (#15382).
- **A helper that needs `request`, `tmp_path`, or another fixture** is a fixture or a fixture factory. Tests then don't pass `tmp_path` on every call (#14034, #14818).
- **Test data** that fixtures are built from is a fixture, or inlined into the one fixture that uses it, not a module constant next to fixtures (#16589, #16651).
- **A builder every test calls on its first line** becomes a fixture (#16590).
- **A value that is expensive to compute** is a `scope="module"` or `scope="session"` fixture (#13992).
- **Fixture names**: name the function after what it provides. Don't use `@pytest.fixture(name=...)` aliases (#14034).

Integration tests follow the same rule: code that picks or builds the test's input is setup (#16444, #16457).

Pure data builders that take only plain arguments and return plain data (a JSON payload, a log line) may stay as public module-level functions with a docstring. Parametrize lists are built the same way (AP-11).

---

### P-16: Clear Singleton State in Teardown

Classes with `metaclass=Singleton` (`NodeLoadInfoServices`, `AdaptiveTimeoutStore` subclasses, `AzureService`) keep state for the whole worker process. Clear it in an `autouse` fixture before and after `yield`.

**Symptom:** tests pass with `-n0` and fail with `-n2`, with stale node names or cached values.

```python
@pytest.fixture(autouse=True)
def clear_node_load_info_services_singleton():
    """Start and end each test with an empty NodeLoadInfoServices cache."""
    NodeLoadInfoServices()._services.clear()
    yield
    NodeLoadInfoServices()._services.clear()
```

- Clear before the test as well as after it. Another fixture or module (such as `FakeTester`, which builds a `NemesisNodeAllocator`) may create the Singleton and never clear it, and under `-n2` the next test on that worker inherits it (#15996).
- When the test needs a fresh instance, drop the instance itself before and after, as `test_config.py` does: `Singleton._instances.pop(AzureService, None)`.
- Fix a race between tests by isolating the state in a fixture. If the race can't be removed, group the tests with `@pytest.mark.xdist_group("<name>")` and give the reason in the docstring. Don't reset shared state inside a helper (#14872, #15219).
- A Singleton with no public reset is the one place a fixture touches a private attribute such as `_services`. Don't add a reset to `sdcm/` just for tests (P-20).
- Put the fixture in `conftest.py` when several modules share the Singleton.
- Read results from the instance the fixture passed in. `SomeStore()` in the test body returns the shared instance, which other tests may have changed.

```python
def test_decommission_stores_metrics(fake_node, adaptive_timeout_store):
    """The decommission timeout is recorded in the store passed to adaptive_timeout."""
    with adaptive_timeout(..., stats_storage=adaptive_timeout_store):
        ...

    assert adaptive_timeout_store.get(operation="DECOMMISSION")
```

---

### P-17: Assert Every Claim in the Test Name

Reviewers trust the name. "Never reaches the network", "does not retry", and "logs a warning" each need an assert that fails when the claim breaks (#16093).

```python
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(pipeline, remote_connections, restore_root_logger):
    """Linting resolves images from stubs and opens no outbound connection."""
    ...
    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
```

The `remote_connections` fixture in `unit_tests/unit/lint/conftest.py` monkeypatches `socket.socket.connect` and `connect_ex`, lets loopback through, and returns the list of refused addresses. It refuses as well as records, so a missing stub never makes a real call.

---

### P-18: Mutation-Check Every New Test

A passing test proves its assertions held. A mutation check proves the code under test is what made them hold. Do this for each new test before calling it done:

1. Break the code under test the way the test claims to catch it: delete the stub entry, invert the condition, return early.
2. Run the test and confirm it **fails**, with a message that points at the break.
3. Revert the break and confirm the test passes again.

```bash
# e.g. comment out one entry in _CLOUD_API_PATCHES, then:
uv run python -m pytest unit_tests/unit/lint/test_validator.py -n0 -q   # must FAIL
git checkout sdcm/utils/lint/validator.py                           # restore
```

For each stub, list what the real code does that the stub skips, and cover the parts the fix depends on with their own test (#16142). Name the mutations you checked in the PR description, for example: "with the `_shutdown_pool()` calls removed, both parametrizations fail".

**For a bug fix, run every new case against the pre-fix code** (#15727, #16408, #16545, #16665). Commit the fix first; otherwise the restore below discards it:

```bash
git checkout HEAD~1 -- sdcm/<fixed_module>.py                            # pre-fix code
uv run python -m pytest unit_tests/unit/test_<subject>.py -n0 -q         # every new case FAILS
git checkout HEAD -- sdcm/<fixed_module>.py                              # restore the fix
```

Each case must fail on an assert about the bug. A case that passes on the old code proves nothing; add the input that triggers the bug. A case that fails only with an `AttributeError` because the fixed method is new, or a `TypeError` from a mock's child attribute, doesn't pin the bug either. Check those cases with a mutation of the new code instead.

---

### P-19: File Layout

A test module reads top to bottom in this order: module docstring, imports, constants, fixtures, data builders, tests. Fixtures shared by several modules go in `conftest.py` (#14884, #15633, #16633, #16735).

```python
"""Tests for sdcm.nemesis.monkey.modify_table."""

import pytest

from sdcm.nemesis.monkey.modify_table import ModifyTableCommentMonkey

MODULE = "sdcm.nemesis.monkey.modify_table"


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with a single non-system table."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    return base_runner


def test_comment_monkey(runner):
    """disrupt() applies a random comment with ALTER TABLE."""
    ...
```

- **Names**: no leading underscore on fixtures, helpers, or constants (`set_params`, `MODULE`, `stub_common_helpers`). pytest injects fixtures by name, and nothing in a test module is private (#15486, #15591, #16633, #16735).
- **Descriptive names**: a fixture is named for what it provides (`stub_slow_waits`, `restore_snapshot_runner`), and variables use whole words (`tester`, not `t`) (#14256, #16735).
- **Names that match the asserts**: a fake counter that counts attempts is `store_attempts`, not `store_count`, and a stub default client is `dummy_default_client` (#13527, #13947).
- **One subject per module**: tests that need nothing else from a file go in a new module (#16633). A module with many groups becomes a package with one module per group and a shared `conftest.py`, like `unit_tests/unit/nemesis/monkey/grow_shrink/` (#15857). A file that is already long gets a new module for a new subject (#16737).
- **Mirror the production layout**: tests of `sdcm/<package>/<module>.py` go in `unit_tests/unit/<package>/test_<module>.py`, like `unit_tests/unit/xcloud/test_api_client.py` (#16590). A new backend or subsystem starts its own test package and `conftest.py` (#14145, #14182, #15219).
- **Large inputs**: one file per scenario under `unit_tests/test_data/<subject>/`, loaded through `test_data_dir`, not long module-level strings (#13964).
- **Header**: a new module copies the license header with the current year (#15591, #15608).
- **Only what the test needs**: no tests that fail on purpose to demo CI, no comments left over from instructions to an AI tool, no `# type: ignore` (nothing type-checks tests), and one `with a, b:` instead of nested `with` blocks (#13527, #13964, #14034, #14875). A comment that points at an issue gives the full URL (#15096).
- **Production helpers that only tests call** are dead code. Delete them, and point the test at the helper production uses (#16736).

**Check:** the first `def test_` in the file comes after every fixture and helper.

---

### P-20: Test-Only Safeguards Live in Fixtures

Code that exists only to detect a missing stub or mock belongs in a test fixture. `sdcm/` keeps only what production needs (#16093).

```python
# unit_tests/unit/lint/conftest.py
@pytest.fixture
def remote_connections(monkeypatch):
    """Refuse every outbound connection and record where it was headed."""
    attempts = []
    ...
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    return attempts
```

Guards against real-world misuse, such as input validation or refusing to run without credentials, protect production and stay in `sdcm/`.

---

### P-21: The Test Body Is Act and Assert

When setup is longer than the act and assert lines, move it into a fixture named for the scenario (#13333, #15486, #15789). The test then reads as one action and its checks. Abridged from `test_manager.py`, where `restore_snapshot_runner` builds the cluster, Manager, and snapshot mocks:

```python
def test_restore_happy_path_existing_keyspace(restore_snapshot_runner, snapshot_tag, restored_ks):
    """The keyspace already exists: restore the data, validate it, then drop the keyspace."""
    MgmtRestore(restore_snapshot_runner).disrupt()

    mgr_cluster = restore_snapshot_runner.cluster.get_cluster_manager.return_value
    mgr_cluster.create_restore_task.assert_called_once_with(
        restore_data=True, location_list=["s3:backup-bucket-us-east-1"], snapshot_tag=snapshot_tag
    )
    restore_snapshot_runner.cluster.restart_scylla.assert_not_called()
    restore_snapshot_runner.target_node.run_cqlsh.assert_called_once_with(f'DROP KEYSPACE IF EXISTS "{restored_ks}";')
```

When two tests need almost the same setup, build one fixture on top of the other instead of copying it.

---

### P-22: Fast, Deterministic Timing

A unit test takes milliseconds and gives the same result on a slow machine. Timing is the most common source of flaky SCT tests (#13527, #15346, #16034, #16406, #16502).

- **No wall-clock asserts.** A stuck call already hangs the test, so `elapsed < 3` adds nothing but flakiness under `-n2` (#15346). To check a timeout argument, assert what the code did with it.
- **No counts after `time.sleep`.** "At least N runs in 2 seconds" breaks on a slow machine. Call the cycle method directly, or wait on a `threading.Event` with a deadline (#13527).
- **Fixed waits are patched** in an `autouse` fixture, at the module that calls them. An unpatched `time.sleep(5)` made 88 provisioning tests take 157 seconds (#16034):
  ```python
  @pytest.fixture(autouse=True)
  def no_retry_sleep():
      """Skip the fixed waits between retries; the tests check what is retried, not how long it waits."""
      with patch("sdcm.utils.decorators.time.sleep"):
          yield
  ```
- **Retry loops run on a fake clock.** Patch `time.monotonic` and `time.sleep` together, so the deadline passes at once and exact call counts mean something (#16502).
- **Stop signals**: give the code under test a long timeout and the test a short one (`future.result(timeout=5)`). A broken stop then fails fast every time, not as a race at the same deadline (#16502).
- **No runtime machinery the test doesn't check**: no extra threads or queues that mirror production, and no heavy real work (decoding, compression of large inputs) on every PR (#14463).

Find slow tests with `uv run python -m pytest unit_tests/unit/test_<subject>.py --durations=10`.
