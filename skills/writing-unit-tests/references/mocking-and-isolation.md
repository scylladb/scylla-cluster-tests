# Mocking and Isolation

What to mock, how to fake it, and how to keep tests independent, fast, and deterministic. Read it before Phase 2 of the workflow, and when a test is slow or flaky. Each rule shows only the shape to follow.

## UT-10: Exercise the Real Class, Mock Only Its I/O

Construct the real `sdcm` class and fake only what it talks to: the node, the remoter, the cloud client. Then the code under test is what runs.

- **Build the subject through its constructor**, with mocked collaborators. `Class.__new__(Class)`, `method.__get__(stub)`, and `types.MethodType(real_method, mock)` skip the real initialization, and on the pre-fix code the test fails with an `AttributeError` instead of the bug (#14182, #14734, #14906, #15727, #16469).
- **Keep production logic out of the test**: out of helpers, stubs, subclasses that override the method under test, integration-test steps, and expected values. A stub class that needs a real method takes it from the real class, such as `description = NemesisBaseClass.__dict__["description"]` (#14307, #14508, #15253, #16545, #16604, #16812).
- **Reach the real object cheaply.** A real remoter pointed at a refused local address (`127.0.0.1:1`) fails at once, where an unroutable address waits for a timeout (#16406).

Adapted from the remote logger tests (#16142): real loggers, with a fake node that answers the pid-file lookup done by `SSHLoggerBase.remote_pid`:

```python
@pytest.fixture
def remote_pid():
    """PID the node reports for the remote logger process."""
    return "1234"


@pytest.fixture
def node(remote_pid):
    """A node whose remoter answers the pid-file lookup done by SSHLoggerBase.remote_pid."""
    db_node = MagicMock()
    db_node.remoter.run.return_value = Result(stdout=remote_pid, exited=0 if remote_pid else 1)
    return db_node


@pytest.fixture(params=["ssh", "hdr"])
def logger(request, node, tmp_path):
    """One real logger of each kind that owns a log-follower pool."""
    if request.param == "hdr":
        return HDRHistogramFileLogger(
            node=node, remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
        )
    return SSHGeneralSystemdLogger(node=node, target_log_file=str(tmp_path / "target.log"))
```

The no-pid case uses the same real classes and overrides one fixture: `@pytest.mark.parametrize("remote_pid", [pytest.param("", id="no-pid")])`.

**Check:** every class the test exercises is imported from `sdcm`, and no method body from `sdcm/` appears in the test file. When a stand-in mock is unavoidable, give it `spec=RealClass`, so a missing attribute raises the real `AttributeError` instead of returning a child mock (#16444, #16590).

**No network.** The `fake_remoter` autouse fixture blocks SSH. HTTP-based services (boto3, requests, REST APIs) are **not** blocked automatically, so mock them with `unittest.mock.patch`, `monkeypatch`, or `moto`. Mock the outermost external call (the remote command, the HTTP request, the SDK client) and let SCT logic run. An illustration that runs against `sdcm.utils.common.s3_download_dir`:

```python
@pytest.fixture
def s3_client():
    """S3 client that lists one log file and one directory marker under the prefix."""
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": [{"Key": "logs/a.log"}, {"Key": "logs/sub/"}]}
    with patch("boto3.client", return_value=client):
        yield client


def test_s3_download_dir_skips_directory_markers(s3_client, tmp_path):
    """Each object under the prefix lands at its relative path; directory markers are skipped."""
    s3_download_dir("bucket", "logs", str(tmp_path))

    s3_client.download_file.assert_called_once_with("bucket", "logs/a.log", str(tmp_path / "a.log"))
```

For code that makes many AWS calls, wrap the fixture body in `mock_aws()` from moto and seed the in-memory service there. The test still calls SCT code and asserts on what it returns.

## UT-11: Reuse Existing Test Infrastructure

Start from what `unit_tests/` already provides and extend it.

- `unit_tests/lib/`: `FakeEventsDevice`, `FakeRemoter`, `make_fake_events`, `DotDict` (a dict with attribute access)
- `unit_tests/unit/nemesis/__init__.py` and `fake_cluster.py`: `TestRunner`, `make_mock_node`, `FakeTester`
- `unit_tests/unit/nemesis/monkey/conftest.py`: `base_runner`
- `unit_tests/conftest.py`: `events`, `events_function_scope`, `params`
- `SCTConfiguration.model_construct(...)`: a real config object that loads no files, instead of a `dict` posing as params (#16136, `test_cost.py`)

Code that publishes events uses the events fixtures or the fake events device. A hand-made recorder that replaces `SctEvent.publish` leaves each event marked unpublished, and its destructor logs a warning (#15346, #16498). When a PR moves tests onto a fake, some tests keep running the real event system (#13966).

`events_function_scope` (from `unit_tests/conftest.py`) creates an isolated events system per test: fresh temp directory, events device, and registry patcher. Read the raw events log with `get_raw_events_log()` and the events logger with `get_events_logger()`. Use `events` (module scope) when many tests share expensive event setup and don't interact.

Abridged from `diagnostic_collector/test_handlers.py`:

```python
def test_event_exception_handler_publishes_event(mock_collector, events_function_scope, method, stage):
    """Each stage publishes a real TestFrameworkEvent naming the failing collector, stage and error."""
    getattr(EventExceptionHandler(), method)(mock_collector, RuntimeError("boom"))

    published = events_function_scope.get_events_by_category()[Severity.WARNING.name]
    assert len(published) == 1, f"exactly one WARNING event should be published, got {published}"
    ...
```

Extend a shared fixture in the module that needs more:

```python
@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with a single non-system table for modify-table tests."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    return base_runner
```

When the shared fake is too heavy for the subject (`FakeTester` pulls in `BaseScyllaCluster` and `NemesisNodeAllocator`), write a minimal local fake and say in its docstring why the shared one doesn't fit (#14051, #14186).

Shared fakes, stubs, and fixtures live in `conftest.py`, `fake_cluster.py`, or `unit_tests/lib/`. Test modules import only from those, never from other test modules.

## UT-12: Real Objects and Plain Data

Build inputs from real SCT types and plain Python data. Keep fakes to canned return values (#13924, #14884, #15591, #15633, #16710).

- **Events, configs, results**: construct the real class. `TestStepEvent(step=..., publish_event=False)` with `add_error(...)` is an error event.
- **Structured input**: a `dict` or the real dataclass. Use `SimpleNamespace` or an ad hoc `namedtuple` only when no real type exists and the code reads attributes.
- **External systems**: return canned values for each call. The test does not reimplement Scylla, Manager, or a cloud API.
- **Hard-to-reach code**: use the existing fixture (for example `events` for code wrapped in event filters). When a test would need a simulator or `__wrapped__` to bypass a decorator, extract the logic into a function first, or leave it to integration tests.
- **Library formats**: when a test needs to know how a library reads its input, call the library: `pkgutil.resolve_name` for a dotted path, `shlex.split` for a command line, `yaml.safe_load` for YAML. Don't reimplement its parsing in the test (#16093).

```python
def test_format_error_includes_all_error_steps():
    """All failing steps appear in the exception message."""
    event_a = TestStepEvent(step="Step A", publish_event=False)
    event_a.add_error(["err A"])
    event_b = TestStepEvent(step="Step B", publish_event=False)
    event_b.add_error(["err B"])

    with pytest.raises(NemesisSubTestFailure, match=r"(?s)Step: Step A\. Error:.*Step: Step B\. Error:"):
        SlaMonkeyBase.format_error_for_sla_test_and_raise([event_a, event_b])
```

## UT-13: Fakes Behave Like the Real Thing

A fake behaves like the boundary it replaces, or the test passes where production fails.

**Return real types.** A `MagicMock` return value creates attributes on access and breaks serialization. An illustration with the real `SSHKey`:

```python
@pytest.fixture
def ssh_key():
    """KeyStore hands out a real SSHKey, so code that writes the key to disk gets bytes."""
    key = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA\n", private_key=b"dummy\n")
    with patch.object(KeyStore, "get_ssh_key_pair", return_value=key):
        yield key
```

The fake also matches the boundary in these ways:

- **Same shape as production today.** When the PR changes what a function returns, update every fake of it (#14423).
- **No more lenient than the real thing.** A fake `send_files` that runs `cp` in a shell expands `$HOME`, while rsync leaves it literal (#15697).
- **Built from the same source.** When production builds a name and another place matches it (a process name and its `pgrep` pattern), build the input with the real builder, not a hand-written string (#16854).
- **A stub that must not be called** is `MagicMock(side_effect=AssertionError("must not resolve"))` (#16233).

**Set attributes explicitly.** Give a fake exactly the attributes the code reads: a `MagicMock` with named attributes set, or a real object (UT-12). Every attribute then has a value the test chose.

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

## UT-14: Patch Where the Name Is Called

`from X import func` copies the reference into the importing module. Patch the import site, and the source too if other callers read it there. The fragments below show the patch target; in a test module they sit inside a fixture (UT-6).

```python
with (
    patch("sdcm.utils.common.convert_name_to_ami_if_needed", return_value="ami-fake"),
    patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", return_value="ami-fake"),
):
    config = SCTConfiguration()
```

The config import site is `sdcm.sct_config.config`, not `sdcm.sct_config`. The package `__init__.py` does not re-export these names, so a patch on `sdcm.sct_config.<name>` raises `AttributeError`. This also holds for names defined inside the package. `config.py` does `from sdcm.sct_config.types import _check_file_exists`, so the target is `sdcm.sct_config.config._check_file_exists`.

**Widely imported classes.** `KeyStore` is imported by name in 20+ modules. `patch.object` changes the class itself, so every importer sees the patch.

```python
with patch.object(KeyStore, "sync", return_value=None):
    ...
```

**Session and module fixtures.** `monkeypatch` is function-scoped. Wider-scoped fixtures use `unittest.mock.patch` and `yield`.

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

## UT-15: Give FakeRemoter a Result for Every Command

Code that runs remote commands needs a `FakeRemoter.result_map` entry for each command. A missing entry raises `ValueError: No fake result specified for command`. An illustration:

```python
@pytest.fixture
def remote_commands(fake_remoter):
    """The commands the code under test runs, and the output each returns."""
    fake_remoter.result_map = {
        re.compile(r"nodetool status"): Result(stdout="UN 10.0.0.1", exited=0),
        re.compile(r"cat /etc/scylla/scylla.yaml"): Result(stdout="cluster_name: test", exited=0),
    }
```

`fake_remoter` is the `FakeRemoter` class, and `result_map` is a class attribute, so every test that runs remote commands requests a fixture that sets it.

## UT-16: Environment, Files, and Paths

Each test gets its state from fixtures, its environment variables from `monkeypatch`, and its files from `tmp_path`, so it passes alone, in any order, and in parallel.

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
- **Local binaries**: a subprocess that runs a local binary or one of our scripts still makes a unit test (#14752). A binary that the hydra image lacks gets a `pytest.skip` when `shutil.which` doesn't find it.

**Files.** Write files under `tmp_path`. pytest creates a fresh directory per test and cleans it up.

```python
@pytest.fixture
def config_file(tmp_path):
    """A minimal YAML config on disk."""
    path = tmp_path / "test_config.yaml"
    path.write_text("key: value")
    return path
```

## UT-17: Clear Singleton State

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
- Fix a race between tests by isolating the state in a fixture. Don't reset shared state inside a helper (#14872, #15219). `@pytest.mark.xdist_group("<name>")` groups tests only under `--dist loadgroup`, which `sct.py integration-tests` passes and `sct.py unit-tests` does not, so it is a last resort for integration tests, with the reason in the docstring.
- A Singleton with no public reset is the one place a fixture touches a private attribute such as `_services`. Don't add a reset to `sdcm/` just for tests (UT-20).
- Put the fixture in `conftest.py` when several modules share the Singleton.
- Read results from the instance the fixture passed in. `SomeStore()` in the test body returns the shared instance, which other tests may have changed.

```python
def test_decommission_stores_metrics(fake_node, adaptive_timeout_store):
    """The decommission timeout is recorded in the store passed to adaptive_timeout."""
    with adaptive_timeout(..., stats_storage=adaptive_timeout_store):
        ...

    assert len(adaptive_timeout_store.get(operation="DECOMMISSION")) == 1
```

## UT-18: Fast, Deterministic Timing

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

## UT-19: Lazy Initialization for Clients in Production Code

Module-level code runs at import time, during test collection, before any fixture is active. Create clients and fetch credentials on first use.

```python
# sdcm/utils/docker_utils.py
@cache
def get_docker_hub_credentials() -> dict:
    LOGGER.debug("Get Docker Hub credentials")
    return KeyStore().get_docker_hub_credentials()
```

## UT-20: Test-Only Safeguards Live in Fixtures

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
