# Fixtures and Layout

How to shape a test module: parametrized tests, setup in fixtures, file layout, and names. Read it before Phase 2 of the workflow. Each rule shows only the shape to follow.

## UT-5: One Parametrized Test per Behavior

Write one test per behavior and give it a `pytest.param(id=...)` for each case. This is the most frequent review request on SCT unit tests (#13333, #13924, #14256, #15417, #15591, #16063).

- **Cases that differ only in input and expected value** belong in one parametrized test, not several near-identical functions.
- **Several inputs checked in one test** become parameters. Each case then reports on its own, so a failing first case doesn't hide the others.
- **An expected result that depends on the case** is another parameter. Keep `if`/`else` out of the test body.
- **A new case for an existing test** is a new `pytest.param` in that test, not a new function.

```python
@pytest.mark.parametrize(
    "input_sstables, expected_output",
    [
        pytest.param(["/data/clean-Data.db", "/data/dirty-Data.db"], ["/data/clean-Data.db"], id="mixed"),
        pytest.param(["/data/dirty-Data.db"], [], id="all-dirty"),
        pytest.param(["/data/clean-Data.db"], ["/data/clean-Data.db"], id="all-clean"),
        pytest.param([], [], id="empty"),
    ],
)
def test_filter_out_sstables_with_tombstones(sstable_utils_with_mock_status, input_sstables, expected_output):
    """filter_out_sstables_with_tombstones removes dirty sstables and keeps clean ones."""
    assert sstable_utils_with_mock_status.filter_out_sstables_with_tombstones(input_sstables) == expected_output
```

When the expected error depends on the case, the exception type is a parameter too (from `test_add_remove_dc.py`):

```python
@pytest.mark.parametrize(
    "prepare_phase_active,error_type,expected_error",
    [
        pytest.param(True, UnsupportedNemesis, "prepare phase", id="active"),
        pytest.param(False, RuntimeError, "workflow started", id="inactive"),
    ],
)
def test_disrupt_prepare_phase_gate(runner, prepare_phase_active, error_type, expected_error):
    """disrupt() skips only while the prepare phase event is set."""
    ...
    with pytest.raises(error_type, match=expected_error):
        monkey.disrupt()
```

**Check:** no two test functions in the file share the same body shape with different literals.

**Parameter lists:** build the parameter list from the rule that selects the cases, in a plain function: parametrize lists are built at collection time, so they can't come from fixtures. A case the test doesn't apply to is left out of the list. Keep `pytest.skip` for conditions known only at run time, such as a missing optional tool (#16093).

## UT-6: Setup Lives in Fixtures

Mocks, patches, and the collaborators the object under test needs are built in fixtures, never in the test body. This holds even when the fixture needs no other fixture: pytest wires dependencies, runs teardown, and lets tests compose setup by name (#13333, #13924, #15486, #15591, #16589).

Two things stay in the test. One is the constructor call on those collaborators, as part of the act: `MgmtRestore(restore_snapshot_runner).disrupt()`. The other is plain data for one case, which may also be a `pytest.param`: a literal, a log line, a real SCT object built in a line or two (UT-12).

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
- **Several types under one test** use one parametrized fixture (see the `logger` fixture in UT-10).
- **Side effects** (`side_effect` functions, retry simulations) live in a fixture factory that takes what varies, such as `first_error`, and returns the state the asserts read. Tests contain no inner functions (#13718).
- **Isolation patches** that keep the code away from a cloud go in one `autouse` fixture. Each test then lists only the patches its own case needs (#14376).
- **`autouse` only when every test in scope needs it.** A fixture one module uses is requested by name in that module (#15382).
- **A helper that needs `request`, `tmp_path`, or another fixture** is a fixture or a fixture factory. Tests then don't pass `tmp_path` on every call (#14034, #14818).
- **Test data** that fixtures are built from is a fixture, or inlined into the one fixture that uses it, not a module constant next to fixtures (#16589, #16651).
- **A builder every test calls on its first line** becomes a fixture (#16590).
- **A value that is expensive to compute** is a `scope="module"` or `scope="session"` fixture (#13992).
- **Fixture names**: name the function after what it provides. Don't use `@pytest.fixture(name=...)` aliases (#14034).

Integration tests follow the same rule: code that picks or builds the test's input is setup (#16444, #16457).

Pure data builders that take only plain arguments and return plain data (a JSON payload, a log line) may stay as public module-level functions with a docstring. Parametrize lists are built the same way (UT-5).

**The test body is act and assert.** When the plain data left in a test is longer than the act and assert lines, move it into a fixture named for the scenario (#13333, #15486, #15789). The test then reads as one action and its checks. Abridged from `test_manager.py`, where `restore_snapshot_runner` builds the cluster, Manager, and snapshot mocks:

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

## UT-7: File Layout

Tests are module-level `def test_*` functions. pytest needs no classes, and fixture injection, `autouse` fixtures, and `pytest-xdist` are designed for functions.

A test module reads top to bottom in this order: module docstring, imports, constants, fixtures, data builders, tests. Fixtures shared by several modules go in `conftest.py` (#14884, #15633, #16633, #16735). Adapted from `test_modify_table.py`:

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


def test_modify_table_property_forwards_filter_out_counter(runner):
    """filter_out_table_with_counter is forwarded to get_non_system_ks_cf_list."""
    ...
```

- **One subject per module**: tests that need nothing else from a file go in a new module (#16633). A module with many groups becomes a package with one module per group and a shared `conftest.py`, like `unit_tests/unit/nemesis/monkey/grow_shrink/` (#15857). A file that is already long gets a new module for a new subject (#16737).
- **Mirror the production layout**: tests of `sdcm/<package>/<module>.py` go in `unit_tests/unit/<package>/test_<module>.py`, like `unit_tests/unit/xcloud/test_api_client.py` (#16590). A new backend or subsystem starts its own test package and `conftest.py` (#14145, #14182, #15219).
- **Helper classes** are defined at module level and instantiated in fixtures.
- **Large inputs**: one file per scenario under `unit_tests/test_data/<subject>/`, loaded through `test_data_dir`, not long module-level strings (#13964).
- **Header**: a new module copies the license header with the current year (#15591, #15608).
- **Only what the test needs**: no tests that fail on purpose to demo CI, no comments left over from instructions to an AI tool, no `# type: ignore` (nothing type-checks tests), and one `with a, b:` instead of nested `with` blocks (#13527, #13964, #14034, #14875). A comment that points at an issue gives the full URL (#15096).
- **Production helpers that only tests call** are dead code. Delete them, and point the test at the helper production uses (#16736).

**Check:** the first `def test_` in the file comes after every fixture and helper.

## UT-8: Public Names, Test Names, and Docstrings

Tests import and call public names. In new code, functions are public. A leading underscore marks nothing as private in Python and makes call sites harder to read (#15591).

- **Production code you add**: no new `_name` functions, methods, or module constants. If a test calls it, it is public.
- **Existing private names**: test through the public entry point, or drop the underscore in the PR that adds the test.
- **Test modules**: no leading underscore on fixtures, helpers, or constants (`set_params`, `MODULE`, `stub_common_helpers`). pytest injects fixtures by name, and nothing in a test module is private (#15486, #15591, #16633, #16735).
- **Library internals** (`concurrent.futures.thread._threads_queues`): observe the behavior directly instead (#16142):
  ```python
  def journal_task():
      state.worker = threading.current_thread()
      ...

  logger.stop()
  state.worker.join(timeout=10)
  assert not state.worker.is_alive(), "pool worker still alive after stop()"
  ```

Renaming the roughly 1,200 existing `_private` names in `sdcm/` is out of scope. Just don't add new ones.

**Test names** say the subject and the scenario, and the expected outcome when the test has one case: `test_<subject>_<scenario>_<expected>`. A parametrized test is named for the behavior, and its `pytest.param` ids name the cases.

```python
def test_comparable_scylla_version_unparsable_string_raises_value_error(): ...
def test_filter_out_sstables_with_tombstones(sstable_utils_with_mock_status, input_sstables, expected_output): ...
```

- **Descriptive names**: a fixture is named for what it provides (`stub_slow_waits`, `restore_snapshot_runner`), and variables use whole words (`tester`, not `t`) (#14256, #16735).
- **Names that match the asserts**: a fake counter that counts attempts is `store_attempts`, not `store_count`, and a stub default client is `dummy_default_client` (#13527, #13947).

**Docstrings:** every test and fixture has a docstring (Google format) that says what the test checks and why it matters. The name says *what*; the docstring says the goal. Notes that would sit next to an assert belong there:

```python
def test_stop_releases_the_pool_worker(logger):
    """stop() must retire the pool worker; a live one would block interpreter shutdown."""
```

## UT-9: Imports at the Top of the File

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

## Converting unittest-Style Tests

A conversion leaves no classes behind (#13952):

- `setUp` and `setup_method` become fixtures. One that every test in the class uses becomes `autouse`.
- `tearDown` becomes the code after `yield` in that fixture.
- `self.assertEqual(a, b)` becomes `assert a == b`, and `self.assertRaises` becomes `pytest.raises(..., match=...)`.
- Methods become module-level `def test_*` functions. A class that grouped one subject becomes its own module, with its fixtures in a shared `conftest.py` when several modules need them.
- Methods that differ only in literals become one parametrized test (UT-5).
