# Test Design Rules

What to test and what to assert. Each rule shows only the shape to follow. See also: [common-pitfalls.md](common-pitfalls.md) for mocking, isolation, and test structure rules (P-1 through P-22).

## AP-1: Test Behavior Through the Result

Check what the code produces, not how it gets there. Feed real input and assert on the output.

```python
def test_load_config_reads_backend(tmp_path):
    """A YAML file on disk loads into a config mapping."""
    config_file = tmp_path / "test.yaml"
    config_file.write_text("cluster_backend: docker")

    config = load_config(str(config_file))

    assert config["cluster_backend"] == "docker"
```

When the effect itself is a call to an external boundary (a remote command, a cloud API, a CQL statement), assert that call exactly. See AP-13.

## AP-2: One Parametrized Test per Behavior

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

## AP-3: Assert on What the Code Does With a Mocked Value

A mock's return value is input. Assert on the code that consumes it.

```python
def test_process_nodes_counts_nodes():
    """process_nodes() reports one entry per node returned by the cluster."""
    with patch("sdcm.cluster.get_nodes", return_value=["node1"]):
        result = process_nodes()

    assert result.count == 1
```

## AP-4: Flat Module-Level Test Functions

pytest needs no classes. Write module-level `def test_*` functions so fixture injection, `autouse` fixtures, and `pytest-xdist` work as designed.

```python
def test_evaluate_skip_default_returns_empty_list(runner):
    """A nemesis with no skip conditions reports nothing to skip."""
    assert CustomNemesisA(runner=runner).evaluate_skip() == []
```

A module covers one subject. When it grows several groups of tests, split it into a package with one module per group and a shared `conftest.py` (see P-19).

## AP-5: Reuse Existing Test Infrastructure

Start from what `unit_tests/` already provides and extend it.

- `unit_tests/lib/`: `FakeEventsDevice`, `FakeRemoter`, `make_fake_events`, `DotDict` (a dict with attribute access)
- `unit_tests/unit/nemesis/__init__.py` and `fake_cluster.py`: `TestRunner`, `make_mock_node`, `FakeTester`
- `unit_tests/unit/nemesis/monkey/conftest.py`: `base_runner`
- `unit_tests/conftest.py`: `events`, `events_function_scope`, `params`
- `SCTConfiguration.model_construct(...)`: a real config object that loads no files, instead of a `dict` posing as params (#16136, `test_cost.py`)

Code that publishes events uses the events fixtures or the fake events device. A hand-made recorder that replaces `SctEvent.publish` leaves each event marked unpublished, and its destructor logs a warning (#15346, #16498). When a PR moves tests onto a fake, some tests keep running the real event system (#13966).

When the shared fake is too heavy for the subject (`FakeTester` pulls in `BaseScyllaCluster` and `NemesisNodeAllocator`), write a minimal local fake and say in its docstring why the shared one doesn't fit (#14051, #14186).

```python
def test_publishes_critical(events_function_scope):
    """A failed health check publishes a CRITICAL event."""
    run_something()

    assert events_function_scope.get_events_by_category()["CRITICAL"]
```

```python
@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with a single non-system table for modify-table tests."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    return base_runner
```

Shared fakes, stubs, and fixtures live in `conftest.py`, `fake_cluster.py`, or `unit_tests/lib/`. Test modules import only from those, never from other test modules.

## AP-6: Exercise the Real Class, Mock Only Its I/O

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

## AP-7: Test Non-Python Code With Its Own Runtime or Through the Python Decision

Bash scripts that Python helpers generate, Groovy in `vars/*.groovy` and `*.jenkinsfile`, and `*.sh` files are checked by running them. Their text is not a test subject. Pick the first option that applies (#16165):

1. **Run the code with its own runtime.** Use the language's test tool (bats for shell, JenkinsPipelineUnit for Groovy) or a runnable test script that executes the real code and checks the result, like `.github/workflows/test_cache_issues.sh` and `scripts/test-renovate-local.sh`. A bash script that Python generates gets at least a syntax check, as in `unit_tests/unit/provisioner/test_docker_user_data.py` (#16124):
   ```python
   subprocess.run(["bash", "-n"], input=script, text=True, check=True)
   ```
2. **Test the Python logic around it.** Move the decision (distro checks, version gates, computed timeouts, package lists) into a function that returns plain data and test that function. `should_skip_epel` is an illustration, not an existing helper:
   ```python
   @pytest.mark.parametrize("distro,expected", [
       pytest.param(Distro.FEDORA36, True, id="fedora-skips-epel"),
       pytest.param(Distro.ROCKY9, False, id="rocky-uses-epel"),
   ])
   def test_skip_epel_by_distro(distro, expected):
       """EPEL is skipped on Fedora and used on Rocky."""
       assert should_skip_epel(distro) is expected
   ```
   How Python handles command output is also behavior. Return output through `FakeRemoter.result_map`, then assert on what the code does with it: parsed values, raised errors, retries, events.
3. **Otherwise, test it in a real job and write no unit test.** Run a shell change on a node (a `test-provision-*` label, a Docker integration test from the `writing-integration-tests` skill, or a manual VM run). Run a Groovy change in a real Jenkins job, for example through `staging_trigger.py` (see `docs/contrib.md`). Link the run in the PR description.

This rule does not cover functions whose output *is* a computed command line (stress-tool or `nodetool` argument builders): compare parsed tokens with `shlex.split(cmd)`. It also does not cover Python parsers of non-Python text, like `unit_tests/unit/lint/test_jenkins_parser.py`, where the text is input and the parser's output is checked.

## AP-8: Assert Behavior, Not Declarations

Test the behavior a definition exists for, through the public entry point. A declaration that is read back unchanged proves nothing about the code that uses it. These are declarations:

- **Constants and lookup tables**, e.g. `x in CONSTANT` (#16093).
- **Nemesis flags** (`disruptive`, `kubernetes`, ...). They change as nemeses evolve and are a configuration choice, not behavior (#14884, #15486, #15607).
- **Registry discovery and `__all__`** for a nemesis you added or moved. Discovery is covered once in `unit_tests/unit/nemesis/test_registry.py` (#15604, #15857).
- **Behavior tied to an internal constant nobody can configure**, such as a batch size (#15591).
- **Static yaml and config contents**: keys present, list lengths, a script name in a string. `lint_test_cases` checks test-case files (#15608).
- **Structure**: method signatures, inheritance, or "the class declares X". Test the path that uses them (#14135, #14734, #15160).
- **A default that raises `NotImplementedError`**: make the method `abc.abstractmethod` instead (#15726).
- **Unsupported features**: when support is dropped, delete its params, tests, and references. Don't add a test that it is rejected (#15906).
- **Relationships between constants**: if `A` must exceed `2 * B`, define `A` from `B` in production (#16124).
- **The mock's own setup, or one-line math on a mock** (#16040).
- **Library behavior**, such as `uuid4()` returning unique values (#16455).
- **Scenarios production cannot reach**, such as a test id that changes mid-run (#15418).

An invariant over every declaration (no config field name contains `__`, #15716) may be a test when it fails on the broken state and no linter rule can enforce it. Where a linter rule exists, such as ruff's D rules for docstrings, enable the rule instead (#16604, #16734).

For a nemesis, test what `disrupt()` does: the commands it runs, the nodes it picks, the errors it raises. From #16093, a pipeline that takes each lookup path, asserted never to reach the network:

```python
@pytest.mark.parametrize("pipeline", [
    pytest.param("azure-released-image", id="azure-released-image"),
    pytest.param("oci-released-image", id="oci-released-image"),
])
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(pipeline, remote_connections, test_data_dir):
    """Linting a pipeline that resolves a cloud image never opens a remote connection."""
    pipeline_path = test_data_dir / "lint" / f"{pipeline}.jenkinsfile"
    is_error, message = validate_pipeline(pipeline_path, build_env(parse_jenkinsfile(pipeline_path)))

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
```

A missing stub now makes the matching pipeline try to connect, so the test fails for the real reason.

## AP-9: Inputs Belong to the Test

Load inputs from `unit_tests/test_data/` through the `test_data_dir` fixture. A minimal test-only file keeps the test stable when production jobs change, and keeps covering the path when they stop using it (#16093).

```groovy
// unit_tests/test_data/lint/aws-capacity-reservation.jenkinsfile
// Test-only pipeline for unit_tests/unit/lint/test_validator.py -- not a real job.
longevityPipeline(
    backend: 'aws',
    region: 'eu-west-1',
    test_name: 'longevity_test.LongevityTest.test_custom_time',
    test_config: '''["unit_tests/test_data/lint/base.yaml", "unit_tests/test_data/lint/capacity-reservation.yaml"]''',
)
```

Keep each file to the few keys that select the path under test, and say in a comment that it is not a real job. Code whose purpose is to process every production file (the `lint-pipelines` command) is not a unit test.

The same holds for `configurations/`, `test-cases/`, and the trigger yamls: unit tests read their own minimal copies, so the tests check code and don't fail when a production file changes (#15380).

## AP-10: Let the Library Answer Questions About Itself

When a test needs to know how a library reads its input, call the library: `pkgutil.resolve_name` for a dotted path, `shlex.split` for a command line, `yaml.safe_load` for YAML. Don't reimplement its parsing in the test (#16093).

## AP-11: Generate Only the Parameters a Test Applies To

Build the parameter list from the rule that selects the cases, in a plain function: parametrize lists are built at collection time, so they can't come from fixtures. A case the test doesn't apply to is left out of the list. Keep `pytest.skip` for conditions known only at run time, such as a missing optional tool (#16093).

## AP-12: Public Names Only

Tests import and call public names. In new code, functions are public. A leading underscore marks nothing as private in Python and makes call sites harder to read (#15591).

- **Production code you add**: no new `_name` functions, methods, or module constants. If a test calls it, it is public.
- **Existing private names**: test through the public entry point, or drop the underscore in the PR that adds the test.
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

## AP-13: Assert Exact Calls, Errors, and Results

When a mock call is the observable effect, assert the full call with `assert_called_once_with(...)`. Don't check only `call_count`, `assert_called_once()`, or one argument picked out of `call_args` (#14884, #15591, #15607). The exact call catches a wrong argument, and the failure message shows the difference.

```python
def test_modify_table_property_forwards_filter_out_counter(runner):
    """filter_out_table_with_counter is forwarded to get_non_system_ks_cf_list."""
    monkey = ModifyTableCommentMonkey(runner)
    monkey.modify_table_property(name="x", val=1, filter_out_table_with_counter=True)

    monkey.runner.cluster.get_non_system_ks_cf_list.assert_called_once_with(
        db_node=monkey.runner.target_node,
        filter_out_table_with_counter=True,
        filter_out_mv=True,
    )
```

For several calls, compare the whole list, written out literally: `assert mock.call_args_list == [call(...), call(...)]`. For calls that take no arguments, the order is the effect: attach the mocks to one parent with `attach_mock` and compare `parent.mock_calls` (#13718, #15160).

The same exactness applies to every result:

- **Errors**: `pytest.raises(SpecificError, match="...")`. `pytest.raises(Exception)` also catches the `AttributeError` of a method that doesn't exist, so the test passes for the wrong reason (#13527, #14818, #15686). Validation cases become one parametrized test of input, exception, and pattern.
- **Generated text**: one `==` on the whole command or string, not several `in` checks (#13509).
- **Everything the test put in**: a test that feeds ten fields asserts all ten; a test that provokes three errors asserts the exact list (#14818).
- **Counts**: `assert len(results) == 1`, not `assert results`, so a duplicate match fails (#16457).
- **Cleanup**: compare the state before and after the act, not the number of commands issued (#16090).
- **The path not taken**: the test of the generic path asserts the special behavior did not happen, with `assert_not_called()`. Keep those asserts when editing a test (#16180, #16535).
- **The real cause first**: when the code collects errors instead of raising, assert `not errors` before the derived check, so a failure shows the cause (#16450).
- **The goal, once**: pick the assert that states what the test is for, and drop asserts it already implies (#14062, #14182).

## AP-14: Every Assert Checks the Case Under Test

Each assert must be able to fail because of the code under test, in this case (#15604, #16063, #16518, #16603, #16735).

- Assert on values the code produced, not on preconditions the test arranged itself.
- Assert on this case's result, not on a fact that holds for every case (such as the default severity of an event type).
- Assert on outcomes (what was chosen, returned, raised, or run), not on internal state left untouched. For seeded randomness, two runs with the same seed make the same choices.
- Aim at the effect that matters. For a network nemesis, that is the firewall rules it adds, not whether it was discovered.

Adapted from `test_sct_events_database.py`: the line is classified, and the classified event carries the suppressed severity:

```python
def test_info_level_runtime_error_line_is_suppressed():
    """Real example (SCT-772) that used to become RUNTIME_ERROR/ERROR."""
    line = (
        "2026-07-30T11:37:43.815 longevity-10gb-3h-master-db-node-41da73ad-eastus-1  !INFO | scylla[12796]  "
        "[shard 0: gms] rpc - client 10.0.0.9:7000: ignoring error response: seastar::rpc::remote_verb_error "
        "(std::runtime_error (Got stream_blob_cmd::error from peer 7336616b-4533-4c59-a3d1-f6d825de4d8a))"
    )
    event = classify(line)

    assert event.type == DatabaseLogEvent.INFO.type
    assert event.severity == Severity.SUPPRESS
```

## AP-15: Real Objects and Plain Data

Build inputs from real SCT types and plain Python data. Keep fakes to canned return values (#13924, #14884, #15591, #15633, #16710).

- **Events, configs, results**: construct the real class. `TestStepEvent(step=..., publish_event=False)` with `add_error(...)` is an error event.
- **Structured input**: a `dict` or the real dataclass. Use `SimpleNamespace` or an ad hoc `namedtuple` only when no real type exists and the code reads attributes.
- **External systems**: return canned values for each call. The test does not reimplement Scylla, Manager, or a cloud API.
- **Hard-to-reach code**: use the existing fixture (for example `events` for code wrapped in event filters). When a test would need a simulator or `__wrapped__` to bypass a decorator, extract the logic into a function first, or leave it to integration tests.

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

## AP-16: Inputs From Real Runs, Expected Values From Intent

- **A bug found in a test run**: use the real log lines or command output from that run as input (#16063, #16665). Keep them inline if short, under `unit_tests/test_data/` if long, and name the issue or run in the docstring.
- **Expected values**: derive them from the intended behavior (the issue, the docs, the reasoning in the PR), never by running the code and copying its output. #14983 found a bug that an earlier test had pinned as expected.
- **Expected numbers**: write the worked-out literal for each scenario. A named production constant may be an input (#14304), but the test never re-runs the formula under test to get its expectation (#16324).
- **Negative cases**: use values that are obviously invalid, such as `"nonexistent_option"`, so the reader knows they are not real (#16710).
- **Arbitrary URLs, hosts, and ids**: obvious stubs such as `https://example.com/build.tar.gz`. A production URL can reach the real service and pollute its logs (#15695).
- **Input shape**: build the input the way the real caller builds it, in the same argument order (#15769).
- **Copies of production data**: a pattern list copied into a test keeps the production order, since order decides which pattern matches. Better, import the shared constant (#15382).
- **External tool formats**: pin the output format of a CLI with a real sample in a test, not with prose in a docstring. The test fails when the format changes (#15591).
- **Constants**: inline a value used once. Data several tests share is a fixture (P-15); module constants are for fixed strings such as a patch-target `MODULE` (#16589, #16633, #16651).

## AP-17: Cover Every Behavior the Change Adds

Most "missing tests" review comments name a branch the PR added and no test reaches (#14463, #14561, #15456, #16345, #16491, #16545, #16674, #16769). Before writing tests, list what the diff adds or changes, and give each item a case:

- **Each new branch, guard, and parser path**, including the happy path and the fallback (#14561, #16233).
- **Each changed argument at a call site**, pinned with `assert_called_once_with` (#16836).
- **The caller path, not only the helpers.** Send the input through the entry point the backend uses (#16378, #16462).
- **The behavior the fix is for.** A bug fix tests the motivating behavior, and the PR description names the bug and the test (#14669, #15364).

Logic that sits in a module no unit test can import (a test class such as `mgmt_cli_test.py`) moves to `sdcm/` first (#16251). A branch that only formats an error message, or a one-line guard that a CI stage already runs, may go without a unit test (#16532).

## AP-18: Test Each Behavior Once

- **Before adding a test**, search `unit_tests/` and the linters for one that already checks the behavior. Extend it, or keep the better of the two (#14062, #14370, #14616, #15608).
- **After changing code**, delete tests the change made identical to another or no longer describe, and drop parameter rows that another test already covers (#16568, #16651, #16910).
- **Test what the PR changed**, not unchanged code that other tests cover (#15604, #15608).
- **Rename a test when its asserts outgrow its name**, or split it (#16524).
