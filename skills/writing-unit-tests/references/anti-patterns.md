# Unit Test Anti-Patterns

Broader testing anti-patterns that reduce test value. See also: [common-pitfalls.md](common-pitfalls.md) for specific pitfalls (P-1 through P-20).

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

**Variant — stub subclasses that override the logic under test.** A subclass of the real class that overrides a method the test depends on has the same problem: the override runs, the real method does not.

❌ **Bad — PR [#16142](https://github.com/scylladb/scylla-cluster-tests/pull/16142) overrode `remote_pid` and the command template:**
```python
class _StubSSHLogger(SSHGeneralSystemdLogger):
    @property
    def _logger_cmd_template(self) -> str:
        return "cat {since}"

    @property
    def remote_pid(self) -> str:
        return "1234"
```

✅ **Good — the real class; mock only the node, so the real `remote_pid` runs:**
```python
def _node(remote_pid: str = "1234") -> MagicMock:
    """A node whose remoter answers the pid-file lookup done by SSHLoggerBase.remote_pid."""
    node = MagicMock()
    node.remoter.run.return_value = SimpleNamespace(ok=bool(remote_pid), stdout=remote_pid)
    return node


logger = SSHGeneralSystemdLogger(node=_node(), target_log_file=str(tmp_path / "target.log"))
```

The no-pid case is then the same real class with `_node(remote_pid="")`, not another stub subclass.

**How to spot it:** if you grep the test file and find the same method bodies from `sdcm/` appearing verbatim, the tests are copies. A good test constructs the real `sdcm.*` class and mocks only at the external boundary (network, file system, cloud APIs).

**Correct approach:** always instantiate the real class and mock only its external I/O. Only fall back to `MagicMock(spec=RealClass)` as a last resort when the real constructor has unavoidable heavy side effects that cannot be mocked — and document why.

## AP-7: Asserting on the Text of Non-Python Code (Bash, Groovy, ...)

Some SCT code is not Python: shell scripts that Python helpers generate and run on nodes (`configure_vector_target_script`, install snippets), Jenkins Groovy in `vars/*.groovy` and `*.jenkinsfile`, and standalone `*.sh` scripts. Agents sometimes "test" this code from pytest by reading it as a string, from a helper's return value or from the file on disk, and asserting on substrings. Such a test does not check behavior. The text can be exactly what the test expects and still fail when it runs: wrong path, missing package, a systemd unit that ignores the setting, a Groovy step that Jenkins rejects, a distro that behaves differently. The test also breaks on harmless edits like reordering lines, changing quoting, or rewording a comment, so it costs maintenance and gives no coverage.

Real example: PR [#16165](https://github.com/scylladb/scylla-cluster-tests/pull/16165) ("fix(distro): make SCT work on current Fedora hosts") added `unit_tests/unit/provisioner/test_vector_target_script.py`. The review asked to drop the file because testing text inside a bash script is not useful. Agents have tried the same with Groovy pipeline code.

❌ **Bad — tests from PR #16165, abridged (dropped in review):**
```python
def test_vector_stop_timeout_outlasts_graceful_shutdown():
    script = configure_vector_target_script(host="10.0.0.1", port=6000)

    drop_in = "/etc/systemd/system/vector.service.d/sct-stop-timeout.conf"
    assert f"cat > {drop_in} <<'EOF'\n[Service]\nTimeoutStopSec=90s\nEOF" in script
    assert script.index(drop_in) < script.index("systemctl daemon-reload") < script.index("systemctl restart vector")
    assert "address: 10.0.0.1:6000" in script


def test_vector_target_script_survives_single_quote_wrapping():
    script = configure_vector_target_script(host="10.0.0.1", port=6000)
    tokens = shlex.split(shell_script_cmd(script, quote="'"))
    assert "TimeoutStopSec=90s" in tokens[2]
    subprocess.run(["bash", "-n", "-c", tokens[2]], check=True)
```

Both tests copy the script back into the assertions. Neither one shows that vector actually survives its graceful shutdown on Fedora, which was the bug being fixed. `bash -n` only shows that the script parses. It does not show that the script works.

✅ **Good — pick the first option that applies:**

1. **Run the code with its own runtime.** If the language has a test tool (for example bats for shell, or JenkinsPipelineUnit for Groovy), test the code there, not from pytest. SCT has no such framework set up today. What it has are runnable test scripts that execute the real code and check the result, for example `.github/workflows/test_cache_issues.sh` (runs the cache-issues `gh api` pagination for real) and `scripts/test-renovate-local.sh` (runs the Renovate JSONata transform against live S3 data). Follow that pattern when the code can run outside a job.
2. **Test the Python logic around it.** If Python code picks values or branches (distro checks, version gates, computed timeouts, which packages to install), move that decision into a function that returns plain data and test that function (`should_skip_epel` below is an illustration, not an existing helper):
   ```python
   @pytest.mark.parametrize("distro,expected", [
       pytest.param(Distro.FEDORA36, True, id="fedora-skips-epel"),
       pytest.param(Distro.ROCKY9, False, id="rocky-uses-epel"),
   ])
   def test_skip_epel_by_distro(distro, expected):
       assert should_skip_epel(distro) is expected
   ```
   Testing how Python handles command results is also behavior testing. Use `FakeRemoter.result_map` to return command output, then assert on what the Python code *does* with it: parsed values, raised errors, retries, events. The command pattern in the map only routes the fake. It is not what the test is checking.
3. **Otherwise, leave it to integration or manual testing in real jobs and runs.** Run a shell change on a node: an artifacts or provision test on the target distro (request the matching `test-provision-*` label), a Docker-backed integration test (see the `writing-integration-tests` skill), or a manual run on a VM. Run a Groovy or Jenkinsfile change in a real Jenkins job, for example through `staging_trigger.py` (see `docs/contrib.md`). Link the run in the PR description, and **write no unit test**.

**How to spot it:** the test reads shell or Groovy code, from a helper's return value or from a file, and every assertion is an `in`, `index()`, `startswith()`, or regex match against that text, or a `shlex.split`/`bash -n` check on it.

**Not covered by this rule:**
- Functions whose output *is* a computed command line, such as stress-tool or `nodetool` argument builders. When Python decides the arguments, asserting on the resulting arguments tests that decision. Prefer comparing parsed tokens (`shlex.split(cmd)`) to substring checks.
- Python code that parses non-Python text, such as the Jenkinsfile parser tested in `unit_tests/lint/test_jenkins_parser.py`. There the Groovy is the input, and the test checks the parser's output.

## AP-8: Asserting on Constants and Definitions

A test that reads a constant, a lookup table, or a fake object back from the module that defines it only restates the definition. It passes whenever the definition is unchanged and says nothing about whether the code that *uses* the constant works. Test the behavior the constant exists for, through the public entry point.

Real example: PR [#16093](https://github.com/scylladb/scylla-cluster-tests/pull/16093) (pipeline linter cloud-API isolation). The review called these tests valueless because they "verify a constant".

❌ **Bad — restating `_CLOUD_API_PATCHES` and `_FAKE_IMAGE` (dropped in review):**
```python
@pytest.mark.parametrize("target", [
    "sdcm.provision.azure.utils.get_released_scylla_images",
    "sdcm.utils.oci_utils.get_scylla_images_by_version",
    ...
])
def test_every_cloud_lookup_reached_from_sct_configuration_is_stubbed(target):
    assert target in _CLOUD_API_PATCHES


@pytest.mark.parametrize("attribute", ["image_id", "self_link", "id", "unique_id", "name"])
def test_fake_image_carries_every_attribute_the_resolvers_read(attribute):
    assert getattr(_FAKE_IMAGE, attribute)
```

✅ **Good — lint a pipeline that takes each lookup path and assert it never reaches the network (abridged from the PR):**
```python
@pytest.mark.parametrize("pipeline", [
    pytest.param("azure-released-image", id="azure-released-image"),
    pytest.param("oci-released-image", id="oci-released-image"),
    ...
])
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(pipeline, remote_connections, test_data_dir):
    pipeline_path = test_data_dir / "lint" / f"{pipeline}.jenkinsfile"
    is_error, message = validate_pipeline(pipeline_path, build_env(parse_jenkinsfile(pipeline_path)))

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
```

A missing patch entry now makes the matching pipeline try to connect, and a `_FAKE_IMAGE` missing a field breaks the resolver that reads it. Both fail for the real reason.

**How to spot it:** the assertion is `x in CONSTANT`, `getattr(FAKE, attr)`, or `CONSTANT[key] == literal`, and the parametrize list copies the constant's contents.

## AP-9: Depending on Production Pipelines and Configs

A unit test that reads a real file from `jenkins-pipelines/`, `test-cases/`, or `configurations/` breaks whenever someone edits that job for an unrelated reason, and silently stops covering the path if the job stops using the feature. Unit tests check the machinery, so they need inputs that belong to the test.

❌ **Bad — PR #16093 linted a real perf job (dropped in review):**
```python
# An aws pipeline whose test-case enables capacity reservation.
_CAPACITY_RESERVATION_PIPELINE = Path(
    "jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/"
    "latte-perf-regression-predefined-throughput-steps-tablets.jenkinsfile"
)
```

✅ **Good — a minimal test-only pipeline under `unit_tests/test_data/`, loaded with the `test_data_dir` fixture:**
```groovy
// unit_tests/test_data/lint/aws-capacity-reservation.jenkinsfile
// Test-only pipeline for unit_tests/lint/test_validator.py -- not a real job.
longevityPipeline(
    backend: 'aws',
    region: 'eu-west-1',
    test_name: 'longevity_test.LongevityTest.test_custom_time',
    test_config: '''["unit_tests/test_data/lint/base.yaml", "unit_tests/test_data/lint/capacity-reservation.yaml"]''',
)
```

Keep each fixture to the few keys that select the path under test, and say in a comment that it is not a real job. Code whose *purpose* is to process every production file (the `lint-pipelines` command itself) is not a unit test and is out of scope here.

## AP-10: Reimplementing Library Behavior in the Test

When a test needs to know how a library interprets its input, call the library. A helper that re-derives the library's rules is a second implementation that can drift from the real one, and it needs its own tests.

❌ **Bad — PR #16093 re-derived how `mock.patch` resolves a dotted target (dropped in review):**
```python
def _split_target(target):
    """Split a patch target the way `mock.patch` does: longest importable prefix, then attributes."""
    parts = target.split(".")
    for split_at in range(len(parts) - 1, 0, -1):
        try:
            module = importlib.import_module(".".join(parts[:split_at]))
        except ImportError:
            continue
        return module, ".".join(parts[:split_at]), parts[split_at:]
    raise AssertionError(f"no importable module in {target!r}")


def test_patch_target_attribute_exists(target):
    obj, module_path, attrs = _split_target(target)
    for attr in attrs:
        assert hasattr(obj, attr)
        obj = getattr(obj, attr)
```

✅ **Good — let `mock.patch` (or `pkgutil.resolve_name`) do it:**
```python
def test_patch_target_attribute_exists(target):
    """The dotted path must resolve, or `mock.patch` raises at linting time."""
    with patch(target):
        pass
```

When the test needs the resolved object itself, use `pkgutil.resolve_name(target)` from the standard library.

## AP-11: `pytest.skip` for a Parameter That Should Not Be Generated

A `pytest.skip` inside a parametrized test that fires for a fixed subset of parameters means the parameter list is wrong. The skipped cases add noise to every run and hide the real rule for which inputs the test applies to. Filter the list when building it. Keep `pytest.skip` for conditions known only at run time, such as a missing optional tool.

❌ **Bad — PR #16093 generated class-attribute targets, then skipped them:**
```python
@pytest.mark.parametrize("target", _target_ids())
def test_no_call_site_shadows_the_patch_target(target):
    _, module_path, attrs = _split_target(target)
    if len(attrs) > 1:
        pytest.skip(f"{target} patches an attribute on a class object -- an import cannot shadow it")
    ...
```

✅ **Good — only generate the parameters the test applies to:**
```python
def _module_level_target_ids():
    """Targets naming a module attribute -- the only kind an import can shadow."""
    return [t for t in _target_ids() if inspect.ismodule(pkgutil.resolve_name(t.rpartition(".")[0]))]


@pytest.mark.parametrize("target", _module_level_target_ids())
def test_no_call_site_shadows_the_patch_target(target):
    ...
```

## AP-12: Importing Private Names Into Tests

`from sdcm.x import _helper` in a test is a smell. Either the test is coupled to internals and should go through the public entry point (see AP-8), or `_helper` is really test infrastructure and belongs in a `conftest.py` fixture (see P-20). If a name is legitimately part of the module's tested contract, drop the underscore.

❌ **Bad — PR #16093 imported four private names from production code:**
```python
from sdcm.utils.lint.validator import (
    _CLOUD_API_PATCHES,
    _FAKE_IMAGE,
    _FAKE_OCI_IMAGE,
    LintNetworkAccessError,
    _no_remote_network,
    validate_pipeline,
)
```

✅ **Good — test the public function; the network guard moved to a fixture:**
```python
from sdcm.utils.lint.validator import validate_pipeline
```

**The same goes for library internals.** PR [#16142](https://github.com/scylladb/scylla-cluster-tests/pull/16142) imported `concurrent.futures.thread._threads_queues` to find live pool workers. A private stdlib name can change in any Python release. Observe the behavior directly instead:

❌ **Bad:**
```python
from concurrent.futures.thread import _threads_queues

def _live_pool_workers() -> set[threading.Thread]:
    return {thread for thread in _threads_queues if thread.is_alive()}
```

✅ **Good — the stubbed task records the thread it ran on, and the test checks that thread:**
```python
def journal_task():
    state.worker = threading.current_thread()
    ...

logger.stop()
journal.worker.join(timeout=10)
assert not journal.worker.is_alive(), "pool worker still alive after stop()"
```

**Exception:** a test whose subject *is* a private table, such as checking that every string target in `_CLOUD_API_PATCHES` still resolves, may import it. Keep that to the one test that needs it.
