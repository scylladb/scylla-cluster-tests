# Correctness, Error Handling, and Documentation (T13 + T6 + T2)

Data-backed from 140 T13 (logic correctness) + 98 T6 (error handling) + 137 T2 (documentation)
review comments across 11,312 SCT PRs.

---

## T13: Logic Correctness

Logic bugs in SCT are especially dangerous because they often produce wrong test results silently —
a test "passes" even though the behavior is wrong.

### Rule 1: Data Format Must Match API Contracts

The most common correctness bug: passing a Python object where an API expects a serialized string.

> Reviewer quote: "this code is wrong, seeds need to be a comma separated list of addresses, not an actual list"

**Bad** — Python list passed where string is required:

```python
# sdcm/cluster.py
def configure_scylla_yaml(self, seeds: list[str]) -> None:
    scylla_config = {
        "seeds": seeds,  # WRONG: Scylla expects "10.0.0.1,10.0.0.2" not ["10.0.0.1", "10.0.0.2"]
        "cluster_name": self.name,
    }
    self.node.remoter.run(f"echo '{yaml.dump(scylla_config)}' > /etc/scylla/scylla.yaml")
```

**Good**:

```python
def configure_scylla_yaml(self, seeds: list[str]) -> None:
    scylla_config = {
        "seeds": ",".join(seeds),  # Scylla API requires comma-separated string
        "cluster_name": self.name,
    }
    self.node.remoter.run(f"echo '{yaml.dump(scylla_config)}' > /etc/scylla/scylla.yaml")
```

**Check for**: Any place where a list, dict, or Python object is passed to a remote command, YAML
file, REST API, or cassandra-stress parameter string.

### Rule 2: Never Return Wrong Defaults on Failure

Returning a neutral default (zero, empty list, False) when an operation fails can make downstream
code conclude "everything is fine" when it should fail loudly.

> Reviewer quote: "If we fail to count them, shouldn't we throw an exception? By returning zero the test might pass"

**Bad** — silent failure allows test to pass incorrectly:

```python
def count_running_nodes(cluster) -> int:
    try:
        return len([n for n in cluster.nodes if n.is_running()])
    except Exception:
        return 0  # WRONG: caller checks `count > 0` and silently proceeds
```

**Good** — raise or re-raise to fail loudly:

```python
def count_running_nodes(cluster) -> int:
    """Count nodes in running state.

    Raises:
        NodeCountError: If node status cannot be determined.
    """
    try:
        return len([n for n in cluster.nodes if n.is_running()])
    except Exception as exc:
        raise NodeCountError(f"Cannot determine node count for {cluster.name}") from exc
```

Or, if a default is truly appropriate, document it explicitly:

```python
def count_running_nodes(cluster, default_on_error: int | None = None) -> int | None:
    """Count nodes in running state.

    Args:
        default_on_error: Value to return if count fails. If None, raises on error.
    """
    try:
        return len([n for n in cluster.nodes if n.is_running()])
    except Exception:
        if default_on_error is not None:
            return default_on_error
        raise
```

### Rule 3: Edge Cases — Empty, Single, Boundary

Check: does the function work correctly when:
- Input is empty (`[]`, `""`, `0`, `None`)?
- Input has exactly one element?
- Input is at a numerical boundary (max, min, zero)?

**Example check for a function that processes node lists**:

```python
# Does this work with an empty cluster?
nodes = []
configure_replication(nodes, replication_factor=3)
# If replication_factor > len(nodes), does it raise or silently produce wrong config?
```

### T13 Review Checklist

```
[ ] List/dict values passed to remote APIs are serialized (comma-joined, JSON-dumped, etc.)
[ ] Functions that can fail do not return neutral defaults that hide the failure
[ ] Edge cases tested: empty input, single element, boundary values
[ ] Return types match what callers expect (int vs str, list vs iterator, etc.)
[ ] No "I'll return 0 if something goes wrong" patterns where 0 is a valid count
```

---

## T6: Error Handling

SCT uses an event system to track failures — errors that don't raise events are invisible in Argus
and the monitoring dashboard. Context managers that don't use `try/finally` can leak resources.

### Rule 1: Raise Error Events for Escaped Failures

When an error occurs in `check_node_health`, a nemesis, or a monitoring thread, it must be visible
in Argus. Use SCT's event system (`CoreDumpEvent`, `NodeNotResponding`, etc.) so the failure
appears in the test timeline.

> Reviewer quote: "if something escaped check_node_health we should raise error event, so it would be visible in Argus"

**Bad** — error logged but not published as event:

```python
def check_node_health(self, node):
    try:
        result = node.remoter.run("nodetool status")
        if "DN" in result.stdout:
            self.log.error("Node %s is DOWN", node.name)
            # WRONG: error is logged but not visible in Argus/monitoring
    except Exception as exc:
        self.log.exception("Health check failed: %s", exc)
```

**Good** — publish a SCT event:

```python
from sdcm.sct_events.health import NodeNotResponding


def check_node_health(self, node):
    try:
        result = node.remoter.run("nodetool status")
        if "DN" in result.stdout:
            NodeNotResponding(node=node, message="nodetool reports node DOWN").publish()
    except Exception as exc:
        NodeNotResponding(node=node, message=f"Health check exception: {exc}").publish()
        raise
```

### Rule 2: `try/finally` Around `yield` in Context Managers

When a function or fixture uses `yield`, cleanup code MUST be in `finally`. Without it, an
exception before `yield` or during the `with` block skips the cleanup.

> Reviewer quote: "does not use try/finally around the yield, so event_stop will not be called"

**Bad** — cleanup is skipped on exception:

```python
@contextmanager
def nemesis_operation(node):
    event = DisruptionEvent(node=node).begin()
    yield node
    event.end()  # NEVER called if an exception occurs inside the `with` block
```

**Good** — `try/finally` guarantees cleanup:

```python
from contextlib import contextmanager


@contextmanager
def nemesis_operation(node):
    event = DisruptionEvent(node=node).begin()
    try:
        yield node
    finally:
        event.end()  # ALWAYS called, even on exception
```

**Same pattern for pytest fixtures**:

```python
@pytest.fixture()
def running_cluster(request):
    cluster = create_test_cluster(request.config)
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.destroy()  # Always destroys, even if test fails
```

### Rule 3: Validate Inputs Before Use

Don't assume callers pass valid inputs. Validate at the function boundary so errors are caught
with clear messages rather than obscure AttributeError/TypeError downstream.

**Bad**:

```python
def set_replication_factor(cluster, rf):
    # If rf is None or a string, this fails with confusing TypeError later
    for keyspace in cluster.keyspaces:
        keyspace.alter(replication_factor=rf)
```

**Good**:

```python
def set_replication_factor(cluster, rf: int) -> None:
    """Set replication factor for all keyspaces.

    Args:
        cluster: Target ScyllaDB cluster.
        rf: Replication factor. Must be a positive integer.

    Raises:
        ValueError: If rf is not a positive integer.
    """
    if not isinstance(rf, int) or rf < 1:
        raise ValueError(f"Replication factor must be a positive integer, got: {rf!r}")
    for keyspace in cluster.keyspaces:
        keyspace.alter(replication_factor=rf)
```

### T6 Review Checklist

```
[ ] Escaped errors in health checks and monitoring publish SCT error events
[ ] All contextmanager/fixture yields wrapped in try/finally
[ ] Event start (.begin()) and stop (.end()) always paired via try/finally
[ ] Function inputs validated with clear error messages before use
[ ] Exception handling does not silently swallow errors that should propagate
```

---

## T2: Missing Documentation

### Rule 1: Non-Empty Config Parameter Descriptions

Every `SctField` in `sct_config.py` MUST have a non-empty `description`. Empty help text makes
`sct.py` `--help` useless and makes it impossible to understand what a parameter does without
reading the source.

> Reviewer quote: "write the descriptions for those, we shouldn't have empty help values"

**Bad**:

```python
# sdcm/sct_config.py
max_parallel_health_checks: int = SctField(
    description="",  # EMPTY — blocked by reviewers
)
```

**Good**:

```python
max_parallel_health_checks: int = SctField(
    description="""
        Number of nodes to health-check simultaneously.

        Default: 5
        Valid range: 1-20 (higher values increase load on the monitoring node)
        Example: max_parallel_health_checks: 10
    """,
)
```

### Rule 2: Magic Numbers Need 'Why' Comments

When a number, threshold, or limit is set to a specific value, the comment must explain WHY that
value is correct — not just restate what the code already says.

> Reviewer quote: "why max is 10? what makes it reasonable? please add clear comment about the why"

**Bad**:

```python
MAX_WORKERS = 10  # max is 10
```

**Bad** (same problem, different form):

```python
# Set to 10 workers
MAX_WORKERS = 10
```

**Good**:

```python
# 10 workers: empirically determined as the point where adding more workers
# increases memory pressure without improving throughput on c5.2xlarge nodes.
# See perf test results in SCT-1234.
MAX_WORKERS = 10
```

Or inline for a parameter:

```python
max_parallel_health_checks: int = SctField(
    description="""
        Number of parallel health check workers.

        Default: 5
        Valid range: 1-10. Above 10, additional workers increase load on the
        monitoring node without reducing total check time (health checks are
        I/O-bound on the target node, not the monitoring node).
    """,
)
```

### Rule 3: Google-Style Docstrings for Non-Obvious Functions

Functions with non-obvious behavior — particularly those that have side effects, complex logic,
or interact with remote systems — need Google-style docstrings.

**When a docstring is required**:
- Public methods in `sdcm/` classes
- Functions with non-obvious return values
- Functions that raise specific exceptions
- Functions with side effects (writes to remote, publishes events)

**Google docstring format**:

```python
def wait_for_nodes_up(self, nodes: list, timeout: int = 300) -> bool:
    """Wait until all specified nodes report UP status.

    Polls nodetool status every 10 seconds until all nodes show UN (Up/Normal)
    or the timeout expires. Publishes a NodeNotResponding event for each node
    that remains down at timeout.

    Args:
        nodes: List of BaseNode instances to wait for.
        timeout: Maximum wait time in seconds. Default: 300.

    Returns:
        True if all nodes are UP before timeout, False otherwise.

    Raises:
        TimeoutError: If the remoter connection itself fails (not just node status).
    """
    ...
```

**When a docstring is NOT needed**:
- Simple property accessors (`@property def name(self): return self._name`)
- Methods where the signature is completely self-documenting
- Private helper functions that are obvious in context

### T2 Review Checklist

```
[ ] All SctField descriptions are non-empty and explain the parameter's purpose
[ ] SctField descriptions include: default value, valid range (if applicable), example
[ ] Magic numbers and limits have comments explaining WHY (not just WHAT)
[ ] Public methods in sdcm/ classes have Google-style docstrings
[ ] Functions with non-obvious returns/raises/side-effects are documented
[ ] No "# set x to 5" style comments — comments explain intent, not mechanics
```

---

## Combined T13 + T6 + T2 Review Checklist

```
T13  [ ] List/dict values serialized correctly for remote APIs (comma-join, JSON, etc.)
T13  [ ] Failures raise exceptions rather than returning neutral defaults
T13  [ ] Edge cases covered: empty, single element, boundary values
T6   [ ] Escaped errors publish SCT error events (NodeNotResponding, CoreDumpEvent, etc.)
T6   [ ] All context manager yields use try/finally to guarantee cleanup
T6   [ ] Inputs validated with clear ValueError messages before use
T2   [ ] SctField descriptions are non-empty with purpose, default, valid range
T2   [ ] Magic numbers have comments explaining WHY the value is correct
T2   [ ] Public sdcm/ methods have Google-style docstrings where non-obvious
```
