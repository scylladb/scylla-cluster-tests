# SCT Code Review Checklist

Detailed per-check guidance for reviewing SCT pull requests. Each check includes trigger conditions, what to look for, and concrete examples.

## Check 1: Override & Inheritance Safety

**Priority**: CRITICAL — This is the most dangerous class of bug in SCT.

### Why This Matters

SCT uses deep, multi-level class hierarchies for cluster management. A method like `_create_instances` is defined in a base class and overridden in 5+ backend-specific subclasses. When a parent method gains a new parameter, Python dispatches calls to the subclass override — which rejects the unknown keyword argument with a `TypeError` at runtime. No linter, type checker, or existing unit test catches this.

### Trigger

Any PR that modifies a `def` line (method signature) in:
- `sdcm/cluster.py`
- `sdcm/cluster_aws.py`
- `sdcm/cluster_cloud.py`
- Any file under `sdcm/cluster_k8s/`
- Any `cluster_*.py` file

### How to Check

1. Identify every method whose signature changed in the diff
2. For each method, run: `grep -rn "def <method_name>" sdcm/ unit_tests/`
3. Compare the override's parameter list against the updated parent
4. Verify `super()` calls forward the new parameter
5. Check test stubs in `unit_tests/dummy_remote.py`, `unit_tests/test_cluster.py`, and `unit_tests/test_scylla_yaml_builders.py`

### High-Risk Methods Reference

| Method | Override Count | Files to Audit |
|---|---|---|
| `add_nodes` | 18+ | `cluster.py`, `cluster_cloud.py`, `cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`, `cluster_oci.py`, `cluster_docker.py`, `cluster_baremetal.py`, `cluster_k8s/__init__.py`, `cluster_k8s/eks.py`, `cluster_k8s/gke.py`, `kafka/kafka_cluster.py`, `unit_tests/test_cluster.py`, `unit_tests/test_scylla_yaml_builders.py`, `unit_tests/dummy_remote.py` |
| `_create_instances` | 5+ | `cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`, `cluster_oci.py`, `cluster_k8s/eks.py` |
| `_create_on_demand_instances` | 2+ | `cluster_aws.py` and subclasses |
| `_create_spot_instances` | 2+ | `cluster_aws.py` and subclasses |
| `wait_for_init` | 5+ | All backend cluster files |
| `destroy` | 5+ | All backend cluster and node files |
| `_create_or_find_instances` | 3+ | `cluster_cloud.py`, `cluster_aws.py` |

### Example: What a Reviewer Should Flag

**Diff shows** (in `cluster_aws.py`):
```python
- def _create_instances(self, count, ec2_user_data="", dc_idx=0, instance_type=None):
+ def _create_instances(self, count, ec2_user_data="", dc_idx=0, instance_type=None, ami_id=None):
```

**Reviewer must ask**: "Does `MonitorSetEKS._create_instances` in `cluster_k8s/eks.py` also accept `ami_id`? What about `cluster_gce.py`, `cluster_azure.py`, `cluster_oci.py`?"

---

## Check 2: Import Conventions

### Trigger

Any new or modified `import` statement in a `.py` file.

### Rules

1. **No inline imports** — All imports at file top. Only exception: cyclic dependency with explanatory comment.
2. **Three groups** separated by blank lines:
   - Group 1: Built-in (`os`, `sys`, `typing`, `pathlib`)
   - Group 2: Third-party (`pytest`, `boto3`, `cassandra`)
   - Group 3: Internal (`sdcm.cluster`, `sdcm.utils.common`)
3. **Alphabetically sorted** within each group
4. **No wildcard imports** (`from module import *`)

### Example

```python
# WRONG - inline import
def do_something():
    from sdcm.utils import common  # BAD: inline import
    return common.get_data()

# RIGHT - top-level import
from sdcm.utils import common

def do_something():
    return common.get_data()
```

---

## Check 3: Error Handling

### Trigger

Any `try/except`, `raise`, exception class definition, or error handling logic.

### Rules

1. **No empty catch blocks**: `except Exception: pass` hides real errors
2. **Use `silence()` context manager** instead of bare try/except where appropriate
3. **Include context in error messages**: Node name, IP, operation being performed
4. **Log before raising** when the exception might be caught upstream
5. **Use SCT event system** (`sct_events`) for errors that need test-level visibility

---

## Check 3b: Thread Pool Lifecycle

### Trigger

Any new or modified `ThreadPoolExecutor(...)`, or a class that holds one on an attribute.

### Rules

1. **Every pool needs a shutdown**: either `with ThreadPoolExecutor(...) as executor:` or an
   explicit `shutdown()` on every path that ends the work. A pool assigned to `self.<attr>` with
   no matching `shutdown(` anywhere in the class is a leak — flag it.
2. **Why it is not cosmetic**: pool workers are non-daemon and outlive their task. At interpreter
   shutdown `concurrent.futures.thread._python_exit()` joins every live worker **with no timeout**,
   so one leaked worker hangs the whole run after the test has finished (SCT-575: ~25h idle).
3. **There is no way to opt out of that join.** Reject `atexit.unregister(_python_exit)` (wrong
   registry — it is registered via `threading._register_atexit`) and reject deleting entries from
   `_threads_queues` (no effect on Python 3.14+, where the join moved into C-level
   `_thread._shutdown()`). The only fix is to not leave the worker alive.
4. **Prefer `threading.Thread(daemon=True)`** for fire-and-forget background work nothing joins —
   daemon threads are never tracked for the shutdown join.
5. **Pool creation belongs in `start()`**, not `__init__`, for any class supporting a
   start/stop/start cycle: a shut-down pool cannot accept new work.

See `correctness-and-safety.md` T6 Rule 4 for the full rationale and a good/bad example.

---

## Check 4: Test Coverage

### Trigger

Any non-trivial code change (new functions, modified logic, bug fixes).

### Rules

1. **New public methods/functions** should have unit tests in `unit_tests/`
2. **Bug fixes** should have a regression test
3. **pytest style only** — no `unittest.TestCase`, no `setUp`/`tearDown`
4. **Use fixtures** via `@pytest.fixture`, not class-level setup
5. **Use parametrize** for testing multiple inputs: `@pytest.mark.parametrize`
6. **Mock at boundaries** — mock cloud APIs, not internal logic

### What to Look For

- PR adds a new method but no corresponding test file or test function
- PR fixes a bug but has no test that would have caught the original bug
- Tests use `self.assertEqual` instead of plain `assert`

---

## Check 5: Configuration Changes

### Trigger

Any change to `sdcm/sct_config.py`, `defaults/*.yaml`, or `test-cases/*.yaml`.

### Rules

1. **New config options MUST have defaults** in `defaults/test_default.yaml` or backend-specific files (`defaults/aws_config.yaml`, etc.)
2. **Type and description required** in the config field definition
3. **Pre-commit auto-updates** `docs/configuration_options.md` — don't edit it manually

---

## Check 6: Backend Impact & Provision Labels

### Trigger

Any change to backend-specific files.

### File -> Label Mapping

| Files Modified | Required Label |
|---|---|
| `sdcm/cluster_aws.py`, `sdcm/provision/aws/*`, `sdcm/utils/aws_utils.py` | `provision-aws` |
| `sdcm/cluster_gce.py`, `sdcm/utils/gce_utils.py` | `provision-gce` |
| `sdcm/cluster_azure.py`, `sdcm/provision/azure/*`, `sdcm/utils/azure_utils.py` | `provision-azure` |
| `sdcm/cluster_docker.py`, `sdcm/utils/docker_utils.py` | `provision-docker` |
| `sdcm/cluster_k8s/*`, `sdcm/utils/k8s/*` | `provision-k8s` |
| `sdcm/cluster_baremetal.py` | `provision-baremetal` |

### Cross-Backend Consistency

When a change is made to one backend, ask: "Do other backends need the same change?" Common cases:
- Network configuration changes often apply to all cloud backends
- Security/credential handling may need parity across backends
- Monitoring integration changes may affect all backends

---

## Check 7: Commit Message Format

### Trigger

Every PR.

### Format

```
type(scope): subject

body (min 30 chars)

[optional reference]
```

### Valid Types

`ci`, `docs`, `feature`, `fix`, `improvement`, `perf`, `refactor`, `revert`, `style`, `test`, `unit-test`, `build`, `chore`

### Constraints

- Scope: minimum 3 characters
- Subject: 10-120 characters, no trailing period
- Header (type + scope + subject): maximum 100 characters
- Body: minimum 30 characters, max 120 chars per line
