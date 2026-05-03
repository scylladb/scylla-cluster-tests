# Common Issues Catalog

Real incidents from SCT development with root cause analysis and prevention guidance. Use this as a reference during code reviews to catch similar patterns.

---

## Issue 1: Method Override Signature Break

**Severity**: CRITICAL
**Incident**: PR #13445 -> PR #14113 (regression + fix)
**Date**: March 2026

### What Happened

PR #13445 added an `ami_id=None` parameter to `AWSCluster._create_instances()` in `cluster_aws.py` and threaded it through the call chain (`_create_on_demand_instances`, `_create_spot_instances`, `_create_or_find_instances`, `add_nodes`). The change was needed to support platform migration (x86 to ARM) where different AMI images are used per node.

However, `MonitorSetEKS._create_instances()` in `sdcm/cluster_k8s/eks.py` — which overrides the parent method solely to apply a `MonitorId` EC2 tag for multi-tenant EKS setups — was not updated to accept or forward the new parameter.

When `_create_or_find_instances()` (inherited from `AWSCluster`) called `self._create_instances(..., ami_id=ami_id)` on a `MonitorSetEKS` instance, Python's method dispatch resolved to the subclass override, which rejected the unknown keyword argument with `TypeError`.

### Why It Wasn't Caught

1. **The diff didn't include `eks.py`** — reviewers only saw changes to `cluster.py`, `cluster_aws.py`, and the new test file
2. **No linter catches this** — Python doesn't statically verify method override signatures
3. **No unit test covers the EKS monitor creation path** — the override exists only for EC2 tagging
4. **Multiple review rounds approved it** — humans and AI both missed the override because it was outside the diff

### The Fix (PR #14113)

```python
# Before (broken):
def _create_instances(self, count, ec2_user_data="", dc_idx=0, az_idx=0, instance_type=None, is_zero_node=False):
    instances = super()._create_instances(
        count=count, ec2_user_data=ec2_user_data, dc_idx=dc_idx,
        az_idx=az_idx, instance_type=instance_type, is_zero_node=is_zero_node,
    )

# After (fixed):
def _create_instances(
    self, count, ec2_user_data="", dc_idx=0, az_idx=0, instance_type=None, is_zero_node=False, ami_id=None
):
    instances = super()._create_instances(
        count=count, ec2_user_data=ec2_user_data, dc_idx=dc_idx,
        az_idx=az_idx, instance_type=instance_type, is_zero_node=is_zero_node, ami_id=ami_id,
    )
```

### Prevention

- **During review**: When ANY method signature changes in a cluster class, grep for `def <method_name>` across ALL of `sdcm/` and `unit_tests/`
- **During implementation**: Use an IDE's "Find Usages" / "Change Signature" refactoring to update all overrides
- **Testing**: Consider adding unit tests that verify override signatures match parent signatures (signature compatibility tests)

### Detection Pattern for Reviewers

If you see a diff like this:
```python
- def some_method(self, a, b, c):
+ def some_method(self, a, b, c, new_param=None):
```

Immediately ask: "Are there subclasses that override `some_method`? Do they accept `new_param`?"

---

## Issue 2: Missing Configuration Defaults

**Severity**: HIGH
**Pattern**: Recurring

### What Happens

A new configuration option is added to `sdcm/sct_config.py` but no default value is added to `defaults/test_default.yaml` or the relevant backend-specific config file. This causes `None` values to propagate where a string or integer is expected, leading to `TypeError` or `AttributeError` at runtime.

### Prevention

- **During review**: For every new field in `sct_config.py`, verify a corresponding entry exists in `defaults/`
- **During implementation**: Always add defaults immediately when creating the config field

---

## Issue 3: Inline Imports Causing Circular Dependencies

**Severity**: MEDIUM
**Pattern**: Recurring

### What Happens

A developer adds an `import` inside a function body to work around a circular import. This works initially but creates fragile import ordering that can break when other modules change. It also violates SCT's explicit convention against inline imports.

### Prevention

- **During review**: Flag any `import` statement inside a function/method body
- **Acceptable exception**: Only when there's a genuine cyclic dependency that can't be resolved by restructuring. Must include a comment explaining why.
- **Better fix**: Restructure the code to break the cycle (move shared types to a `types.py` or `_base.py` module)

---

## Issue 4: unittest.TestCase in Unit Tests

**Severity**: MEDIUM
**Pattern**: Recurring (legacy code)

### What Happens

New tests are written using `unittest.TestCase` with `setUp`/`tearDown` and `self.assertEqual`. This breaks pytest's fixture injection system, `autouse` fixtures (including the `fake_remoter` that blocks network calls), parametrize, and parallel execution with `pytest-xdist`.

### Prevention

- **During review**: Flag any `class TestFoo(unittest.TestCase)` in `unit_tests/`
- **Correct pattern**: Use plain pytest functions/classes with `@pytest.fixture` and plain `assert`

---

## Issue 5: Missing Provision Test Labels

**Severity**: MEDIUM
**Pattern**: Recurring

### What Happens

A PR modifies backend-specific code (e.g., `cluster_aws.py`) but doesn't request the corresponding provision test label (`provision-aws`). The PR merges without running backend-specific integration tests, and a regression is discovered later in CI.

### Prevention

- **During review**: Check which `cluster_*.py` or `provision/` files are in the diff, and verify the matching labels are applied
- **File-to-label mapping**: See [review-checklist.md](review-checklist.md) Check 6

---

## Issue 5: Bare curl in shell_script_cmd() Blocks

**Severity**: MEDIUM
**Incident**: Argus run c607f0ef — `artifacts-rocky9-nonroot-test` build #847

### What Happens

A `curl -fL` call embedded inside a `shell_script_cmd(f"""...""")` multi-line bash string has no `--retry` flags. A transient CDN connection reset (`curl: (56) Recv failure`) fails the entire test during setUp before any Scylla nodes are provisioned. The specific call site was `sct_config.py:get_version_based_on_conf()` downloading the unified relocatable package.

### Why It Was Missed

PR #13509's initial migration grepped for `remoter.run("curl ...")` patterns but missed curl calls embedded inside `shell_script_cmd()` f-strings, which generate bash scripts run via `LOCALRUNNER.run()`.

### Prevention

- **During review**: Search for `curl` inside `shell_script_cmd()` blocks — these won't match simple `remoter.run("curl` patterns
- **Grep pattern**: `grep -rn "curl" sdcm/ --include="*.py" | grep -v curl_with_retry | grep -v "# no-retry"`
- **Fix pattern**: Interpolate `curl_with_retry()` into the f-string: `f"{curl_with_retry(url, output='file', follow_redirects=True)}"`
