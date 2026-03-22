# Scope and Code Structure (T5 + T4)

Data-backed from 317 T5 (scope hygiene) + 36 T4 (code structure) review comments across 11,312 SCT PRs.

## T5: Scope Hygiene

### The Core Rule

Every change in a PR must be traceable to the PR's stated goal. Reviewers ask: "Is this change
required for the feature/fix this PR claims to implement?" If the answer is no, the change must
be removed and submitted as a separate PR.

> Reviewer quote: "This far out of scope for this PR, needs to be removed and put into its own PR"
> Reviewer quote: "how taking this comment out, is related to this PR?"
> Reviewer quote: "raise a task/subtask in jira"

**Why this matters**: Bundled changes:
- Hide the actual intent of the PR, making review harder
- Create merge conflicts when the unrelated part lands on a different timeline
- Make it harder to revert if the core change causes problems
- Inflate the PR diff, causing reviewer fatigue and missed issues

### What Counts as Out-of-Scope

| Change Type | In-Scope? | Action |
|-------------|-----------|--------|
| Fixing a bug introduced by this PR | YES | Keep it |
| Refactoring code touched by this PR | BORDERLINE | Ask reviewers; often should be separate |
| Fixing a pre-existing typo in a comment | NO | Separate PR or omit |
| Adding unrelated logging | NO | Separate PR |
| Cleaning up imports in an unrelated file | NO | Separate PR |
| Removing an unrelated TODO comment | NO | Separate PR |
| "While I was here" style improvements | NO | Separate PR |

### How to Identify Scope Violations

For each changed file, ask:
1. Would the PR's stated goal break if this file were not modified?
2. Does this change fix a problem introduced by this PR, or a pre-existing problem?
3. Is this change in a file that's logically connected to the PR's domain?

**Example: PR titled "Add support for IPv6 seed addresses"**

```diff
# IN SCOPE — directly related to IPv6 seed handling
- sdcm/utils/network_utils.py  # add IPv6 address parsing
- sdcm/sct_config.py           # add ipv6_seeds config parameter
- defaults/test_default.yaml   # add default for ipv6_seeds

# OUT OF SCOPE — reviewers will flag these
- sdcm/cluster.py              # unrelated cleanup of unrelated method
- sdcm/nemesis.py              # formatting fix in a nemesis method
- unit_tests/test_config.py    # fixing an unrelated broken test
```

### Checklist for Scope Review

```
[ ] Each modified file is directly required by the PR's stated goal
[ ] No "while I was here" cleanup changes bundled in
[ ] No formatting-only changes in files unrelated to the PR
[ ] No pre-existing bug fixes bundled with the feature
[ ] git diff shows only files that the PR description explains
```

---

## T4: Code Structure

Structural issues in SCT code that reviewers consistently flag. These are not style preferences —
they create real maintenance and correctness problems.

### Rule 1: No Inner (Nested) Functions

Inner functions are hard to test, hide complexity, and prevent reuse.

> Reviewer quote: "nit: no need for this to be inner method"

**Bad**:

```python
def setup_monitoring(config: dict) -> MonitoringStack:
    def _build_prometheus_config(targets):
        # 20 lines of logic buried in inner function
        return PrometheusConfig(targets=targets, ...)

    targets = extract_targets(config)
    prom_config = _build_prometheus_config(targets)
    return MonitoringStack(prometheus=prom_config)
```

**Good** — extract to module-level function (or method if it needs `self`):

```python
def _build_prometheus_config(targets: list[str]) -> PrometheusConfig:
    """Build Prometheus configuration from a list of target addresses."""
    return PrometheusConfig(targets=targets, ...)


def setup_monitoring(config: dict) -> MonitoringStack:
    targets = extract_targets(config)
    prom_config = _build_prometheus_config(targets)
    return MonitoringStack(prometheus=prom_config)
```

**Why**: Module-level functions can be imported and tested in isolation. Inner functions cannot.

**Exception**: Inner functions are acceptable for closures that genuinely capture local state and
would not make sense without it (e.g., lambda callbacks that capture a loop variable).

### Rule 2: No Mutable Default Arguments

Python evaluates default arguments once at function definition time, not at call time. Mutable
defaults are shared across all calls that use the default — a classic source of SCT bugs.

> Reviewer quote: "don't initialize an argument with a mutable list"

**Bad**:

```python
def collect_nodes(cluster, extra_nodes=[]):
    # extra_nodes is shared across all calls!
    extra_nodes.append(cluster.master_node)
    return extra_nodes
```

**Good**:

```python
def collect_nodes(cluster, extra_nodes=None):
    if extra_nodes is None:
        extra_nodes = []
    extra_nodes.append(cluster.master_node)
    return extra_nodes
```

**Also bad** (mutable dict default):

```python
def build_config(overrides={}):
    overrides["backend"] = "aws"  # mutates the shared default!
    return overrides
```

**Good**:

```python
def build_config(overrides=None):
    if overrides is None:
        overrides = {}
    overrides["backend"] = "aws"
    return overrides
```

### Rule 3: Centralize Configuration Defaults

Magic values scattered across the codebase make it impossible to tune behavior consistently.
Default values belong in `defaults/` YAML files or as named constants, not inline.

> Reviewer quote: "better to centralize the defaults somewhere so it is easier to change them"

**Bad** (magic numbers scattered in code):

```python
def check_cluster_health(nodes=None, timeout=300, max_retries=5):
    ...

def wait_for_repair(node, timeout=300):  # same 300 but no connection to the other
    ...
```

**Good** — centralize in `defaults/test_default.yaml`:

```yaml
# defaults/test_default.yaml
health_check_timeout: 300
health_check_max_retries: 5
```

Or for code constants, use a named constant:

```python
# sdcm/cluster.py
HEALTH_CHECK_TIMEOUT_SECONDS = 300
HEALTH_CHECK_MAX_RETRIES = 5


def check_cluster_health(nodes=None, timeout=HEALTH_CHECK_TIMEOUT_SECONDS,
                         max_retries=HEALTH_CHECK_MAX_RETRIES):
    ...


def wait_for_repair(node, timeout=HEALTH_CHECK_TIMEOUT_SECONDS):
    ...
```

**For SCT config parameters**: Every new `SctField` in `sct_config.py` MUST have a corresponding
default in `defaults/test_default.yaml` or a backend-specific defaults file. Without it, the
parameter evaluates to `None` when not explicitly set, which breaks code that assumes a value.

```python
# sdcm/sct_config.py — add the field
max_parallel_repairs: int = SctField(
    description="Maximum number of parallel repair operations. Default: 3.",
)

# defaults/test_default.yaml — REQUIRED companion entry
max_parallel_repairs: 3
```

### T4 Review Checklist

```
[ ] No nested (inner) function definitions — extract to module or class level
[ ] No mutable default arguments ([], {}, or mutable objects as defaults)
[ ] New sct_config.py parameters have defaults in defaults/ YAML files
[ ] Magic numbers and timeouts are named constants, not inline literals
[ ] No duplicate constant definitions across files (centralize instead)
```

## Combined T5 + T4 Review Checklist

```
Scope Hygiene (T5)
[ ] Every modified file is traceable to the PR's stated goal
[ ] No "while I was here" cleanups bundled with the feature
[ ] No pre-existing bug fixes mixed with new functionality
[ ] PR description matches what the diff actually contains

Code Structure (T4)
[ ] No inner (nested) functions — testability requires module-level functions
[ ] No mutable default arguments (use None + guard instead)
[ ] New config parameters have defaults in defaults/ files
[ ] Constants are named and centralized, not scattered as literals
```
