---
name: writing-nemesis
description: >-
  Guides writing new nemesis (chaos engineering disruptions) for the SCT
  framework. Use when creating a new NemesisBaseClass subclass, adding
  disruption logic, setting nemesis flags, or configuring target node pools.
  Covers the sdcm/nemesis/ package structure, auto-discovery, flag filtering,
  CI configuration, and unit testing patterns.
---

# Writing Nemesis

Create new chaos engineering disruptions (nemesis) for the SCT framework.

For the full developer guide including architecture details, configuration reference, and execution flow, see [docs/nemesis.md](../../docs/nemesis.md).

## Essential Principles

### Self-Contained Disruption Logic

**New nemesis must implement disruption logic directly in `disrupt()`, not delegate to `NemesisRunner.disrupt_*()` methods.**

The runner's `disrupt_*()` methods are legacy code (~6000 lines) being progressively extracted into dedicated nemesis classes (Phase 3 of the nemesis rework). Writing new nemesis that call `self.runner.disrupt_*()` adds to the legacy debt instead of reducing it. Place your logic in `disrupt()` so the nemesis is self-contained and testable in isolation.

### Auto-Discovery Requires Correct Placement and Inheritance

**Classes must be under `sdcm/nemesis/monkey/` and inherit from `NemesisBaseClass` to be discovered.**

The auto-discovery mechanism at the bottom of `sdcm/nemesis/__init__.py` uses `pkgutil.walk_packages()` to recursively import all submodules under `sdcm/nemesis/`. It then finds subclasses via `__subclasses__()`. If you place your file outside `monkey/` or forget the base class, the nemesis will silently not exist — no error, just missing from the registry.

### Flags Drive Filtering

**Boolean flags on your class determine which `nemesis_selector` expressions match it.** Incorrect flags cause your nemesis to run in the wrong tests (or not run at all). A nemesis that reboots nodes but lacks `disruptive = True` will be included in `"not disruptive"` selectors and disrupt non-disruptive test suites. Review the flags reference below and set them accurately.

### Write Unit Tests

**Every new nemesis must have unit tests in `unit_tests/nemesis/`.** Unit tests catch flag misconfigurations, logic errors, and regressions in seconds — far faster than any integration or Docker test. The existing test suite at `unit_tests/nemesis/` provides patterns for testing registry filtering, disruption logic, node allocation, and lifecycle events. Follow those patterns to keep the test suite consistent.

## When to Use

- Creating a new `NemesisBaseClass` subclass (a new disruption type)
- Implementing disruption logic for a nemesis
- Deciding which boolean flags to set on a nemesis
- Configuring target node pools (`@target_all_nodes`, `@target_data_nodes`)
- Adding CI configuration (`additional_configs`, `additional_params`) for a nemesis
- Understanding the nemesis package structure for development purposes

## When NOT to Use

- Configuring existing nemesis in test YAML files — see the "How to Run Nemesis" section in [docs/nemesis.md](../../docs/nemesis.md)
- Debugging why a nemesis failed during a test run — use standard debugging or the profiling-sct-code skill
- Writing unit tests for nemesis code — use the writing-unit-tests skill for general pytest guidance; this skill covers nemesis-specific test patterns
- Fixing backport conflicts in nemesis files — use the fix-backport-conflicts skill
- Exploring which nemesis are available — use `NemesisRegistry` or read `sdcm/nemesis/monkey/__init__.py`

## Architecture Quick Reference

```
NemesisFlags              # Boolean flags mixin (disruptive, topology_changes, ...)
  +-- NemesisBaseClass    # Abstract base for individual disruptions
       +-- YourNewMonkey  # Your nemesis (in monkey/)
```

**Key files:**

| File | Contains |
|------|----------|
| `sdcm/nemesis/__init__.py` | `NemesisBaseClass`, `NemesisFlags`, `NemesisRunner`, auto-discovery |
| `sdcm/nemesis/registry.py` | `NemesisRegistry` — subclass discovery and flag filtering |
| `sdcm/nemesis/monkey/__init__.py` | Individual nemesis classes |
| `sdcm/nemesis/monkey/runners.py` | Runner subclasses (`SisyphusMonkey`, etc.) |
| `sdcm/nemesis/utils/node_allocator.py` | Thread-safe node locking |

## How to Write a New Nemesis

### Step 1: Create the class with flags

Create a file under `sdcm/nemesis/monkey/` or add to an existing file. Set boolean flags that accurately describe your nemesis behavior:

```python
from sdcm.nemesis import NemesisBaseClass


class DiskCorruptionMonkey(NemesisBaseClass):
    disruptive = True        # This disrupts the node
    networking = False       # No network operations
    kubernetes = False       # Not K8s-compatible

    def disrupt(self):
        ...
```

If your nemesis is part of a logical group, create a new module (e.g., `monkey/disk_operations.py`). The auto-discovery will find it.

### Step 2: Implement disruption logic in disrupt()

Write self-contained logic. Access the target node via `self.runner.target_node`:

```python
def disrupt(self):
    self.runner.log.info("Corrupting data files on %s", self.runner.target_node)
    # Direct node operations — self-contained, no runner delegation
    self.runner.target_node.remoter.run(
        "find /var/lib/scylla/data -name '*-Data.db' | head -1 | xargs dd if=/dev/urandom bs=1 count=1024 seek=100"
    )
    self.runner.target_node.restart_scylla()
    self.runner.target_node.wait_db_up()
```

Do NOT write: `self.runner.disrupt_corrupt_disk()` — that adds to legacy debt.

### Step 3: Configure target node pool (optional)

By default, nemesis target data nodes. Use decorators to change this:

```python
from sdcm.nemesis import NemesisBaseClass, target_all_nodes

@target_all_nodes  # Target any node (data + zero-token)
class MyAllNodesMonkey(NemesisBaseClass):
    def disrupt(self):
        ...
```

### Step 4: Add CI configuration (optional)

If your nemesis needs extra YAML configs or Jenkins parameters for CI jobs:

```python
class DiskCorruptionMonkey(NemesisBaseClass):
    disruptive = True

    additional_configs = ["configurations/disk-corruption.yaml"]
    additional_params = {"disk_size": "100GB"}

    def disrupt(self):
        ...
```

These are consumed by `NemesisJobGenerator` to create per-nemesis Jenkins pipelines.

### Step 5: Write unit tests

Add tests in `unit_tests/nemesis/`. The existing test infrastructure provides reusable fakes and patterns:

**Test infrastructure** (`unit_tests/nemesis/__init__.py`, `fake_cluster.py`):
- `TestRunner` — lightweight mock runner with `MagicMock` attributes (`target_node`, `cluster`, `session`, etc.)
- `FakeTester`, `Cluster`, `Node` — dataclass-based fakes providing the minimal interface for `NemesisRunner.__init__`
- `TestBaseClass` — parallel ABC with custom flags for testing registry filtering in isolation

**Testing disruption logic** — instantiate your nemesis with `TestRunner`, call `disrupt()`, assert on side effects:

```python
import pytest

from sdcm.nemesis.monkey import DiskCorruptionMonkey
from unit_tests.nemesis import TestRunner


@pytest.fixture
def runner():
    return TestRunner()


@pytest.fixture
def nemesis(runner):
    return DiskCorruptionMonkey(runner)


def test_disrupt_restarts_scylla(nemesis):
    nemesis.disrupt()
    nemesis.runner.target_node.restart_scylla.assert_called_once()
    nemesis.runner.target_node.wait_db_up.assert_called_once()
```

**Testing CQL generation** — capture executed statements via `TestRunner.executed`:

```python
def test_nemesis_executes_expected_cql(nemesis):
    nemesis.disrupt()
    assert any("ALTER TABLE" in stmt for stmt in nemesis.runner.executed)
```

Run tests:

```bash
uv run sct.py unit-tests -t unit_tests/nemesis/
```

## Nemesis Flags Reference

All flags are on `NemesisFlags` in `sdcm/nemesis/__init__.py`. Each defaults to `False` unless noted.

| Flag | Set `True` when... |
|------|--------------------|
| `disruptive` | Nemesis disrupts a node (reboot, kill, terminate) |
| `topology_changes` | Nemesis adds/removes nodes or data centers |
| `networking` | Nemesis touches network interfaces (block, delay, partition) |
| `schema_changes` | Nemesis modifies database schema |
| `config_changes` | Nemesis changes scylla.yaml configuration |
| `kubernetes` | Nemesis works on K8s backends |
| `xcloud` | Nemesis works with Scylla Cloud |
| `limited` | Nemesis belongs to a restricted set |
| `free_tier_set` | Include in FreeTier nemesis set |
| `manager_operation` | Nemesis uses Scylla Manager |
| `delete_rows` | Nemesis deletes rows/partitions (creates tombstones) |
| `zero_node_changes` | Nemesis targets zero-token nodes |
| `sla` | Nemesis is for SLA tests |
| `enospc` | Nemesis causes disk-full (ENOSPC) |
| `modify_table` | Nemesis modifies table properties |
| `supports_high_disk_utilization` | Nemesis is safe at 90% disk usage (default: `True`) |
| `disabled` | Nemesis is disabled and excluded by default |

## Decision Guide

| Question | Answer |
|----------|--------|
| Where do I put my file? | `sdcm/nemesis/monkey/` — anywhere in this directory tree |
| New file or existing? | New file for standalone/group; `monkey/__init__.py` for simple additions |
| Should I call `self.runner.disrupt_*()`? | No — write self-contained logic in `disrupt()` |
| How do I test it? | Unit tests in `unit_tests/nemesis/` using `TestRunner` |
| How does CI find it? | Auto-discovery via `pkgutil.walk_packages()` — no registration needed |

## Success Criteria

- [ ] Class inherits from `NemesisBaseClass`
- [ ] File is under `sdcm/nemesis/monkey/`
- [ ] Boolean flags accurately describe the nemesis behavior
- [ ] Disruption logic is self-contained in `disrupt()` — no `self.runner.disrupt_*()` delegation
- [ ] Unit tests in `unit_tests/nemesis/` covering disruption logic
- [ ] No inline imports (SCT convention)
- [ ] Google-style docstring on the class
- [ ] CI config added if nemesis needs extra YAML or Jenkins params
