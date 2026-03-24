# Nemesis Developer Guide

Nemesis are **chaos engineering operations** that test ScyllaDB cluster resilience by introducing controlled disruptions during test execution. Each nemesis performs a specific disruption — rebooting a node, partitioning the network, filling a disk, modifying a schema — and the test framework verifies that the cluster recovers correctly.

This guide covers the nemesis architecture, how to run and configure nemesis, and how to develop new ones.

## Architecture

The nemesis system lives in `sdcm/nemesis/` and is built around four core components:

### Class Hierarchy

```
NemesisFlags              # Boolean flags for filtering (disruptive, topology_changes, ...)
  └─ NemesisBaseClass     # Abstract base
       └─ StopStartMonkey, DecommissionMonkey, ...   (in monkey/)

NemesisRunner             # Orchestrator — contains execution flow, node selection, metrics
  └─ SisyphusMonkey, CategoricalMonkey, ...          (in monkey/runners.py)
```

**`NemesisBaseClass`** (a.k.a. "Monkey") — Each subclass represents a single disruption type. It carries boolean flags (used for filtering) and implements a `disrupt()` method that runs the nemesis. These classes are what you see in test configs and Argus reports.

**`NemesisRunner`** — The orchestrator that is responsible for execution flow, handles node selection via `NemesisNodeAllocator`, reports metrics. Subclasses like `SisyphusMonkey` control *which* nemesis to run and in *what order*.

**`NemesisFlags`** — A mixin that defines all boolean filter flags.

### Supporting Components

**`NemesisRegistry`** (`registry.py`) — Discovers all `NemesisBaseClass` subclasses at runtime using `__subclasses__()` and filters them by boolean flag expressions (e.g. `"not disruptive"`, `"topology_changes and not limited"`).

**`NemesisNodeAllocator`** (`utils/node_allocator.py`) — Thread-safe singleton that tracks which node is running which nemesis. Prevents conflicting nemesis from targeting the same node simultaneously.

**`NemesisJobGenerator`** (`generator.py`) — Generates Jenkins pipeline files and YAML test configs for each nemesis, used by CI to create per-nemesis test jobs.

**Auto-discovery mechanism** (bottom of `__init__.py`) — Uses `pkgutil.walk_packages()` to recursively import all submodules under `sdcm/nemesis/`, ensuring every `NemesisBaseClass` and `NemesisRunner` subclass is registered. This means you can place a new nemesis file anywhere in the `monkey/` directory and it will be automatically discovered.

### Execution Flow

```
Test YAML config
  nemesis_class_name: 'SisyphusMonkey'
  nemesis_selector: 'not disruptive'
           │
           ▼
ClusterTester sets up NemesisRunner subclass (e.g. SisyphusMonkey)
           │
           ▼
SisyphusMonkey.__init__()
  └─ build_disruptions_by_selector(nemesis_selector)
       └─ NemesisRegistry.filter_subclasses("not disruptive")
            └─ Returns list of NemesisBaseClass subclasses matching flags
       └─ Instantiates each matching subclass → disruptions_list
  └─ shuffle_list_of_disruptions(disruptions_list)
           │
           ▼
BaseScyllaCluster.start_nemesis()
  └─ Starts NemesisRunner.run() in a daemon thread
       └─ call_next_nemesis() in a loop (until termination_event)
            └─ execute_nemesis(nemesis)
                 └─ nemesis.disrupt()  →  self.runner.disrupt_*()
                      └─ NemesisNodeAllocator acquires a target node
                      └─ Disruption logic runs
                      └─ Node is released
            └─ Sleep for nemesis_interval
```

## Module Structure

```
sdcm/nemesis/
├── __init__.py              # NemesisBaseClass, NemesisFlags, NemesisRunner and the auto-discovery mechanism
├── registry.py              # NemesisRegistry — discovery and boolean-flag filtering
├── generator.py             # NemesisJobGenerator — CI pipeline/config generation
├── monkey/
│   ├── __init__.py          # Individual nemesis classes (NemesisBaseClass subclasses)
│   ├── runners.py           # NemesisRunner subclasses (SisyphusMonkey, CategoricalMonkey, etc.)
│   ├── modify_table.py      # ModifyTable* nemesis group (extracted module)
│   └── abort_decommission.py  # AbortDecommissionMonkey (extracted module)
└── utils/
    ├── __init__.py          # NEMESIS_TARGET_POOLS enum, DefaultValue, unique_disruption_name
    ├── node_allocator.py    # NemesisNodeAllocator — thread-safe node locking singleton
    ├── indexes.py           # Index and materialized-view helpers
    └── node_operations.py   # iptables blocking, SIGSTOP/SIGCONT helpers
```

> **Note:** The `__init__.py` file in `sdcm/nemesis/` is still large (~6000 lines) because it contains all `disrupt_*` methods on `NemesisRunner`. These methods are being progressively extracted into dedicated modules under `monkey/` as part of the ongoing nemesis rework (Phase 3).

## How to Run Nemesis

### Configuration Parameters

Nemesis behavior is controlled through test YAML configs or environment variables:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nemesis_class_name` | `str` | `'NoOpMonkey'` | Runner class to use. Supports parallel syntax: `"SisyphusMonkey:2"` runs 2 threads |
| `nemesis_selector` | `str` | `None` | Boolean flag expression to filter nemesis (e.g. `"not disruptive"`) |
| `nemesis_interval` | `int` | `5` | Sleep interval between operations, in **minutes** |
| `nemesis_seed` | `int` | random | Seed for reproducible nemesis sequences |
| `nemesis_filter_seeds` | `bool` | `false` | If true, target only non-seed nodes |
| `nemesis_multiply_factor` | `int` | `2` | Multiply the disruption list before shuffling |
| `nemesis_exclude_disabled` | `bool` | `true` | Filter out nemesis with `disabled = True` |
| `nemesis_during_prepare` | `bool` | `true` | Run nemesis during test prepare stage |
| `nemesis_add_node_cnt` | `int` | `3` | Nodes to add/remove in GrowShrinkCluster |
| `nemesis_sequence_sleep_between_ops` | `int` | `None` | Sleep between operations in sequence tests |

### `nemesis_class_name` Syntax

The `nemesis_class_name` parameter supports several formats:

```yaml
# Single nemesis runner, one thread
nemesis_class_name: 'SisyphusMonkey'

# Single nemesis runner, 2 parallel threads
nemesis_class_name: 'SisyphusMonkey:2'

# Multiple runners with different thread counts
nemesis_class_name: 'SisyphusMonkey:1 NoOpMonkey:1'
```

### `nemesis_selector` Syntax

The `nemesis_selector` uses Python boolean expressions to filter nemesis by their flags:

```yaml
# Only non-disruptive nemesis
nemesis_selector: 'not disruptive'

# Only topology-changing nemesis that are not limited
nemesis_selector: 'topology_changes and not limited'

# A specific nemesis by name
nemesis_selector: 'DecommissionMonkey'

# Combine flag filters with specific names
nemesis_selector: 'DecommissionMonkey or GrowShrinkClusterNemesis'

# Exclude specific nemesis
nemesis_selector: 'not DecommissionMonkey'
```

When `nemesis_exclude_disabled` is `true` (default), `" and not disabled"` is automatically appended to the selector.

On Kubernetes backends, `" and kubernetes"` is automatically appended to ensure only K8s-compatible nemesis run.

### Example Configurations

**Standard longevity test with random disruptions:**
```yaml
nemesis_class_name: "SisyphusMonkey"
nemesis_interval: 5
nemesis_multiply_factor: 2
```

**Run a specific nemesis for debugging:**
```yaml
nemesis_class_name: "SisyphusMonkey"
nemesis_selector: "DecommissionMonkey"
nemesis_interval: 5
```

**Multiple parallel nemesis threads:**
```yaml
nemesis_class_name: "SisyphusMonkey:3"
nemesis_interval: 2
```

**Non-disruptive nemesis only:**
```yaml
nemesis_class_name: "SisyphusMonkey"
nemesis_selector: "not disruptive"
```

**Non-disruptive nemesis only (list selector syntax):**
```yaml
nemesis_class_name: "SisyphusMonkey:2"
nemesis_selector: ["not disruptive", ""]
```

### Running with Docker Backend

The Docker backend supports most nemesis but has limitations. Not all operations (like hard reboot) are available. For the full compatibility matrix, see [docker-backend-nemesis.md](docker-backend-nemesis.md).

```bash
export SCT_SCYLLA_VERSION=2025.3.0
export SCT_USE_MGMT=false
uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend docker \
    --config test-cases/PR-provision-test.yaml
```

To run a specific nemesis with Docker:
```bash
hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker \
    -c configurations/nemesis/longevity-5gb-1h-nemesis.yaml \
    -c configurations/nemesis/AbortRepairMonkey.yaml \
    -c configurations/nemesis/additional_configs/docker_backend.yaml
```

## How to Develop a New Nemesis

### Step 1: Create the class

Create a new class that inherits from `NemesisBaseClass`. Place it in the appropriate file under `sdcm/nemesis/monkey/`:

- For a standalone nemesis, add it to `monkey/__init__.py` or create a new file
- For a group of related nemesis, create a new module (e.g. `monkey/my_group.py`)

The auto-discovery mechanism will find your class automatically regardless of where you place it in the `monkey/` directory.

```python
from sdcm.nemesis import NemesisBaseClass


class MyNewMonkey(NemesisBaseClass):
    # Set flags that describe this nemesis
    disruptive = True
    topology_changes = False

    def disrupt(self):
        ...<disruption_code>
```


### Step 2: Implement the disruption logic

If your nemesis reuses existing logic, just call the appropriate runner method.
You can reuse method from NemesisRunner, but it is discouraged and you should make the nemesis self-contained

```python
def disrupt(self):
    self.runner.log.info("Running my new operation on %s", self.runner.target_node)
    # Your disruption logic here
    self.runner.target_node.restart_scylla()
```


> **Future direction:** As part of Phase 3 of the nemesis rework, disruption logic is being progressively moved from `NemesisRunner` into the nemesis classes themselves or into dedicated modules. When writing new nemesis, consider placing the logic directly in your nemesis class if it is self-contained.

### Step 3: Configure target node pool (optional)

By default, nemesis target data nodes. Use decorators to change this:

```python
from sdcm.nemesis import NemesisBaseClass, target_all_nodes, target_data_nodes

@target_all_nodes  # Target any node (data + zero-token)
class MyAllNodesMonkey(NemesisBaseClass):
    def disrupt(self):
        ...

@target_data_nodes  # Explicitly target only data nodes (default)
class MyDataNodeMonkey(NemesisBaseClass):
    def disrupt(self):
        ...
```

### Step 4: Add CI configuration (optional)

If your nemesis requires additional YAML config files or Jenkins parameters:

```python
class MyNewMonkey(NemesisBaseClass):
    disruptive = True

    # Extra config files loaded when running this nemesis in CI
    additional_configs = ["configurations/my-feature.yaml"]

    # Extra parameters for Jenkins pipeline generation
    additional_params = {"my_param": "value"}

    def disrupt(self):
        ...
```

These are used by `NemesisJobGenerator` to create per-nemesis Jenkins jobs and test configs.

### Step 5: Test locally

Run your nemesis locally using the Docker backend:

```yaml
# In test-cases/PR-provision-test-docker.yaml or similar:
test_duration: 60
stress_cmd: "cassandra-stress write cl=QUORUM duration=5m ..."
n_db_nodes: 4
nemesis_class_name: 'SisyphusMonkey'
nemesis_selector: 'MyNewMonkey'
nemesis_interval: 5
```

```bash
uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend docker \
    --config test-cases/PR-provision-test-docker.yaml
```

## How to Develop a New Runner

Runners are `NemesisRunner` subclasses that control *which* nemesis run and *how* they are selected. Place new runners in `sdcm/nemesis/monkey/runners.py`.

### Using `build_disruptions_by_selector`

Filter nemesis by flag expressions:

```python
from sdcm.nemesis import NemesisRunner


class MyCustomRunner(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Select all non-disruptive nemesis
        self.disruptions_list = self.build_disruptions_by_selector("not disruptive")
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)
```

### Using `build_disruptions_by_name`

Select specific nemesis by class name:

```python
class MyFixedSetRunner(NemesisRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name([
            "StopStartMonkey",
            "DecommissionMonkey",
            "MajorCompactionMonkey",
        ])
        self.disruptions_list = self.shuffle_list_of_disruptions(self.disruptions_list)
```

### Overriding `call_next_nemesis`

For custom selection logic (e.g. weighted random), override `call_next_nemesis()`:

```python
class MyWeightedRunner(NemesisRunner):
    def call_next_nemesis(self):
        # Custom selection logic
        nemesis = self.select_weighted_nemesis()
        self.execute_nemesis(nemesis)
```

See `CategoricalMonkey` in `monkey/runners.py` for a complete example of weighted random selection.

### Available Runners

| Runner | Description |
|--------|-------------|
| `SisyphusMonkey` | Standard runner — filters by `nemesis_selector`, shuffles, cycles through all |
| `CategoricalMonkey` | Weighted random selection from a distribution |
| `NoOpMonkey` | Does nothing (sleeps) — useful for control experiments |
| `ScyllaCloudLimitedChaosMonkey` | Limited set for Scylla Cloud (no AWS API access) |
| `K8sSetMonkey` | Kubernetes-specific disruptions |
| `ScyllaOperatorBasicOperationsMonkey` | Scylla Operator focused operations |
| `MdcChaosMonkey` | Multi-datacenter chaos operations |
| `ManagerRcloneBackup` / `ManagerNativeBackup` | Manager backup operations |
| `EnableDisableTableEncryptionAwsKmsProviderMonkey` | AWS KMS encryption operations |

## Nemesis Flags Reference

All flags are defined in the `NemesisFlags` class (`sdcm/nemesis/__init__.py`). Each flag defaults to `False` (except `supports_high_disk_utilization` which defaults to `True`).

| Flag | Description |
|------|-------------|
| `topology_changes` | Nemesis changes cluster topology (adding/removing nodes or data centers) |
| `disruptive` | Nemesis disrupts a node or cluster (reboot, kill, hard reboot, terminate) |
| `supports_high_disk_utilization` | Nemesis can run safely in 90% disk utilization scenarios (default: `True`) |
| `networking` | Nemesis interacts with network interfaces (block, delay, partition) |
| `kubernetes` | Nemesis is compatible with Kubernetes cluster backends |
| `xcloud` | Nemesis can run with Scylla Cloud (xcloud) backend |
| `limited` | Nemesis belongs to a limited/restricted set |
| `schema_changes` | Nemesis modifies database schema (add/drop columns, create indexes) |
| `config_changes` | Nemesis changes Scylla configuration (scylla.yaml modifications) |
| `free_tier_set` | Nemesis should be included in the FreeTier nemesis set |
| `manager_operation` | Nemesis uses Scylla Manager for its operation |
| `delete_rows` | Nemesis deletes partitions or rows, generating tombstones |
| `zero_node_changes` | Nemesis targets zero-token nodes |
| `sla` | Nemesis is designed for SLA (Service Level Agreement) tests |
| `enospc` | Nemesis causes a node to run out of disk space |
| `modify_table` | Nemesis modifies table properties (compression, compaction, gc_grace, etc.) |

## Further Reading

- [Docker Backend Nemesis Compatibility](docker-backend-nemesis.md) — Which nemesis work on Docker backend
- [Docker Backend Overview](docker-backend-overview.md) — Docker backend specifics and limitations
- [Longevity Test Flow](longevity.md) — How nemesis integrate into longevity tests
- [Skip Test Stages](skip-test-stages.md) — How to skip the nemesis stage
- [Configuration Options](configuration_options.md) — Full list of SCT configuration parameters
