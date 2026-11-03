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
  nemesis:
    runners:
      - SisyphusMonkey:
          selector: 'not disruptive'
           │
           ▼
config.nemesis.resolve_runners()  →  (runner class, resolved config) per entry
           │
           ▼
ClusterTester sets up one NemesisRunner subclass per entry (e.g. SisyphusMonkey)
           │
           ▼
SisyphusMonkey.__init__(..., config=<resolved config>)
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
            └─ Sleep for `interval`
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

Nemesis behavior is controlled by the `nemesis` section of the test YAML config:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `nemesis.runners` | `list` | `[{NoOpMonkey: {}}]` | Runners to start, one thread per entry. See [`nemesis.runners`](#nemesisrunners) |
| `nemesis.selector` | `str` | `None` (all nemesis) | Default runner `selector`: boolean flag expression filtering nemesis (e.g. `"not disruptive"`) |
| `nemesis.interval` | `int` | `5` | Default runner `interval`: sleep between operations, in **minutes** |
| `nemesis.seed` | `int` | random per runner | Default runner `seed` for reproducible nemesis sequences |
| `nemesis.filter_seeds` | `bool` | `false` | Default runner `filter_seeds`: target only non-seed nodes |
| `nemesis.multiply_factor` | `int` | `2` | Default runner `multiply_factor`: repeat the disruption list before shuffling |
| `nemesis.during_prepare` | `bool` | `true` | Run nemesis during test prepare stage |
| `nemesis.sequence_sleep_between_ops` | `int` | `None` | Minutes to sleep between operations of sequence nemesis |
| `nemesis.sla` | `bool` | `None` | Run the SLA nemesis; set only in SLA tests |
| `nemesis.grow_shrink.add_node_cnt` | `int` | `3` | Nodes to add/remove in GrowShrinkCluster |
| `nemesis.grow_shrink.instance_type` | `str` | DB instance type | Instance type of the added nodes |
| `nemesis.grow_shrink.double_load_duration` | `int` | `0` | Minutes to double the load between growing and shrinking; `0` disables it |

The `selector`, `interval`, `seed`, `filter_seeds` and `multiply_factor` of the section are defaults: a runner entry that sets its own value uses that instead. A test case with one runner sets these options in the runner's entry. Section-level defaults serve configs with several runners (values the runners share) and overlay files stacked after a test case. An overlay's default reaches only runners that leave the option unset, and an overlay that sets `runners` replaces the whole list, so a test case stacked with a nemesis overlay keeps all of these options as section defaults.

Config layers (defaults, config files, environment) merge the `nemesis` section field by field, so a file setting `nemesis: {interval: 10}` keeps every other default. `runners` is a list: a later layer that sets it replaces the whole list, or extends it with `["++", {SisyphusMonkey: {}}]`.

From the environment, set a scalar field with `SCT_NEMESIS__<FIELD>` (e.g. `SCT_NEMESIS__INTERVAL=10`). `SCT_NEMESIS`, `SCT_NEMESIS__RUNNERS` and `SCT_NEMESIS__GROW_SHRINK` take a YAML or JSON string:

```bash
export SCT_NEMESIS__RUNNERS='[{SisyphusMonkey: {selector: "not disruptive"}}]'
```

### `nemesis.runners`

Each entry is a single-key mapping `{<RunnerClass>: {<options>}}` and starts one runner thread. Example values of `nemesis.runners`:

```yaml
# Single nemesis runner, one thread
runners:
  - SisyphusMonkey: {}

# Single nemesis runner, 2 parallel threads
runners:
  - SisyphusMonkey: {}
  - SisyphusMonkey: {}

# Multiple different runners
runners:
  - SisyphusMonkey: {}
  - NoOpMonkey: {}
```

Every runner takes `interval`, `seed` and `filter_seeds`. Runners that select by flags (e.g. `SisyphusMonkey`, `CategoricalMonkey`) also take `selector` and `multiply_factor`. Options a runner does not declare are rejected at config load. A runner with no `seed` of its own or in the section gets a random one.

### Per-runner Overrides

Options set on a runner entry override the section defaults for that runner only. Here the first runner uses the section `interval` of 10 minutes and the second its own 5, and each filters its own nemesis:

```yaml
nemesis:
  interval: 10
  during_prepare: false
  runners:
    - SisyphusMonkey:
        selector: "topology_changes"
        seed: 253
    - SisyphusMonkey:
        selector: "schema_changes and not disruptive"
        interval: 5
```

Some runners take options of their own:

```yaml
nemesis:
  runners:
    - CategoricalMonkey:
        dist: {StopStartMonkey: 2, DecommissionMonkey: 1}
        default_weight: 0
    - FixedOrderMonkey:
        order: [DecommissionMonkey, RepairMonkey]
```

### `selector` Syntax

`selector` (in the section or on a runner entry) uses Python boolean expressions to filter nemesis by their flags. Example values:

```yaml
# Only non-disruptive nemesis
selector: 'not disruptive'

# Only topology-changing nemesis that are not limited
selector: 'topology_changes and not limited'

# A specific nemesis by name
selector: 'DecommissionMonkey'

# Combine flag filters with specific names
selector: 'DecommissionMonkey or GrowShrinkClusterNemesis'

# Exclude specific nemesis
selector: 'not DecommissionMonkey'
```

On Kubernetes backends, `" and kubernetes"` is automatically appended to ensure only K8s-compatible nemesis run.

### Turning Off Nemesis That Break the Test's Goal

Some tests check one specific setting. For example, a TWCS test needs the table to keep TWCS compaction and its TTL for the whole run. Some nemesis change exactly these settings. The `ModifyTable*` nemesis pick a random table and change one property on it (compaction, compression, `default_time_to_live`, `gc_grace_seconds`, ...). They never change it back. If such a nemesis runs in a TWCS test, the test still passes, but it no longer tests TWCS.

Nemesis do not know what the test is about. So, when you write a test config, ask: "Does my test depend on a setting that a nemesis can change?" Check the settings your test sets in `pre_create_keyspace`, `post_prepare_cql_cmds`, cassandra-stress `-schema`, user profiles, or in the test class. If the answer is yes, turn off that nemesis by name:

```yaml
# TWCS test: keep compaction strategy and TTL unchanged
nemesis:
  selector: 'not ModifyTableCompactionMonkey and not ModifyTableDefaultTimeToLiveMonkey and not ModifyTableTwcsWindowSizeMonkey'
```

Turn off only the nemesis that your test really depends on. Let the others run. When you stack config files, the last file that sets `selector` wins: the files are merged together before `++` appends apply, so `"++ and not X"` appends only to the defaults, not to an earlier file. If an overlay sets its own `selector`, it must repeat the exclusion. A runner entry with its own `selector` ignores the section `selector`, so put the exclusion on that entry too.

### Example Configurations

**Standard longevity test with random disruptions:**
```yaml
nemesis:
  runners:
    - SisyphusMonkey:
        interval: 5
        multiply_factor: 2
```

**Run a specific nemesis for debugging:**
```yaml
nemesis:
  runners:
    - SisyphusMonkey:
        selector: "DecommissionMonkey"
        interval: 5
```

**Multiple parallel nemesis threads:**
```yaml
nemesis:
  interval: 2
  runners:
    - SisyphusMonkey: {}
    - SisyphusMonkey: {}
    - SisyphusMonkey: {}
```

**Non-disruptive nemesis only:**
```yaml
nemesis:
  runners:
    - SisyphusMonkey:
        selector: "not disruptive"
```

**One non-disruptive thread and one unfiltered thread:**
```yaml
nemesis:
  runners:
    - SisyphusMonkey:
        selector: "not disruptive"
    - SisyphusMonkey: {}
```

### `FixedOrderMonkey` order

`FixedOrderMonkey` takes `order`, an ordered list of nemesis class names. It runs them in that order and repeats the sequence once exhausted. A name may repeat to run that nemesis more than once per cycle. It takes no `selector` or `multiply_factor`. Names are validated against the registered nemesis classes at config load; entries the backend cannot run are reported as SKIPPED.

```yaml
nemesis:
  runners:
    - FixedOrderMonkey:
        order: [DecommissionMonkey, RepairMonkey, DecommissionMonkey]
```

Each entry takes its own list, so several entries give parallel threads different sequences:

```yaml
nemesis:
  runners:
    - FixedOrderMonkey:
        order: [DecommissionMonkey, RepairMonkey]
    - FixedOrderMonkey:
        order: [TruncateMonkey]
```

### `CategoricalMonkey` distribution

`CategoricalMonkey` picks each next nemesis at random by weight. `dist` maps nemesis class names to weights; `default_weight` (default `1`) is the weight of every other nemesis matching `selector`. Names in `dist` are validated against the registered nemesis classes at config load.

```yaml
nemesis:
  runners:
    - CategoricalMonkey:
        dist: {StopStartMonkey: 3, HardRebootNodeMonkey: 1}
        default_weight: 0   # run only the nemesis listed in dist
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


### Step 2: Add a `precheck()` for static skip conditions (optional)

If your nemesis has conditions that make it permanently infeasible on certain
backends, configs, or Scylla versions, implement `precheck()` instead of
raising `UnsupportedNemesis` inside `disrupt()`.

`precheck(node)` is called **once** before the execution loop starts, so a
nemesis excluded here costs zero health-check, node-selection, or Argus overhead
on every subsequent cycle. The `node` argument is a representative live node for
static version, feature-flag, and cluster-uniform attribute checks.

```python
class MyTabletNemesis(NemesisBaseClass):
    def precheck(self, node) -> str | None:
        # Static config / backend condition — known before the test starts
        if self.runner.cluster.params.get("cluster_backend") == "docker":
            return "MyTabletNemesis requires a cloud backend"

        # Version / feature flag — uniform across the cluster, checked via a representative node
        if not node.is_tablets_feature_enabled():
            return "tablets feature not enabled on this cluster"

        return None  # runnable — keep in the rotation

    def disrupt(self):
        ...
```

#### What belongs in `precheck()` vs `disrupt()`

| Condition type | Where to check |
|---|---|
| Test config / backend / product edition | `precheck()` |
| Scylla version / feature flags / cluster-uniform node attribute (e.g. OS distro) | `precheck()` |
| Dynamic cluster state (data presence, live node counts, target-node busy state) | **`disrupt()` only** |

> **Representative node rule:** `precheck(node)` receives a representative node
> for any cluster-wide probe (version, feature flags, OS distro). It is not the
> selected `target_node`. Do **not** check per-node dynamic state in `precheck()`.

#### What happens when `precheck()` returns a reason

1. The nemesis is permanently removed from `disruptions_list` — it never enters
   the execution cycle.
2. Exactly one `DisruptionEvent` marked `SKIPPED` is published at precheck time
   (one `NemesisStatus.SKIPPED` row in Argus per excluded nemesis, not per cycle).
3. A warning is logged: `"Nemesis <Name> excluded by precheck: <reason>"`.
4. If **every** selected nemesis is excluded, one `Severity.CRITICAL`
   `TestFrameworkEvent` is published naming each exclusion reason, and the
   nemesis thread stops cleanly. A misconfigured selector that produces an
   empty rotation fails the test loudly.

#### Before / after example

**Before** — static guard inside `disrupt()`, evaluated every cycle:

```python
def disrupt(self):
    if not self.runner.cluster.params.get("use_ldap_auth"):
        raise UnsupportedNemesis("LDAP not configured")
    # ... actual disruption
```

**After** — evaluated once before the execution loop via `precheck(node)`:

```python
def precheck(self, node) -> str | None:
    if not self.runner.cluster.params.get("use_ldap_auth"):
        return "LDAP not configured"
    return None

def disrupt(self):
    # ... actual disruption (no static guard needed here)
```

### Step 3: Implement the disruption logic

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
nemesis:
  runners:
    - SisyphusMonkey:
        selector: 'MyNewMonkey'
        interval: 5
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
        # Select all non-disruptive nemesis. precheck(node) is called by run()
        # before the execution loop — excluded nemesis are reported and logged once.
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

### Adding options to a runner

A runner's entry in `nemesis.runners` is validated by the runner's `config_model`:

- `RunnerConfig` (`sdcm/nemesis/__init__.py`) — `interval`, `seed`, `filter_seeds`
- `NemesisRunnerConfig(RunnerConfig)` — adds `selector` and `multiply_factor`; the default `config_model` of `NemesisRunner`

For options of its own, define a model next to the runner, subclassing `RunnerConfig` or `NemesisRunnerConfig`, and point `config_model` at it. The runner receives its resolved config as the `config` constructor argument and reads options from `self.config`:

```python
from pydantic import Field

from sdcm.nemesis import NemesisRunner, NemesisRunnerConfig


class MyRunnerConfig(NemesisRunnerConfig):
    rounds: int = Field(default=1, description="Times to run each disruption")


class MyRunner(NemesisRunner):
    config_model = MyRunnerConfig

    def __init__(self, tester_obj, termination_event, config: MyRunnerConfig):
        super().__init__(tester_obj, termination_event, config=config)
        disruptions = self.build_disruptions_by_selector(self.nemesis_selector)
        self.disruptions_list = disruptions * self.config.rounds
```

`config.nemesis.resolve_runners()` returns `(runner_class, resolved_config)` per entry: options the entry leaves unset take the `nemesis:` section defaults, filling only the options the runner's model declares. To validate nemesis class names in an option at config load, override `check_disruptions()` (see `CategoricalMonkeyConfig` and `FixedOrderMonkeyConfig` in `sdcm/nemesis/monkey/runners.py`).

### Available Runners

| Runner | Description |
|--------|-------------|
| `SisyphusMonkey` | Standard runner — filters by `selector`, shuffles, cycles through all |
| `FixedOrderMonkey` | Runs the nemesis named in `order` in exact order, repeating the sequence |
| `CategoricalMonkey` | Weighted random selection from the `dist` distribution |
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
