# Source and Destination Cluster Support

## Problem Statement

SCT currently supports a single System-Under-Test (SUT) cluster per test run (with the exception of Gemini's oracle cluster). Tests that need cross-cluster operations — such as cross-cluster replication, data migrations, or CDC stream forwarding — have no native way to provision and manage a second independent Scylla cluster.

### Current Limitations

- No configuration fields exist for a "destination" cluster (version, instance type, AMI, node count).
- The provisioning and lifecycle code paths in `ClusterTester` and the AWS/GCE/Azure provisioning modules assume at most a primary cluster and an oracle cluster.
- Test authors working on cross-cluster features must manually provision and wire up additional clusters outside SCT, losing integration with SCT's lifecycle management (teardown, log collection, Argus reporting).

### Why This Matters

ScyllaDB's roadmap includes features that operate across two independent clusters (e.g., CDC-based replication, live migrations). SCT must be able to natively provision, connect, and tear down both a **source** and a **destination** cluster within a single test run — using different Scylla versions, instance types, or configurations for each.

### Source-First Design Principle

The existing Scylla SUT cluster (`self.db_cluster`) is the **source** cluster by default. In a single-cluster test, it also acts as the implicit target — no additional configuration is required. The destination cluster is purely opt-in: only tests that explicitly define a destination block will provision a second cluster. This keeps all existing tests unchanged and introduces no new concepts to single-cluster workflows.

### Cluster Reuse for Spark and External Workloads (Future)

For Spark-related tests and other scenarios where the source and/or destination clusters are **already running** (not provisioned by SCT), the existing `reuse_cluster` mechanism (`sdcm/sct_config.py:752–758`, documented in `docs/reuse_cluster.md`) provides a path to point at pre-existing clusters by `test_id`. Since `reuse_cluster` is based on the shared `test_id` (a top-level field), a future extension would add a **top-level** `reuse_cluster_destination` field (rather than nesting it inside `destination:`), e.g.:

```yaml
# Top-level — not inside destination: block
reuse_cluster_destination: "7dc6db84-eb01-4b61-a946-b5c72e0f6d71"
```

This would skip provisioning for the destination cluster and instead attach to the existing running infrastructure. **No implementation is planned for this in the current scope** — it is documented here as a known future need to ensure the configuration design does not preclude it.

## Current State

SCT already supports a dual-cluster pattern via its **Gemini oracle cluster**. This pattern is the foundation for the destination-cluster design.

### Oracle Cluster Configuration Fields

The following fields in `sdcm/sct_config.py` define the oracle cluster:

| Field | Type | Location (line) | Purpose |
|-------|------|-----------------|---------|
| `n_test_oracle_db_nodes` | `IntOrList` | `sct_config.py:517` | Node count for oracle cluster |
| `oracle_scylla_version` | `String` | `sct_config.py:603` | Scylla version for oracle |
| `instance_type_db_oracle` | `String` | `sct_config.py:954` | AWS instance type |
| `ami_id_db_oracle` | `String` | `sct_config.py:980` | AMI ID (auto-resolved from version) |
| `oracle_user_data_format_version` | `String` | `sct_config.py:598` | User-data format override |
| `append_scylla_args_oracle` | `String` | `sct_config.py:866` | Extra Scylla CLI arguments |
| `azure_instance_type_db_oracle` | `String` | `sct_config.py:1210` | Azure VM size |

Defaults for these fields are in `defaults/test_default.yaml` (lines 122–124):

```yaml
n_test_oracle_db_nodes: 1
oracle_scylla_version: '2024.1'
append_scylla_args_oracle: '--enable-cache false'
```

### Oracle Cluster Provisioning Trigger

The oracle cluster is triggered when `db_type == "mixed_scylla"`.

- **`sdcm/sct_provision/common/layout.py:41–42`** — `_provision_another_scylla_cluster` returns `True` for `mixed_scylla`.
- **`sdcm/sct_provision/aws/layout.py:72–80`** — `cs_db_cluster` property creates an `OracleDBCluster` instance.
- **`sdcm/sct_provision/aws/layout.py:45–46`** — `provision()` calls `self.cs_db_cluster.provision()` when present.

### Oracle Cluster Classes

- **`sdcm/sct_provision/aws/cluster.py:369–385`** — `OracleDBCluster(ClusterBase)`: node type `oracle-db`, prefix `oracle`, uses `OracleScyllaInstanceParamsBuilder`.
- **`sdcm/sct_provision/aws/instance_parameters_builder.py:195–198`** — `OracleScyllaInstanceParamsBuilder(ScyllaInstanceParamsBuilder)`: maps `instance_type_db_oracle`, `ami_id_db_oracle`, `root_disk_size_db`.

### Oracle Cluster in ClusterTester

In `sdcm/tester.py`:

- **Line 1255** — `self.cs_db_cluster = None` initialized in `setUp()`.
- **Lines 1879–1881** — When `db_type == "mixed_scylla"`, both `self.db_cluster` (scylla) and `self.cs_db_cluster` (mixed_scylla oracle) are created.
- **Lines 1847–1857** — The `create_cluster("mixed_scylla")` branch constructs a `ScyllaAWSCluster` using oracle-specific params (`ami_id_db_oracle`, `instance_type_db_oracle`, `n_test_oracle_db_nodes`).
- **Lines 3594–3601** — `clean_resources()` handles `cs_db_cluster` destroy/keep alongside `db_cluster`.

### Oracle AMI Resolution

In `sdcm/sct_config.py`, the `__init__` method (lines 2504–2530) automatically resolves `oracle_scylla_version` to `ami_id_db_oracle` for AWS, following the same pattern as the main `scylla_version` → `ami_id_db_scylla` resolution.

### Post-Behavior Actions

`sdcm/utils/common.py:2147–2167` — `get_post_behavior_actions()` adds `oracle-db` to `db_nodes` node types when `db_type == "mixed_scylla"`. The oracle cluster shares the `post_behavior_db_nodes` setting with the primary cluster.

## Goals

1. **Source-first design** — the existing SUT cluster (`self.db_cluster`) is the source cluster. When no destination is configured, it also serves as the default target. No new concepts are introduced to single-cluster tests.
2. **Support nested configuration** — destination cluster properties are defined in a `destination:` YAML block, with inheritance from global defaults. This avoids a proliferation of prefixed flat fields and keeps test YAMLs readable.
3. **Implement a trigger mechanism** — the presence of a `destination:` block with `n_db_nodes > 0` enables dual-cluster provisioning (no `db_type` change required).
4. **Add provisioning classes** (`DestinationDBCluster`, `DestinationScyllaInstanceParamsBuilder`) for AWS, modeled after `OracleDBCluster`.
5. **Integrate destination cluster lifecycle** into `ClusterTester` (initialization, stop, cleanup, log collection).
6. **Maintain 100% backwards compatibility** — all existing single-cluster tests must work without any changes.
7. **Expose the destination cluster** as `self.destination_cluster` in `ClusterTester`, separate from `self.db_cluster` (the source).
8. **Keep the design extensible** — the nested configuration and cluster references should not preclude future use of `reuse_cluster` per cluster role (for Spark and similar scenarios).

## Implementation Phases

### Phase 1: Nested Configuration and Defaults

**Objective**: Add destination cluster configuration using a nested YAML layout, with defaults inherited from global (source) settings.

#### Configuration Layout: Nested YAML

Rather than adding many flat `destination_*` prefixed fields (which is error-prone for users and hard to maintain), the destination cluster is configured using a **nested `destination:` block**. Properties inside this block override the corresponding global (source) defaults. Any property not specified in the `destination:` block is inherited from the top-level configuration.

**YAML Example**:

```yaml
# Global settings — these define the source cluster (SUT)
scylla_version: "2024.2"
instance_type_db: "i4i.large"
n_db_nodes: 3
cluster_backend: "aws"

# Destination cluster — only overrides what differs from source
destination:
  scylla_version: "2025.1"     # different version for destination
  n_db_nodes: 3                # required: how many destination nodes
  instance_type_db: "i3.large" # optional: different instance type
  # inherits cluster_backend, region_name, etc. from global
```

**Single-cluster test (no change)**:

```yaml
# No 'destination:' block — standard single-cluster test
scylla_version: "2024.2"
instance_type_db: "i4i.large"
n_db_nodes: 3
```

The source cluster (`self.db_cluster`) is always the SUT. When no `destination:` block is present, the source cluster is also the implicit target — the test behaves identically to today.

#### Implementation in SCTConfiguration

The `destination:` block maps to a set of dedicated fields in `SCTConfiguration` in `sdcm/sct_config.py`. Internally, during `__init__()`, when a `destination:` key is present in the merged YAML, its values are unpacked into these fields. Any value not provided in the `destination:` block falls back to the corresponding global field.

**Fields to add to `SCTConfiguration`**:

| New Field | Type | Inherits From (global) | Description |
|-----------|------|------------------------|-------------|
| `destination_n_db_nodes` | `IntOrList` | `n_db_nodes` | Node count for destination cluster |
| `destination_scylla_version` | `String` | `scylla_version` | Scylla version for destination |
| `destination_instance_type_db` | `String` | `instance_type_db` | AWS instance type |
| `destination_ami_id_db` | `String` | `ami_id_db_scylla` | AMI ID (auto-resolved from version) |
| `destination_append_scylla_args` | `String` | `append_scylla_args` | Extra CLI arguments |
| `destination_user_data_format_version` | `String` | `user_data_format_version` | User-data format override |
| `destination_azure_instance_type_db` | `String` | `azure_instance_type_db` | Azure VM size |
| `post_behavior_destination_db_nodes` | `Literal["destroy","keep","keep-on-failure"]` | — | Independent post-behavior control |

**Parsing logic** (in `SCTConfiguration.__init__()`):

```python
# Mapping of destination fields to their global fallback fields
_DESTINATION_INHERITABLE_FIELDS = {
    "scylla_version": "destination_scylla_version",
    "instance_type_db": "destination_instance_type_db",
    "append_scylla_args": "destination_append_scylla_args",
    "azure_instance_type_db": "destination_azure_instance_type_db",
}

# After loading and merging all config sources:
if destination_block := merged_config.pop("destination", None):
    for key, value in destination_block.items():
        self[f"destination_{key}"] = value
    # For any destination field not explicitly set, inherit from global:
    for global_key, dest_key in _DESTINATION_INHERITABLE_FIELDS.items():
        if not self.get(dest_key):
            self[dest_key] = self.get(global_key)
```

**`has_destination_cluster` property**:

```python
@property
def has_destination_cluster(self) -> bool:
    n_dest = self.get("destination_n_db_nodes")
    if n_dest is None:
        return False
    if isinstance(n_dest, int):
        return n_dest > 0
    if isinstance(n_dest, list):
        return any(int(n) > 0 for n in n_dest)
    return False
```

**Trigger**: The presence of a `destination:` block with `n_db_nodes > 0` triggers dual-cluster provisioning. If `destination:` is absent or `n_db_nodes` is 0, no destination cluster is provisioned.

Add defaults to `defaults/test_default.yaml`:

```yaml
# destination cluster defaults
post_behavior_destination_db_nodes: 'destroy'
```

Note: No default for `destination_n_db_nodes` is needed — the absence of the `destination:` block means no destination cluster.

Add AMI resolution logic for `destination_scylla_version` in `SCTConfiguration.__init__()`, following the existing oracle pattern at lines 2504–2530. When resolving AMIs, the architecture lookup uses `destination_instance_type_db` (which falls back to `instance_type_db` via inheritance).

**Definition of Done**:
- Nested `destination:` YAML block is parsed and mapped to `destination_*` fields
- Inheritance from global config works for unspecified destination properties
- `has_destination_cluster` property works correctly (False when no destination block, True when nodes > 0)
- AMI auto-resolution works for `destination_scylla_version` on AWS
- `docs/configuration_options.md` is auto-generated (via pre-commit hook)
- Unit tests cover: nested parsing, inheritance, `has_destination_cluster` with various inputs, absence of destination block

**Dependencies**: None (first phase)

---

### Phase 2: AWS Provisioning Classes

**Objective**: Add provisioning infrastructure for the destination cluster in the AWS backend.

**Deliverables**:

1. **`sdcm/sct_provision/aws/instance_parameters_builder.py`** — Add `DestinationScyllaInstanceParamsBuilder(ScyllaInstanceParamsBuilder)` mapping `destination_instance_type_db`, `destination_ami_id_db`, `root_disk_size_db`.

2. **`sdcm/sct_provision/aws/cluster.py`** — Add `DestinationDBCluster(ClusterBase)`:
   - `_NODE_TYPE = "destination-db"`
   - `_NODE_PREFIX = "destination"`
   - `_INSTANCE_TYPE_PARAM_NAME = "destination_instance_type_db"`
   - `_NODE_NUM_PARAM_NAME = "destination_n_db_nodes"`
   - `_INSTANCE_PARAMS_BUILDER = DestinationScyllaInstanceParamsBuilder`
   - `_USER_PARAM = "ami_db_scylla_user"`
   - `_user_data` property using `destination_user_data_format_version`

3. **`sdcm/sct_provision/common/layout.py`** — Add `_provision_destination_cluster` property (delegates to `self._params.has_destination_cluster`) and `destination_db_cluster` property (returns `None` by default).

4. **`sdcm/sct_provision/aws/layout.py`** — Add `destination_db_cluster` cached property creating `DestinationDBCluster` when `_provision_destination_cluster` is True. Call `self.destination_db_cluster.provision()` in the `provision()` method.

**Definition of Done**:
- `DestinationDBCluster` and `DestinationScyllaInstanceParamsBuilder` classes exist with correct attributes
- `SCTProvisionAWSLayout.provision()` provisions the destination cluster when enabled
- Base `SCTProvisionLayout` has `destination_db_cluster` property returning `None`
- Unit tests verify class attributes and provisioning gate logic

**Dependencies**: Phase 1 (configuration fields must exist)

---

### Phase 3: ClusterTester Integration

**Objective**: Wire the destination cluster into `ClusterTester`'s initialization, lifecycle, and teardown.

**Source-First Principle**: `self.db_cluster` is always the source cluster. In single-cluster tests it also serves as the implicit target — no code changes needed. Only when `self.params.has_destination_cluster` is True does a second cluster get created.

**Deliverables**:

1. **`sdcm/tester.py` — `setUp()`** (around line 1255): Initialize `self.destination_cluster = None`.

2. **`sdcm/tester.py` — `get_cluster_aws()`**: Add a `"destination_scylla"` branch in the `create_cluster()` inner function that builds a `ScyllaAWSCluster` using destination-specific params (`destination_ami_id_db`, `destination_instance_type_db`, `destination_n_db_nodes`). After the `db_type` branching logic (line ~1886), add:
   ```python
   if self.params.has_destination_cluster:
       self.destination_cluster = create_cluster("destination_scylla")
   ```

3. **`sdcm/tester.py` — `stop_resources()`** (around line 3533): Add handling for `self.destination_cluster`:
   ```python
   if self.destination_cluster:
       self.stop_resources_stop_tasks_threads(self.destination_cluster)
       self.get_backtraces(self.destination_cluster)
   ```

4. **`sdcm/tester.py` — `clean_resources()`** (around line 3588): Add a section for `self.destination_cluster` using the `destination_db_nodes` action from `get_post_behavior_actions()`, following the same destroy/keep-on-failure pattern used for other cluster types.

5. **`sdcm/utils/common.py` — `get_post_behavior_actions()`**: Add `"destination_db_nodes"` with node type `["destination-db"]` to `action_per_type`.

6. **Argus reporting** — Report metadata from both clusters with role labels:

   **Current oracle cluster reporting** (for context): The oracle cluster version is only reported inside `argus_submit_gemini_results()` as a Gemini-specific field (`oracle_node_scylla_version`). It is **not** reported as a general package to Argus. The main `argus_get_scylla_version()` only reads from `self.db_cluster.nodes[0]`, and `update_argus_with_version()` in `sct_config.py` reports a single `scylla-server-target` package.

   **What to implement for destination clusters**:

   a. **`sdcm/sct_config.py` — `update_argus_with_version()`**: When `has_destination_cluster` is True and `destination_scylla_version` is set, also submit a package with name `scylla-server-destination` (alongside the existing `scylla-server-target` for the source). This ensures Argus receives version metadata for both clusters at config resolution time.

   b. **`sdcm/tester.py` — `argus_get_scylla_version()`**: After reporting the source cluster version (existing `self.db_cluster.nodes[0]`), if `self.destination_cluster` is not None, submit a `scylla-server-destination` package via `submit_packages()` using the version from `self.destination_cluster.nodes[0].get_scylla_binary_version()`. Use `submit_packages()` (not `update_scylla_version()`) since the destination is an additional package, not a replacement for the primary Scylla version.

   c. **Argus accumulation**: Argus's `submit_packages([...])` accepts a list and should support multiple packages per test run. This needs verification during implementation — if Argus replaces rather than accumulates packages, a follow-up Argus change will be needed. Argus UI may also need a minor update to display dual-cluster metadata.

**Backwards Compatibility**:
- `self.db_cluster` remains the source cluster — existing tests referencing `self.db_cluster.nodes` are unaffected.
- `self.destination_cluster` is `None` by default; only tests with a `destination:` block and `n_db_nodes > 0` will have it.
- The `db_type` field and existing oracle/mixed_scylla logic are untouched.

**Definition of Done**:
- `self.destination_cluster` initialized in `setUp()`, created in `get_cluster_aws()` when configured
- `stop_resources()` handles destination cluster gracefully
- `clean_resources()` applies `post_behavior_destination_db_nodes` to destination cluster
- `get_post_behavior_actions()` includes `destination_db_nodes` entry
- Destination cluster version reported to Argus as `scylla-server-destination` package
- Both cluster versions visible in Argus test run metadata
- Unit tests verify post-behavior actions include destination_db_nodes
- Existing tests pass without modification

**Dependencies**: Phase 1 and Phase 2

---

### Phase 4: GCE and Azure Backend Support

**Objective**: Extend destination cluster provisioning to GCE and Azure backends.

**Deliverables**:

1. **`sdcm/tester.py` — `get_cluster_gce()`**: Add destination cluster creation, similar to the AWS pattern. The destination cluster should use `gce_instance_type_db` (or a new `gce_instance_type_db_destination` field if needed) and resolve GCE images from `destination_scylla_version`.

2. **`sdcm/tester.py` — `get_cluster_azure()`**: Add destination cluster creation using `azure_instance_type_db_destination` and resolving Azure images from `destination_scylla_version`.

3. **Needs Investigation**: Determine if GCE image resolution needs additional fields (e.g., `gce_image_db_destination`) or if the version-based resolution covers it.

**Definition of Done**:
- Destination cluster provisioning works on all three major backends (AWS, GCE, Azure)
- Backend-specific configuration fields are added as needed
- Integration test validates at minimum one backend end-to-end

**Dependencies**: Phase 3

---

### Phase 5: Cross-Cluster Networking

**Objective**: Enable network connectivity between source and destination clusters for cross-cluster operations.

**Needs Investigation**: The networking requirements depend on the specific cross-cluster features (replication, CDC streams, migrations). Key questions:

- Do source and destination clusters need to be in the same VPC, or can VPC peering be configured automatically?
- Is the existing `network_config` infrastructure sufficient, or do we need additional configuration?
- What security group rules are needed for cross-cluster communication?

**Deliverables**:

1. Add VPC peering or networking helper between source and destination cluster VPCs (AWS-specific).
2. Add configuration field (e.g., `destination_vpc_peering: true`) to control peering behavior.
3. Ensure both clusters can resolve each other's nodes by IP.

**Definition of Done**:
- Source cluster nodes can connect to destination cluster nodes and vice versa
- Networking is configured automatically during provisioning
- Teardown properly removes any peering or networking resources

**Dependencies**: Phase 4

---

### Phase 6: Documentation and Test Harness

**Objective**: Document the feature and provide a sample cross-cluster test case.

**Deliverables**:

1. Add a sample test-case YAML in `test-cases/` demonstrating a dual-cluster configuration.
2. Update `AGENTS.md` with destination cluster configuration guidance.
3. Verify `docs/configuration_options.md` is auto-updated via pre-commit.
4. Add a simple functional test that provisions both clusters and verifies connectivity.

**Definition of Done**:
- Sample test case YAML exists and is valid
- Configuration documentation is up to date
- A basic smoke test validates dual-cluster provisioning

**Dependencies**: Phase 3 (minimum), Phase 5 (for connectivity test)

## Testing Requirements

### Unit Tests (Phases 1–3)

- **Nested config parsing**: Verify `destination:` YAML block is correctly parsed into `destination_*` fields.
- **Inheritance tests**: Verify that omitting a field in `destination:` inherits the global value (e.g., `instance_type_db`).
- **`has_destination_cluster`**: Returns False when no `destination:` block, True when `destination.n_db_nodes > 0`, False when `destination.n_db_nodes: 0`.
- **Post-behavior tests**: Verify `get_post_behavior_actions()` includes `destination_db_nodes` with correct node type and action.
- **Provisioning class tests**: Verify `DestinationDBCluster` and `DestinationScyllaInstanceParamsBuilder` have correct class attributes.
- **Backwards compatibility**: Run full existing unit test suite — no test should break.

### Integration Tests (Phases 3–5)

- **Docker backend**: Provision source + destination clusters with `--backend docker`, verify both are accessible.
- **AWS backend**: Provision with different Scylla versions for source and destination, verify AMI resolution.

### Manual Tests (Phase 4–6)

- **Cross-cluster replication**: Requires actual cross-cluster workload test (outside SCT automation).
- **Multi-region**: Verify destination cluster can be in a different region from source.
- **Cleanup verification**: Confirm both clusters are destroyed after test, including on failure paths.

## Success Criteria

1. A test YAML with a `destination:` block containing `n_db_nodes: 3` and `scylla_version: "2024.1"` provisions two independent Scylla clusters.
2. Destination properties inherit from global config — e.g., omitting `instance_type_db` in the `destination:` block uses the global `instance_type_db`.
3. Existing tests without a `destination:` block behave identically to today — zero regressions.
4. `self.db_cluster` is always the source; `self.destination_cluster` is available for cross-cluster operations.
5. Both clusters' logs are collected and reported to Argus.
6. Post-behavior settings independently control source and destination cluster lifecycle.

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Existing tests break | High | No `destination:` block means no destination cluster; `destination_cluster` is `None` by default |
| AMI resolution failure for destination version | Medium | Use same resolution logic as oracle; inherit `instance_type_db` for architecture detection when `destination_instance_type_db` is not set; raise clear error on resolution failure |
| Cross-cluster networking complexity | Medium | Phase 5 is isolated; early phases work with clusters in the same VPC/region |
| Resource leaks (destination cluster not cleaned up) | High | Independent `post_behavior_destination_db_nodes` with default `destroy`; explicit teardown in `clean_resources()` |
| Nested config parsing complexity | Medium | Keep parsing logic simple — `destination:` is a flat dict of overrides, not deeply nested; validate against known field names |
| Configuration explosion (too many new fields) | Low | Nested block with inheritance reduces visible config surface; only overridden fields need to be specified |

## Open Questions

1. **GCE image fields** (resolved): Yes, `destination_scylla_version` should auto-resolve GCE images using the same resolution logic as the source cluster. Both source and destination clusters resolve their versions independently, supporting different Scylla versions on each. Implementation details (field naming, resolution function reuse) to be finalized in Phase 4.
2. **Networking scope**: What level of cross-cluster networking is needed in Phase 5? This depends on the first cross-cluster feature being tested.
3. **Log collection**: Should destination cluster logs go to a separate directory, or be co-located with source cluster logs under a `destination/` subdirectory?
4. **Argus reporting** (resolved): Both clusters report as a single test run. The source version is `scylla-server-target` (existing), the destination version is `scylla-server-destination` (new), submitted via `submit_packages()`. Accumulation behavior needs verification during implementation — if Argus replaces rather than appends packages, a follow-up Argus change will be needed. Note: the oracle cluster currently only reports its version inside Gemini-specific results — not as a general package. The destination cluster approach improves on this by reporting both versions as first-class packages.
5. **Environment variable mapping** (resolved): SCT already supports dot-based nesting for environment variables in `_load_environment_variables()` (`sdcm/sct_config.py:2900–2914`). For example, `SCT_STRESS_READ_CMD.0` and `SCT_STRESS_IMAGE.cassandra-stress` use a `SCT_<FIELD>.<nested_key>` convention. The `destination:` block should use this existing mechanism: `SCT_DESTINATION.scylla_version`, `SCT_DESTINATION.n_db_nodes`, etc. This avoids both the flat `SCT_DESTINATION_SCYLLA_VERSION` approach (ambiguous — is it a nested key or a flat field?) and the pydantic double-underscore `SCT_DESTINATION__SCYLLA_VERSION` convention (not used anywhere in SCT). The dot-nesting code already parses these into a dict value, which maps directly to the `destination:` YAML block.
6. **Spark / cluster reuse** (resolved): `reuse_cluster` is based on `test_id`, which is a shared top-level field. Therefore, cluster reuse configuration should remain at the top level rather than nested inside `destination:`. A future implementation could add `reuse_cluster_destination` as a top-level field (parallel to the existing `reuse_cluster`) to allow reusing a different pre-existing cluster as the destination. Deferred to future implementation.
