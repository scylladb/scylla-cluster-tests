# Source and Destination Cluster Support

## Problem Statement

SCT currently supports a single System-Under-Test (SUT) cluster per test run (with the exception of Gemini's oracle cluster). Tests that need cross-cluster operations — such as cross-cluster replication, data migrations, or CDC stream forwarding — have no native way to provision and manage a second independent Scylla cluster.

### Current Limitations

- No configuration fields exist for a "destination" cluster (version, instance type, AMI, node count).
- The provisioning and lifecycle code paths in `ClusterTester` and the AWS/GCE/Azure provisioning modules assume at most a primary cluster and an oracle cluster.
- Test authors working on cross-cluster features must manually provision and wire up additional clusters outside SCT, losing integration with SCT's lifecycle management (teardown, log collection, Argus reporting).

### Why This Matters

ScyllaDB's roadmap includes features that operate across two independent clusters (e.g., CDC-based replication, live migrations). SCT must be able to natively provision, connect, and tear down both a **source** and a **destination** cluster within a single test run — using different Scylla versions, instance types, or configurations for each.

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

1. **Add configuration fields** for a destination cluster (version, node count, instance type, AMI, etc.) following the oracle pattern.
2. **Implement a trigger mechanism** — the presence of `n_db_destination_nodes > 0` enables dual-cluster provisioning (no `db_type` change required).
3. **Add provisioning classes** (`DestinationDBCluster`, `DestinationScyllaInstanceParamsBuilder`) for AWS, modeled after `OracleDBCluster`.
4. **Integrate destination cluster lifecycle** into `ClusterTester` (initialization, stop, cleanup, log collection).
5. **Maintain 100% backwards compatibility** — all existing single-cluster tests must work without any changes.
6. **Expose the destination cluster** as `self.destination_cluster` in `ClusterTester`, separate from `self.db_cluster` (the source).

## Implementation Phases

### Phase 1: Configuration Fields and Defaults

**Objective**: Add all configuration fields and defaults for the destination cluster.

**Deliverables**:

Add the following fields to `SCTConfiguration` in `sdcm/sct_config.py`:

| New Field | Type | Modeled After | Description |
|-----------|------|---------------|-------------|
| `n_db_destination_nodes` | `IntOrList` | `n_test_oracle_db_nodes` | Node count for destination cluster |
| `destination_scylla_version` | `String` | `oracle_scylla_version` | Scylla version for destination |
| `instance_type_db_destination` | `String` | `instance_type_db_oracle` | AWS instance type |
| `ami_id_db_destination` | `String` | `ami_id_db_oracle` | AMI ID (auto-resolved from version) |
| `append_scylla_args_destination` | `String` | `append_scylla_args_oracle` | Extra CLI arguments |
| `destination_user_data_format_version` | `String` | `oracle_user_data_format_version` | User-data format override |
| `azure_instance_type_db_destination` | `String` | `azure_instance_type_db_oracle` | Azure VM size |
| `post_behavior_destination_db_nodes` | `Literal["destroy","keep","keep-on-failure"]` | `post_behavior_vector_store_nodes` | Independent post-behavior control |

Add a `has_destination_cluster` property to `SCTConfiguration` that returns `True` when `n_db_destination_nodes` is set and greater than 0.

Add defaults to `defaults/test_default.yaml`:

```yaml
# destination cluster defaults
n_db_destination_nodes: 0
append_scylla_args_destination: ''
post_behavior_destination_db_nodes: 'destroy'
```

Add AMI resolution logic for `destination_scylla_version` in `SCTConfiguration.__init__()`, following the existing oracle pattern at lines 2504–2530. The destination version should fall back to `instance_type_db` if `instance_type_db_destination` is not set.

**Definition of Done**:
- All fields are defined in `SCTConfiguration` with `SctField` descriptors
- Defaults are present in `defaults/test_default.yaml`
- `has_destination_cluster` property works correctly (False by default, True when nodes > 0)
- AMI auto-resolution works for `destination_scylla_version` on AWS
- `docs/configuration_options.md` is auto-generated (via pre-commit hook)
- Unit tests cover: field existence, default values, `has_destination_cluster` with 0/positive/list values

**Dependencies**: None (first phase)

---

### Phase 2: AWS Provisioning Classes

**Objective**: Add provisioning infrastructure for the destination cluster in the AWS backend.

**Deliverables**:

1. **`sdcm/sct_provision/aws/instance_parameters_builder.py`** — Add `DestinationScyllaInstanceParamsBuilder(ScyllaInstanceParamsBuilder)` mapping `instance_type_db_destination`, `ami_id_db_destination`, `root_disk_size_db`.

2. **`sdcm/sct_provision/aws/cluster.py`** — Add `DestinationDBCluster(ClusterBase)`:
   - `_NODE_TYPE = "destination-db"`
   - `_NODE_PREFIX = "destination"`
   - `_INSTANCE_TYPE_PARAM_NAME = "instance_type_db_destination"`
   - `_NODE_NUM_PARAM_NAME = "n_db_destination_nodes"`
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

**Deliverables**:

1. **`sdcm/tester.py` — `setUp()`** (around line 1255): Initialize `self.destination_cluster = None`.

2. **`sdcm/tester.py` — `get_cluster_aws()`**: Add a `"destination_scylla"` branch in the `create_cluster()` inner function that builds a `ScyllaAWSCluster` using destination-specific params. After the `db_type` branching logic (line ~1886), add:
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

**Backwards Compatibility**:
- `self.db_cluster` remains the source cluster — existing tests referencing `self.db_cluster.nodes` are unaffected.
- `self.destination_cluster` is `None` by default; only tests that set `n_db_destination_nodes > 0` will have it.
- The `db_type` field and existing oracle/mixed_scylla logic are untouched.

**Definition of Done**:
- `self.destination_cluster` initialized in `setUp()`, created in `get_cluster_aws()` when configured
- `stop_resources()` handles destination cluster gracefully
- `clean_resources()` applies `post_behavior_destination_db_nodes` to destination cluster
- `get_post_behavior_actions()` includes `destination_db_nodes` entry
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

- **Configuration tests**: Verify all new fields exist in `SCTConfiguration`, default values are correct, `has_destination_cluster` property returns correct values for 0, positive int, and list inputs.
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

1. A test YAML with `n_db_destination_nodes: 3` and `destination_scylla_version: "2024.1"` provisions two independent Scylla clusters.
2. Existing tests with `n_db_destination_nodes: 0` (default) behave identically to today — zero regressions.
3. `self.destination_cluster` is available in test methods for cross-cluster operations.
4. Both clusters' logs are collected and reported to Argus.
5. Post-behavior settings independently control source and destination cluster lifecycle.

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Existing tests break | High | Default `n_db_destination_nodes: 0` ensures no provisioning; `destination_cluster` is `None` by default |
| AMI resolution failure for destination version | Medium | Fall back to `instance_type_db` if `instance_type_db_destination` is not set; validate in config `__init__` |
| Cross-cluster networking complexity | Medium | Phase 5 is isolated; early phases work with clusters in the same VPC/region |
| Resource leaks (destination cluster not cleaned up) | High | Independent `post_behavior_destination_db_nodes` with default `destroy`; explicit teardown in `clean_resources()` |
| Configuration explosion (too many new fields) | Low | Follow established oracle pattern; fields are optional with sensible defaults |

## Open Questions

1. **GCE image fields**: Does `destination_scylla_version` auto-resolve GCE images, or do we need `gce_image_db_destination`? Needs investigation when implementing Phase 4.
2. **Networking scope**: What level of cross-cluster networking is needed in Phase 5? This depends on the first cross-cluster feature being tested.
3. **Log collection**: Should destination cluster logs go to a separate directory, or be co-located with source cluster logs under a `destination/` subdirectory?
4. **Argus reporting**: How should dual-cluster test results be reported — as a single test run with metadata from both clusters, or as linked runs?
